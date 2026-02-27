package metricsgenreceiver

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/elastic/metricsgenreceiver/metricsgenreceiver/internal/distribution"
	"github.com/elastic/metricsgenreceiver/metricsgenreceiver/internal/dp"
	"github.com/elastic/metricsgenreceiver/metricsgenreceiver/internal/expohistogen"
	"github.com/elastic/metricsgenreceiver/metricsgenreceiver/internal/metadata"
	"github.com/elastic/metricsgenreceiver/metricsgenreceiver/internal/metricstmpl"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componentstatus"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.uber.org/zap"
)

type MetricsGenReceiver struct {
	cfg       *Config
	obsreport *receiverhelper.ObsReport
	settings  receiver.Settings

	baseRand    *rand.Rand // base random number generator seeded with the configured seed
	expHistoGen *expohistogen.Generator
	nextMetrics consumer.Metrics
	cancel      context.CancelFunc
	scenarios   []Scenario
	progress    *MetricsProgress
}

type Scenario struct {
	config                     ScenarioCfg
	metricsTemplate            *pmetric.Metrics
	resourceAttributesTemplate pcommon.Resource
	resources                  []pcommon.Resource
}

type MetricsProgress struct {
	start      time.Time
	datapoints atomic.Uint64
}

func newMetricsProgress() *MetricsProgress {
	return &MetricsProgress{
		start:      time.Now(),
		datapoints: atomic.Uint64{},
	}
}

func (p *MetricsProgress) duration() time.Duration {
	return time.Since(p.start)
}
func (p *MetricsProgress) dataPointsPerSecond() float64 {
	return float64(p.datapoints.Load()) / p.duration().Seconds()
}

func (p *MetricsProgress) eta(progressPct float64) time.Duration {
	if progressPct == 0 {
		return time.Duration(0)
	}
	return time.Duration(float64(p.duration().Nanoseconds())/progressPct) - p.duration()
}

func newMetricsGenReceiver(cfg *Config, set receiver.Settings) (*MetricsGenReceiver, error) {
	if cfg.Threads > 0 {
		runtime.GOMAXPROCS(cfg.Threads)
	}

	obsreport, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID:             set.ID,
		ReceiverCreateSettings: set,
	})
	if err != nil {
		return nil, err
	}

	nowish := time.Now().Truncate(time.Second)
	if cfg.StartTime.IsZero() {
		cfg.StartTime = nowish.Add(-cfg.StartNowMinus)
	}
	if cfg.EndTime.IsZero() {
		cfg.EndTime = nowish.Add(-cfg.EndNowMinus)
	}

	baseRand := rand.New(rand.NewSource(cfg.Seed))
	expHistoGen, err := expohistogen.NewGenerator(cfg.GetExponentialHistogramsTemplatePath())
	if err != nil {
		return nil, err
	}

	scenarios := make([]Scenario, 0, len(cfg.Scenarios))
	for _, scn := range cfg.Scenarios {

		metrics, err := metricstmpl.RenderMetricsTemplate(scn.Path, scn.TemplateVars)
		if err != nil {
			return nil, err
		}

		if scn.ForceExponentialHistograms() {
			dp.ForEachMetric(&metrics, func(res pcommon.Resource, is pcommon.InstrumentationScope, m pmetric.Metric) {
				replaceHistogramsWithExponentialHistograms(m)
			})
		}
		dp.ForEachDataPoint(&metrics, func(res pcommon.Resource, is pcommon.InstrumentationScope, m pmetric.Metric, dp dp.DataPoint) {
			dp.SetStartTimestamp(pcommon.NewTimestampFromTime(cfg.StartTime))
			if scn.AggregationTemporalityOverride() != pmetric.AggregationTemporalityUnspecified {
				switch m.Type() {
				case pmetric.MetricTypeSum:
					m.Sum().SetAggregationTemporality(scn.AggregationTemporalityOverride())
				case pmetric.MetricTypeHistogram:
					m.Histogram().SetAggregationTemporality(scn.AggregationTemporalityOverride())
				default:
					// no-op
				}
			}
			// initialize exponential histograms with clean values and set their temporality to delta as we currently only support that
			if m.Type() == pmetric.MetricTypeExponentialHistogram {
				expHistoGen.GenerateInto(baseRand, dp.(pmetric.ExponentialHistogramDataPoint))
				m.ExponentialHistogram().SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
			}

		})
		resources, err := metricstmpl.GetResources(scn.Path, cfg.StartTime, scn.Scale, scn.TemplateVars, baseRand, scn.InstanceIDOffset)
		if err != nil {
			return nil, err
		}
		scenarios = append(scenarios, Scenario{
			config:          scn,
			metricsTemplate: &metrics,
			resources:       resources,
		})
	}

	return &MetricsGenReceiver{
		cfg:         cfg,
		settings:    set,
		baseRand:    baseRand,
		expHistoGen: expHistoGen,
		obsreport:   obsreport,
		scenarios:   scenarios,
		progress:    newMetricsProgress(),
	}, nil
}

func (r *MetricsGenReceiver) Start(ctx context.Context, host component.Host) error {
	ctx, cancel := context.WithCancel(ctx)
	r.cancel = cancel
	if r.cfg.Sync != nil && r.cfg.Sync.Enabled {
		go r.runSyncLoop(ctx, host)
	} else {
		go r.runLocalLoop(ctx, host)
	}
	return nil
}

// runSyncLoop drives generation from NATS tick messages so all instances use the same timestamp.
func (r *MetricsGenReceiver) runSyncLoop(ctx context.Context, host component.Host) {
	nc, err := nats.Connect(r.cfg.Sync.NatsURL)
	if err != nil {
		r.settings.Logger.Error("sync: failed to connect to NATS", zap.Error(err))
		return
	}
	defer nc.Drain()

	sub, err := nc.SubscribeSync(r.cfg.Sync.SubjectTick)
	if err != nil {
		r.settings.Logger.Error("sync: failed to subscribe to tick subject", zap.Error(err))
		return
	}
	defer sub.Unsubscribe()

	nextLog := r.progress.start.Add(10 * time.Second)
	for {
		if ctx.Err() != nil {
			return
		}
		msg, err := sub.NextMsg(time.Minute)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			r.settings.Logger.Debug("sync: next message wait failed", zap.Error(err))
			continue
		}
		var currentTime time.Time
		if err := currentTime.UnmarshalText(msg.Data); err != nil {
			r.settings.Logger.Error("sync: invalid tick payload (expected RFC3339)", zap.Error(err), zap.ByteString("payload", msg.Data))
			continue
		}
		if currentTime.UnixNano() >= r.cfg.EndTime.UnixNano() {
			r.settings.Logger.Info("sync: received end tick, finishing")
			if r.cfg.ExitAfterEnd {
				if r.cfg.ExitAfterEndTimeout > 0 {
					time.Sleep(r.cfg.ExitAfterEndTimeout)
				}
				componentstatus.ReportStatus(host, componentstatus.NewFatalErrorEvent(errors.New("exiting because exit_after_end is set to true")))
			}
			return
		}
		if time.Now().After(nextLog) {
			progressPct := currentTime.Sub(r.cfg.StartTime).Seconds() / r.cfg.EndTime.Sub(r.cfg.StartTime).Seconds()
			r.settings.Logger.Info("generating metrics progress",
				zap.Int("progress_percent", int(progressPct*100)),
				zap.String("eta", r.progress.eta(progressPct).Round(time.Second).String()),
				zap.Uint64("datapoints", r.progress.datapoints.Load()),
				zap.Float64("data_points_per_second", r.progress.dataPointsPerSecond()),
			)
			nextLog = nextLog.Add(10 * time.Second)
		}
		intervalNanos := r.cfg.Interval.Nanoseconds()
		i := int((currentTime.UnixNano() - r.cfg.StartTime.UnixNano()) / intervalNanos)
		r.progress.datapoints.Add(r.produceMetrics(ctx, currentTime))
		r.applyChurn(i, currentTime)
		// Publish done so coordinator can wait for all instances before next tick.
		donePayload, _ := json.Marshal(map[string]string{"instance_id": r.cfg.Sync.InstanceID, "ts": currentTime.Format(time.RFC3339Nano)})
		if err := nc.Publish(r.cfg.Sync.SubjectDone, donePayload); err != nil {
			r.settings.Logger.Warn("sync: failed to publish done", zap.Error(err))
		}
	}
}

func (r *MetricsGenReceiver) runLocalLoop(ctx context.Context, host component.Host) {
	nextLog := r.progress.start.Add(10 * time.Second)
	ticker := time.NewTicker(r.cfg.Interval)
	defer ticker.Stop()
	currentTime := r.cfg.StartTime
	for i := 0; currentTime.UnixNano() < r.cfg.EndTime.UnixNano(); i++ {
		if ctx.Err() != nil {
			return
		}
		if time.Now().After(nextLog) {
			progressPct := currentTime.Sub(r.cfg.StartTime).Seconds() / r.cfg.EndTime.Sub(r.cfg.StartTime).Seconds()
			r.settings.Logger.Info("generating metrics progress",
				zap.Int("progress_percent", int(progressPct*100)),
				zap.String("eta", r.progress.eta(progressPct).Round(time.Second).String()),
				zap.Uint64("datapoints", r.progress.datapoints.Load()),
				zap.Float64("data_points_per_second", r.progress.dataPointsPerSecond()),
			)
			nextLog = nextLog.Add(10 * time.Second)
		}
		simulatedTime := addJitter(currentTime, r.cfg.IntervalJitterStdDev, r.cfg.Interval)
		r.progress.datapoints.Add(r.produceMetrics(ctx, simulatedTime))
		r.applyChurn(i, simulatedTime)

		if r.cfg.RealTime {
			<-ticker.C
		}
		currentTime = currentTime.Add(r.cfg.Interval)
	}
	if r.cfg.ExitAfterEnd {
		if r.cfg.ExitAfterEndTimeout > 0 {
			r.settings.Logger.Info("finished generating metrics, waiting before exiting",
				zap.Duration("exit_after_end_timeout", r.cfg.ExitAfterEndTimeout),
			)
			time.Sleep(r.cfg.ExitAfterEndTimeout)
		} else {
			r.settings.Logger.Info("finished generating metrics, exiting immediately")
		}
		componentstatus.ReportStatus(host, componentstatus.NewFatalErrorEvent(errors.New("exiting because exit_after_end is set to true")))
	}
}

func replaceHistogramsWithExponentialHistograms(m pmetric.Metric) {
	if m.Type() == pmetric.MetricTypeHistogram {
		// Get the histogram data
		histogram := m.Histogram()
		histogramDPs := histogram.DataPoints()

		// Store only the data point information declared in the DataPoint interface
		type dpInfo struct {
			startTimestamp pcommon.Timestamp
			timestamp      pcommon.Timestamp
			attributes     pcommon.Map
		}

		dpInfos := make([]dpInfo, histogramDPs.Len())
		for i := 0; i < histogramDPs.Len(); i++ {
			dp := histogramDPs.At(i)
			dpInfos[i] = dpInfo{
				startTimestamp: dp.StartTimestamp(),
				timestamp:      dp.Timestamp(),
				attributes:     dp.Attributes(),
			}
		}

		// Convert to exponential histogram
		expHist := m.SetEmptyExponentialHistogram()
		expHist.SetAggregationTemporality(histogram.AggregationTemporality())

		// Create exponential histogram data points preserving the series
		expDPs := expHist.DataPoints()
		expDPs.EnsureCapacity(len(dpInfos))

		for _, info := range dpInfos {
			expDP := expDPs.AppendEmpty()
			expDP.SetStartTimestamp(info.startTimestamp)
			expDP.SetTimestamp(info.timestamp)
			info.attributes.CopyTo(expDP.Attributes())
		}
	}

}

func addJitter(t time.Time, stdDev time.Duration, interval time.Duration) time.Time {
	if stdDev == 0 {
		return t
	}
	jitter := time.Duration(int64(math.Abs(rand.NormFloat64() * float64(stdDev))))
	if jitter >= interval {
		jitter = interval - 1
	}
	return t.Add(jitter)
}

func (r *MetricsGenReceiver) applyChurn(interval int, simulatedTime time.Time) {
	for _, scn := range r.scenarios {
		if scn.config.Churn == 0 {
			continue
		}

		startTime := simulatedTime.Format(time.RFC3339)
		for i := 0; i < scn.config.Churn; i++ {
			id := scn.config.Scale + interval*scn.config.Churn + i
			resource, err := metricstmpl.RenderResource(scn.config.Path, id, startTime, scn.config.TemplateVars, r.baseRand, scn.config.InstanceIDOffset)
			if err != nil {
				r.settings.Logger.Error("failed to apply churn", zap.Error(err))
			} else {
				scn.resources[id%len(scn.resources)] = resource
			}
		}
	}
}

func (r *MetricsGenReceiver) produceMetrics(ctx context.Context, currentTime time.Time) uint64 {
	dataPoints := new(uint64)
	wg := sync.WaitGroup{}
	for _, scn := range r.scenarios {

		// we don't keep track of the data points for each instance individually to reduce memory pressure
		// we still advance the metrics template have a new baseline that's used when simulating the metrics for each individual instance
		// this makes sure counters are increasing over time
		dp.ForEachDataPoint(scn.metricsTemplate, func(res pcommon.Resource, is pcommon.InstrumentationScope, m pmetric.Metric, dp dp.DataPoint) {
			distribution.AdvanceDataPoint(dp, r.baseRand, m, r.cfg.Distribution, r.expHistoGen)
		})

		concurrency := scn.config.Concurrency
		if r.cfg.Threads > 0 {
			concurrency = r.cfg.Threads
		}

		if concurrency == 0 {
			for i := range scn.config.Scale {
				*dataPoints += uint64(r.produceMetricsForInstance(ctx, r.baseRand, currentTime, scn, scn.resources[i]))
			}
			continue
		}

		for i := 0; i < concurrency; i++ {
			// Use a new random number generator for each goroutine to avoid race conditions
			ra := r.getNewRand()

			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				for j := 0; j < scn.config.Scale/concurrency; j++ {
					resource := scn.resources[j+i*scn.config.Scale/concurrency]
					currentDataPoints := r.produceMetricsForInstance(ctx, ra, currentTime, scn, resource)
					atomic.AddUint64(dataPoints, uint64(currentDataPoints))
				}
			}(i)
		}
	}
	wg.Wait()
	return *dataPoints
}

func (r *MetricsGenReceiver) produceMetricsForInstance(ctx context.Context, ra *rand.Rand, currentTime time.Time, scn Scenario, instanceResource pcommon.Resource) int {
	r.obsreport.StartMetricsOp(ctx)
	metrics := pmetric.NewMetrics()
	scn.metricsTemplate.CopyTo(metrics)
	resourceMetrics := metrics.ResourceMetrics()
	for j := 0; j < resourceMetrics.Len(); j++ {
		overrideExistingAttributes(instanceResource, resourceMetrics.At(j).Resource())
	}

	dp.ForEachDataPoint(&metrics, func(res pcommon.Resource, is pcommon.InstrumentationScope, m pmetric.Metric, dp dp.DataPoint) {
		distribution.AdvanceDataPoint(dp, ra, m, r.cfg.Distribution, r.expHistoGen)
		dp.SetTimestamp(pcommon.NewTimestampFromTime(currentTime))
	})
	dataPoints := metrics.DataPointCount()
	err := r.nextMetrics.ConsumeMetrics(ctx, metrics)
	r.obsreport.EndMetricsOp(ctx, metadata.Type.String(), dataPoints, err)
	return dataPoints
}

func overrideExistingAttributes(source, target pcommon.Resource) {
	targetAttr := target.Attributes()
	source.Attributes().Range(func(k string, v pcommon.Value) bool {
		if _, exists := targetAttr.Get(k); exists {
			targetValue := targetAttr.PutEmpty(k)
			v.CopyTo(targetValue)
		}
		return true
	})
}

func (r *MetricsGenReceiver) Shutdown(_ context.Context) error {
	if r.cancel != nil {
		r.cancel()
	}
	r.settings.Logger.Info("finished generating metrics",
		zap.Uint64("datapoints", r.progress.datapoints.Load()),
		zap.String("duration", r.progress.duration().Round(time.Millisecond).String()),
		zap.Float64("data_points_per_second", r.progress.dataPointsPerSecond()),
	)
	return nil
}

// getNewRand returns a new random number generator seeded with the configured seed.
// This is NOT thread-safe, so it should only be used in a single goroutine.
func (r *MetricsGenReceiver) getNewRand() *rand.Rand {
	return rand.New(rand.NewSource(r.baseRand.Int63()))
}
