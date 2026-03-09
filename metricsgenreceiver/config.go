package metricsgenreceiver

import (
	"fmt"
	"time"

	"github.com/elastic/metricsgenreceiver/metricsgenreceiver/internal/distribution"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type Config struct {
	StartTime                         time.Time                    `mapstructure:"start_time"`
	StartNowMinus                     time.Duration                `mapstructure:"start_now_minus"`
	EndTime                           time.Time                    `mapstructure:"end_time"`
	EndNowMinus                       time.Duration                `mapstructure:"end_now_minus"`
	Interval                          time.Duration                `mapstructure:"interval"`
	IntervalJitterStdDev              time.Duration                `mapstructure:"interval_jitter_std_dev"`
	RealTime                          bool                         `mapstructure:"real_time"`
	ExitAfterEnd                      bool                         `mapstructure:"exit_after_end"`
	ExitAfterEndTimeout               time.Duration                `mapstructure:"exit_after_end_timeout"`
	Threads                           int                          `mapstructure:"threads"`
	Seed                              int64                        `mapstructure:"seed"`
	Scenarios                         []ScenarioCfg                `mapstructure:"scenarios"`
	Distribution                      distribution.DistributionCfg `mapstructure:"distribution"`
	ExponentialHistogramsTemplatePath string                       `mapstructure:"exponential_histograms_template_path"`
	Sync                              *SyncConfig                  `mapstructure:"sync"`
}

// SyncConfig configures NATS-based coordination so multiple receiver instances
// generate metrics for the same simulated timestamp, avoiding out-of-order data.
type SyncConfig struct {
	Enabled                      bool   `mapstructure:"enabled"`
	GetAssignmentFromCoordinator bool   `mapstructure:"get_assignment_from_coordinator"`
	NatsURL                      string `mapstructure:"nats_url"`
	SubjectJoin                  string `mapstructure:"subject_join"`
	SubjectTick                  string `mapstructure:"subject_tick"`
	SubjectDone                  string `mapstructure:"subject_done"`
	InstanceID                   string `mapstructure:"instance_id"`
}

type ScenarioCfg struct {
	Path                string         `mapstructure:"path"`
	Scale               int            `mapstructure:"scale"`
	Concurrency         int            `mapstructure:"concurrency"`
	Churn               int            `mapstructure:"churn"`
	InstanceIDOffset    int            `mapstructure:"instance_id_offset"`
	TemplateVars        map[string]any `mapstructure:"template_vars"`
	TemporalityOverride string         `mapstructure:"temporality_override"`
	HistogramOverride   string         `mapstructure:"histogram_override"`
}

func (c ScenarioCfg) AggregationTemporalityOverride() pmetric.AggregationTemporality {
	switch c.TemporalityOverride {
	case "cumulative":
		return pmetric.AggregationTemporalityCumulative
	case "delta":
		return pmetric.AggregationTemporalityDelta
	default:
		return pmetric.AggregationTemporalityUnspecified
	}
}

func (c Config) GetExponentialHistogramsTemplatePath() string {
	if c.ExponentialHistogramsTemplatePath != "" {
		return c.ExponentialHistogramsTemplatePath
	}
	return "builtin/exponential-histograms-low-frequency.ndjson"
}

func (c ScenarioCfg) ForceExponentialHistograms() bool {
	return c.HistogramOverride == "exponential"
}

func createDefaultConfig() component.Config {
	return &Config{
		Threads:      4,
		Seed:         0,
		Scenarios:    make([]ScenarioCfg, 0),
		Distribution: distribution.DefaultDistribution,
	}
}

func (cfg *Config) Validate() error {
	if cfg.Interval.Seconds() < 1 {
		return fmt.Errorf("the interval has to be set to at least 1 second (1s)")
	}

	getAssignment := cfg.Sync != nil && cfg.Sync.Enabled && cfg.Sync.GetAssignmentFromCoordinator
	if !getAssignment && cfg.StartTime.After(cfg.EndTime) {
		return fmt.Errorf("start_time must be before end_time")
	}

	if cfg.Threads < 0 {
		return fmt.Errorf("threads must be a positive number")
	}

	for _, scn := range cfg.Scenarios {
		concurrency := scn.Concurrency
		if cfg.Threads > 0 {
			concurrency = cfg.Threads
		}
		if concurrency != 0 && scn.Scale%concurrency != 0 {
			return fmt.Errorf("scale must be a multiple of concurrency")
		}
		if concurrency < 0 {
			return fmt.Errorf("concurrency must be a positive number")
		}
		if !getAssignment && scn.InstanceIDOffset < 0 {
			return fmt.Errorf("instance_id_offset must be non-negative")
		}
	}
	if cfg.Sync != nil && cfg.Sync.Enabled {
		if cfg.Sync.NatsURL == "" {
			return fmt.Errorf("sync.nats_url is required when sync is enabled")
		}
		if cfg.Sync.SubjectTick == "" {
			return fmt.Errorf("sync.subject_tick is required when sync is enabled")
		}
		if cfg.Sync.SubjectDone == "" {
			return fmt.Errorf("sync.subject_done is required when sync is enabled")
		}
		if cfg.Sync.GetAssignmentFromCoordinator {
			if cfg.Sync.SubjectJoin == "" {
				return fmt.Errorf("sync.subject_join is required when sync.get_assignment_from_coordinator is true")
			}
		} else {
			if cfg.Sync.InstanceID == "" {
				return fmt.Errorf("sync.instance_id is required when sync is enabled and get_assignment_from_coordinator is false")
			}
		}
	}
	return nil
}
