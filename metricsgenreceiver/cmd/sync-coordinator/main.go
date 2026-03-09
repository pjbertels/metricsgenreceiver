// sync-coordinator drives multiple metricsgenreceiver instances via NATS so they
// all generate metrics for the same simulated timestamp, avoiding out-of-order data.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"gopkg.in/yaml.v3"
)

type config struct {
	NatsURL          string        `yaml:"nats_url"`
	SubjectJoin      string        `yaml:"subject_join"`
	SubjectTick      string        `yaml:"subject_tick"`
	SubjectDone      string        `yaml:"subject_done"`
	NumInstances     int           `yaml:"num_instances"`
	BaseSeed         int64         `yaml:"base_seed"`
	ScalePerInstance int           `yaml:"scale_per_instance"`
	StartDelay       time.Duration `yaml:"start_delay"` // delay after all joined before first tick (so last joiner can subscribe)
	StartTime        string        `yaml:"start_time"`  // RFC3339
	EndTime          string        `yaml:"end_time"`    // RFC3339
	Interval         time.Duration `yaml:"interval"`
	DoneTimeout      time.Duration `yaml:"done_timeout"` // max wait for N dones per tick (default 5m)
}

func main() {
	configPath := flag.String("config", "", "Path to YAML config file")
	flag.Parse()
	if *configPath == "" {
		fmt.Fprintf(os.Stderr, "Usage: %s -config <path>\n", os.Args[0])
		os.Exit(1)
	}

	data, err := os.ReadFile(*configPath)
	if err != nil {
		log.Fatalf("read config: %v", err)
	}
	var cfg config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		log.Fatalf("parse config: %v", err)
	}
	if cfg.DoneTimeout == 0 {
		cfg.DoneTimeout = 5 * time.Minute
	}
	if cfg.SubjectJoin != "" && cfg.StartDelay == 0 {
		cfg.StartDelay = 5 * time.Second
	}
	if cfg.NumInstances < 1 {
		log.Fatal("num_instances must be at least 1")
	}
	if cfg.SubjectTick == "" || cfg.SubjectDone == "" {
		log.Fatal("subject_tick and subject_done are required")
	}
	if cfg.SubjectJoin != "" && cfg.ScalePerInstance < 0 {
		log.Fatal("scale_per_instance must be non-negative when subject_join is set")
	}
	if cfg.NatsURL == "" {
		cfg.NatsURL = nats.DefaultURL
	}

	startTime, err := time.Parse(time.RFC3339, cfg.StartTime)
	if err != nil {
		log.Fatalf("start_time: %v", err)
	}
	endTime, err := time.Parse(time.RFC3339, cfg.EndTime)
	if err != nil {
		log.Fatalf("end_time: %v", err)
	}
	if cfg.Interval < time.Second {
		log.Fatal("interval must be at least 1s")
	}

	nc, err := nats.Connect(cfg.NatsURL)
	if err != nil {
		log.Fatalf("connect to NATS: %v", err)
	}
	defer nc.Drain()

	// If join subject is set, wait for N instances to join before starting the tick loop.
	if cfg.SubjectJoin != "" {
		log.Printf("join mode enabled: waiting for %d instance(s) on %s before publishing any tick", cfg.NumInstances, cfg.SubjectJoin)
		type assignmentPayload struct {
			Seed             int64  `json:"seed"`
			StartTime        string `json:"start_time"`
			EndTime          string `json:"end_time"`
			InstanceID       string `json:"instance_id"`
			InstanceIDOffset int    `json:"instance_id_offset"`
		}
		var joinMu sync.Mutex
		joinCount := 0
		allJoined := make(chan struct{})
		_, err = nc.Subscribe(cfg.SubjectJoin, func(msg *nats.Msg) {
			joinMu.Lock()
			slot := joinCount
			if slot >= cfg.NumInstances {
				joinMu.Unlock()
				log.Printf("join request ignored (already have %d instances)", cfg.NumInstances)
				return
			}
			joinCount++
			reached := joinCount == cfg.NumInstances
			joinMu.Unlock()

			log.Printf("join received: instance %d/%d joining", joinCount, cfg.NumInstances)

			seed := cfg.BaseSeed + int64(slot)
			offset := slot * cfg.ScalePerInstance
			payload := assignmentPayload{
				Seed:             seed,
				StartTime:        cfg.StartTime,
				EndTime:          cfg.EndTime,
				InstanceID:       fmt.Sprintf("%d", slot),
				InstanceIDOffset: offset,
			}
			data, _ := json.Marshal(payload)
			if err := msg.Respond(data); err != nil {
				log.Printf("respond to join: %v", err)
				return
			}
			log.Printf("join assigned slot %d: seed=%d instance_id=%s instance_id_offset=%d", slot, seed, payload.InstanceID, offset)
			if reached {
				close(allJoined)
			}
		})
		if err != nil {
			log.Fatalf("subscribe to join: %v", err)
		}
		log.Printf("waiting for %d instance(s) on %s ...", cfg.NumInstances, cfg.SubjectJoin)
		<-allJoined
		log.Printf("all %d instances joined, starting tick loop", cfg.NumInstances)
		if cfg.StartDelay > 0 {
			log.Printf("waiting %v for receivers to subscribe to tick subject before first tick", cfg.StartDelay)
			time.Sleep(cfg.StartDelay)
		}
	} else {
		log.Printf("no subject_join configured; starting tick loop immediately (receivers must already be subscribed)")
	}

	// Subscribe to done before publishing any tick so we don't miss messages.
	type doneMsg struct {
		InstanceID string `json:"instance_id"`
		TS         string `json:"ts"`
	}
	var (
		mu        sync.Mutex
		doneCount int
		currentTS string
	)
	sub, err := nc.Subscribe(cfg.SubjectDone, func(msg *nats.Msg) {
		var d doneMsg
		if err := json.Unmarshal(msg.Data, &d); err != nil {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if d.TS == currentTS {
			doneCount++
		}
	})
	if err != nil {
		log.Fatalf("subscribe to done: %v", err)
	}
	defer sub.Unsubscribe()

	currentTime := startTime
	tickIndex := 0
	for currentTime.UnixNano() < endTime.UnixNano() {
		ts := currentTime.Format(time.RFC3339Nano)
		mu.Lock()
		currentTS = ts
		doneCount = 0
		mu.Unlock()

		if err := nc.Publish(cfg.SubjectTick, []byte(ts)); err != nil {
			log.Fatalf("publish tick: %v", err)
		}
		log.Printf("tick %d: published %s", tickIndex, ts)

		deadline := time.Now().Add(cfg.DoneTimeout)
		for {
			mu.Lock()
			n := doneCount
			mu.Unlock()
			if n >= cfg.NumInstances {
				break
			}
			if time.Now().After(deadline) {
				log.Printf("tick %d: timeout waiting for dones (got %d/%d)", tickIndex, n, cfg.NumInstances)
				break
			}
			time.Sleep(100 * time.Millisecond)
		}

		currentTime = currentTime.Add(cfg.Interval)
		tickIndex++
	}

	// Publish one final tick with end_time so receivers know to stop.
	mu.Lock()
	currentTS = endTime.Format(time.RFC3339Nano)
	doneCount = 0
	mu.Unlock()
	_ = nc.Publish(cfg.SubjectTick, []byte(endTime.Format(time.RFC3339Nano)))
	log.Printf("published end tick %s", endTime.Format(time.RFC3339Nano))
}
