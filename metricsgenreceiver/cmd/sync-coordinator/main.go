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
	NatsURL      string        `yaml:"nats_url"`
	SubjectTick  string        `yaml:"subject_tick"`
	SubjectDone  string        `yaml:"subject_done"`
	NumInstances int           `yaml:"num_instances"`
	StartTime    string        `yaml:"start_time"` // RFC3339
	EndTime      string        `yaml:"end_time"`   // RFC3339
	Interval     time.Duration `yaml:"interval"`
	DoneTimeout  time.Duration `yaml:"done_timeout"` // max wait for N dones per tick (default 5m)
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
	if cfg.NumInstances < 1 {
		log.Fatal("num_instances must be at least 1")
	}
	if cfg.SubjectTick == "" || cfg.SubjectDone == "" {
		log.Fatal("subject_tick and subject_done are required")
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
