package main

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server struct {
		Host string `yaml:"host"`
		Port int    `yaml:"port"`
	} `yaml:"server"`
	Upstream struct {
		BaseURL string `yaml:"base_url"`
	} `yaml:"upstream"`
	Logging struct {
		LogDir  string `yaml:"log_dir"`
		Console bool   `yaml:"console"`
	} `yaml:"logging"`
	Observer struct {
		PollIntervalMS int   `yaml:"poll_interval_ms"`
		PollTimeoutMS  int   `yaml:"poll_timeout_ms"`
		MaxParseBytes  int64 `yaml:"max_parse_bytes"`
	} `yaml:"observer"`
}

func LoadConfig(path string) (Config, error) {
	var cfg Config
	cfg.Server.Host = "127.0.0.1"
	cfg.Server.Port = 5602
	cfg.Upstream.BaseURL = "http://127.0.0.1:8080/"
	cfg.Logging.LogDir = "logs"
	cfg.Observer.PollIntervalMS = 1000
	cfg.Observer.PollTimeoutMS = 2000
	cfg.Observer.MaxParseBytes = 50 * 1024 * 1024

	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return cfg, err
	}
	if cfg.Server.Host == "" {
		cfg.Server.Host = "127.0.0.1"
	}
	if cfg.Server.Port == 0 {
		cfg.Server.Port = 5602
	}
	if cfg.Upstream.BaseURL == "" {
		cfg.Upstream.BaseURL = "http://127.0.0.1:8080/"
	}
	if cfg.Logging.LogDir == "" {
		cfg.Logging.LogDir = "logs"
	}
	if cfg.Observer.PollIntervalMS <= 0 {
		cfg.Observer.PollIntervalMS = 1000
	}
	if cfg.Observer.PollTimeoutMS <= 0 {
		cfg.Observer.PollTimeoutMS = 2000
	}
	if cfg.Observer.MaxParseBytes <= 0 {
		cfg.Observer.MaxParseBytes = 50 * 1024 * 1024
	}
	return cfg, nil
}

func (c Config) PollInterval() time.Duration {
	return time.Duration(c.Observer.PollIntervalMS) * time.Millisecond
}

func (c Config) PollTimeout() time.Duration {
	return time.Duration(c.Observer.PollTimeoutMS) * time.Millisecond
}
