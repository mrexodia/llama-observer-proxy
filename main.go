package main

import (
	"flag"
	"fmt"
	loggingproxy "github.com/mrexodia/logging-proxy"
	"log"
	"net/http"
	"os"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to config file")
	flag.Parse()

	cfg, err := LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}
	logger, err := NewObserverLogger(cfg)
	if err != nil {
		log.Fatalf("failed to create observer logger: %v", err)
	}

	proxy := loggingproxy.NewProxyServer("")
	if err := proxy.AddRoute("/", cfg.Upstream.BaseURL, logger); err != nil {
		log.Fatalf("failed to add proxy route: %v", err)
	}

	var handler http.Handler = InjectMiddleware{
		Next:    proxy,
		Enabled: true,
	}

	addr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)
	log.Printf("llama observer proxy listening on %s", addr)
	log.Printf("upstream: %s", cfg.Upstream.BaseURL)
	log.Printf("logs: %s", cfg.Logging.LogDir)
	if err := http.ListenAndServe(addr, handler); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
