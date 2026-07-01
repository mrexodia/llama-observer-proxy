package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func (s *requestState) startPoller() {
	s.snapshotProps("props_start.json")
	go s.pollLoop()
}

func (s *requestState) stopPoller() {
	s.once.Do(func() {
		close(s.stop)
	})
}

func (s *requestState) pollLoop() {
	metricsFile, _ := os.OpenFile(filepath.Join(s.dir, "metrics.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if metricsFile != nil {
		defer metricsFile.Close()
	}
	slotsFile, _ := os.OpenFile(filepath.Join(s.dir, "slots.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if slotsFile != nil {
		defer slotsFile.Close()
	}

	ticker := time.NewTicker(s.logger.cfg.PollInterval())
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return
		default:
		}
		s.pollOnce(metricsFile, slotsFile)
		select {
		case <-s.stop:
			return
		case <-ticker.C:
		}
	}
}

func (s *requestState) pollOnce(metricsFile, slotsFile *os.File) {
	now := time.Now()
	if metricsFile != nil {
		entry := s.fetchMetrics(now)
		appendJSONLine(metricsFile, entry)
		if metrics, ok := entry["metrics"].(map[string]any); ok {
			s.update(func(summary *Summary) {
				summary.PollSamples++
				summary.LastMetrics = metrics
			})
		}
	}
	if slotsFile != nil {
		appendJSONLine(slotsFile, s.fetchJSONEndpoint("slots", now))
	}
	s.writeSummary()
}

func (s *requestState) snapshotProps(filename string) {
	entry := s.fetchJSONEndpoint("props", time.Now())
	writeJSONAtomic(filepath.Join(s.dir, filename), entry)
}

func (s *requestState) fetchMetrics(ts time.Time) map[string]any {
	body, status, err := s.fetchEndpoint("metrics")
	entry := map[string]any{
		"ts":     ts,
		"status": status,
	}
	if err != nil {
		entry["ok"] = false
		entry["error"] = err.Error()
		entry["body"] = string(body)
		return entry
	}
	entry["ok"] = true
	entry["metrics"] = parsePrometheusMetrics(body)
	return entry
}

func (s *requestState) fetchJSONEndpoint(endpoint string, ts time.Time) map[string]any {
	body, status, err := s.fetchEndpoint(endpoint)
	entry := map[string]any{
		"ts":     ts,
		"status": status,
	}
	if err != nil {
		entry["ok"] = false
		entry["error"] = err.Error()
		entry["body"] = string(body)
		return entry
	}
	entry["ok"] = true
	var value any
	if err := json.Unmarshal(body, &value); err != nil {
		entry["ok"] = false
		entry["error"] = err.Error()
		entry["body"] = string(body)
		return entry
	}
	entry["data"] = value
	return entry
}

func (s *requestState) fetchEndpoint(endpoint string) ([]byte, int, error) {
	if s.model == "" {
		return nil, 0, fmt.Errorf("model is empty")
	}
	u := *s.logger.upstream
	u.Path = joinURLPath(u.Path, endpoint)
	q := url.Values{}
	q.Set("model", s.model)
	q.Set("autoload", "0")
	u.RawQuery = q.Encode()

	ctx, cancel := context.WithTimeout(context.Background(), s.logger.cfg.PollTimeout())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, 0, err
	}
	resp, err := s.logger.client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return body, resp.StatusCode, readErr
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return body, resp.StatusCode, fmt.Errorf("GET %s returned %s", u.String(), resp.Status)
	}
	return body, resp.StatusCode, nil
}

func parsePrometheusMetrics(body []byte) map[string]any {
	out := map[string]any{}
	scanner := bufio.NewScanner(strings.NewReader(string(body)))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		value, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			out[fields[0]] = fields[1]
			continue
		}
		out[fields[0]] = value
	}
	return out
}

func joinURLPath(base, elem string) string {
	base = strings.TrimRight(base, "/")
	if base == "" {
		base = "/"
	}
	if base == "/" {
		return "/" + strings.TrimLeft(elem, "/")
	}
	return base + "/" + strings.TrimLeft(elem, "/")
}
