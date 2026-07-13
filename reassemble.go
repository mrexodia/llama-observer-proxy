package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"net/http"
	"sort"
	"strings"
)

// responseBuffer captures the downstream handler's response so it can be
// reassembled before anything is written to the real client.
type responseBuffer struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func newResponseBuffer() *responseBuffer {
	return &responseBuffer{header: make(http.Header), status: http.StatusOK}
}

func (b *responseBuffer) Header() http.Header         { return b.header }
func (b *responseBuffer) WriteHeader(status int)      { b.status = status }
func (b *responseBuffer) Write(p []byte) (int, error) { return b.body.Write(p) }
func (b *responseBuffer) Flush()                      {}

func (b *responseBuffer) isSSE() bool {
	return strings.HasPrefix(b.header.Get("Content-Type"), "text/event-stream")
}

// writeThrough forwards the buffered response to the client unchanged.
func (b *responseBuffer) writeThrough(w http.ResponseWriter) {
	for key, values := range b.header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(b.status)
	_, _ = w.Write(b.body.Bytes())
}

type toolCallAccumulator struct {
	id        string
	callType  string
	name      string
	arguments strings.Builder
}

type choiceAccumulator struct {
	role         string
	content      strings.Builder
	reasoning    strings.Builder
	finishReason any
	toolCalls    map[int]*toolCallAccumulator
}

// reassembleSSE folds a buffered chat.completion.chunk SSE stream into a
// single non-streaming chat.completion JSON body. Returns false when the
// stream does not look like a chat completion (caller should pass the
// original bytes through).
func reassembleSSE(raw []byte) ([]byte, bool) {
	var (
		id                string
		object            bool
		created           any
		model             any
		systemFingerprint any
		usage             any
		timings           any
		choices           = map[int]*choiceAccumulator{}
	)

	scanner := bufio.NewScanner(bytes.NewReader(raw))
	scanner.Buffer(make([]byte, 64*1024), 64*1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "" || data == "[DONE]" {
			continue
		}
		var chunk map[string]any
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			continue
		}
		if errObj, ok := chunk["error"]; ok {
			body, _ := json.Marshal(map[string]any{"error": errObj})
			return body, true
		}
		if v, ok := chunk["id"].(string); ok && id == "" {
			id = v
		}
		if v, ok := chunk["created"]; ok && created == nil {
			created = v
		}
		if v, ok := chunk["model"]; ok && model == nil {
			model = v
		}
		if v, ok := chunk["system_fingerprint"]; ok && systemFingerprint == nil {
			systemFingerprint = v
		}
		if v, ok := chunk["usage"]; ok && v != nil {
			usage = v
		}
		if v, ok := chunk["timings"]; ok && v != nil {
			timings = v
		}
		chunkChoices, _ := chunk["choices"].([]any)
		for _, c := range chunkChoices {
			object = true
			choice, _ := c.(map[string]any)
			index := 0
			if v, ok := choice["index"].(float64); ok {
				index = int(v)
			}
			acc := choices[index]
			if acc == nil {
				acc = &choiceAccumulator{toolCalls: map[int]*toolCallAccumulator{}}
				choices[index] = acc
			}
			if fr, ok := choice["finish_reason"]; ok && fr != nil {
				acc.finishReason = fr
			}
			delta, _ := choice["delta"].(map[string]any)
			if delta == nil {
				continue
			}
			if v, ok := delta["role"].(string); ok && v != "" {
				acc.role = v
			}
			if v, ok := delta["content"].(string); ok {
				acc.content.WriteString(v)
			}
			if v, ok := delta["reasoning_content"].(string); ok {
				acc.reasoning.WriteString(v)
			}
			deltaCalls, _ := delta["tool_calls"].([]any)
			for _, dc := range deltaCalls {
				call, _ := dc.(map[string]any)
				callIndex := 0
				if v, ok := call["index"].(float64); ok {
					callIndex = int(v)
				}
				tc := acc.toolCalls[callIndex]
				if tc == nil {
					tc = &toolCallAccumulator{}
					acc.toolCalls[callIndex] = tc
				}
				if v, ok := call["id"].(string); ok && v != "" {
					tc.id = v
				}
				if v, ok := call["type"].(string); ok && v != "" {
					tc.callType = v
				}
				if fn, ok := call["function"].(map[string]any); ok {
					if v, ok := fn["name"].(string); ok && v != "" {
						tc.name = v
					}
					if v, ok := fn["arguments"].(string); ok {
						tc.arguments.WriteString(v)
					}
				}
			}
		}
	}
	if !object {
		return nil, false
	}

	indices := make([]int, 0, len(choices))
	for index := range choices {
		indices = append(indices, index)
	}
	sort.Ints(indices)

	outChoices := make([]any, 0, len(indices))
	for _, index := range indices {
		acc := choices[index]
		role := acc.role
		if role == "" {
			role = "assistant"
		}
		message := map[string]any{
			"role":    role,
			"content": acc.content.String(),
		}
		if acc.reasoning.Len() > 0 {
			message["reasoning_content"] = acc.reasoning.String()
		}
		if len(acc.toolCalls) > 0 {
			callIndices := make([]int, 0, len(acc.toolCalls))
			for ci := range acc.toolCalls {
				callIndices = append(callIndices, ci)
			}
			sort.Ints(callIndices)
			calls := make([]any, 0, len(callIndices))
			for _, ci := range callIndices {
				tc := acc.toolCalls[ci]
				callType := tc.callType
				if callType == "" {
					callType = "function"
				}
				calls = append(calls, map[string]any{
					"id":   tc.id,
					"type": callType,
					"function": map[string]any{
						"name":      tc.name,
						"arguments": tc.arguments.String(),
					},
				})
			}
			message["tool_calls"] = calls
		}
		outChoices = append(outChoices, map[string]any{
			"index":         index,
			"message":       message,
			"finish_reason": acc.finishReason,
		})
	}

	out := map[string]any{
		"id":      id,
		"object":  "chat.completion",
		"created": created,
		"model":   model,
		"choices": outChoices,
	}
	if systemFingerprint != nil {
		out["system_fingerprint"] = systemFingerprint
	}
	if usage != nil {
		out["usage"] = usage
	}
	if timings != nil {
		out["timings"] = timings
	}
	body, err := json.Marshal(out)
	if err != nil {
		return nil, false
	}
	return body, true
}
