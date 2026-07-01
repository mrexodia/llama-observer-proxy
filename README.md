# llama-observer-proxy

A llama.cpp-aware proxy built on top of `github.com/mrexodia/logging-proxy`.

It keeps raw HTTP request/response logs and adds llama.cpp-specific observability:

- injects lightweight diagnostic request options for chat completions
- records timestamped SSE events
- polls `/metrics`, `/slots`, and `/props` for the requested model
- uses `autoload=0` only for telemetry polling
- writes a per-request `summary.json`
- stores raw request and response HTTP without redaction so they can be replayed

## Run

```sh
go build
./llama-observer-proxy -config config.yaml
```

Default config listens on `127.0.0.1:5602` and forwards to `http://127.0.0.1:8080/`.

Point OpenAI-compatible clients at:

```text
http://127.0.0.1:5602/v1
```

## Request injection

For LLM requests, the proxy injects:

```json
{
  "stream": true,
  "stream_options": { "include_usage": true },
  "timings_per_token": true,
  "return_progress": true
}
```

It does not inject `verbose`, `n_probs`, or `post_sampling_probs`.

## Artifacts

Each request gets a directory like:

```text
logs/2026-07-01_14-20-15.770_2cdc9642/
  request.http
  request_metadata.json
  response.http
  response_metadata.json
  sse_events.jsonl
  metrics.jsonl
  slots.jsonl
  props_start.json
  props_end.json
  summary.json
```

Telemetry polls include `autoload=0` so polling does not load models. Normal client requests are not modified with `autoload=0`, so pi can still autoload models.
