module github.com/mrexodia/llama-observer-proxy

go 1.23.0

require (
	github.com/mrexodia/logging-proxy v0.0.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/andybalholm/brotli v1.2.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
)

replace github.com/mrexodia/logging-proxy => ../logging-proxy
