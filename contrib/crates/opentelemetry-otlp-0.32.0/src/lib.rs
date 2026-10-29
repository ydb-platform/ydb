//! # OpenTelemetry OTLP Exporter
//!
//! The OTLP Exporter enables exporting telemetry data (logs, metrics, and traces) in the
//! OpenTelemetry Protocol (OTLP) format to compatible backends. These backends include:
//!
//! - OpenTelemetry Collector
//! - Open-source observability tools (Prometheus, Jaeger, etc.)
//! - Vendor-specific monitoring platforms
//!
//! This crate supports sending OTLP data via:
//! - gRPC
//! - HTTP (binary protobuf or JSON)
//!
//! ## Quickstart with OpenTelemetry Collector
//!
//! The examples below show traces, but the same pattern applies to metrics
//! ([`MetricExporter`]) and logs ([`LogExporter`]) — just swap the exporter
//! builder and the corresponding SDK provider.
//!
//! ### HTTP Transport (Port 4318)
//!
//! Run the OpenTelemetry Collector:
//!
//! ```shell
//! $ docker run -p 4318:4318 otel/opentelemetry-collector:latest
//! ```
//!
//! Configure your application to export traces via HTTP:
//!
//! ```no_run
//! # #[cfg(all(feature = "trace", feature = "http-proto"))]
//! # {
//! use opentelemetry::global;
//! use opentelemetry::trace::Tracer;
//! use opentelemetry_otlp::Protocol;
//! use opentelemetry_otlp::WithExportConfig;
//!
//! fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
//!     // Initialize OTLP exporter using HTTP binary protocol
//!     let otlp_exporter = opentelemetry_otlp::SpanExporter::builder()
//!         .with_http()
//!         .with_protocol(Protocol::HttpBinary)
//!         .build()?;
//!
//!     // Create a tracer provider with the exporter
//!     let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
//!         .with_batch_exporter(otlp_exporter)
//!         .build();
//!
//!     // Set it as the global provider
//!     global::set_tracer_provider(tracer_provider);
//!
//!     // Get a tracer and create spans
//!     let tracer = global::tracer("my_tracer");
//!     tracer.in_span("doing_work", |_cx| {
//!         // Your application logic here...
//!     });
//!
//!     Ok(())
//! # }
//! }
//! ```
//!
//! ### gRPC Transport (Port 4317)
//!
//! Run the OpenTelemetry Collector:
//!
//! ```shell
//! $ docker run -p 4317:4317 otel/opentelemetry-collector:latest
//! ```
//!
//! Configure your application to export traces via gRPC (the tonic client requires a Tokio runtime):
//!
//! - With `[tokio::main]`
//!
//! ```no_run
//! # #[cfg(all(feature = "trace", feature = "grpc-tonic"))]
//! # {
//! use opentelemetry::{global, trace::Tracer};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
//!     // Initialize OTLP exporter using gRPC (Tonic)
//!     let otlp_exporter = opentelemetry_otlp::SpanExporter::builder()
//!         .with_tonic()
//!         .build()?;
//!
//!     // Create a tracer provider with the exporter
//!     let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
//!         .with_batch_exporter(otlp_exporter)
//!         .build();
//!
//!     // Set it as the global provider
//!     global::set_tracer_provider(tracer_provider);
//!
//!     // Get a tracer and create spans
//!     let tracer = global::tracer("my_tracer");
//!     tracer.in_span("doing_work", |_cx| {
//!         // Your application logic here...
//!     });
//!
//!     Ok(())
//! # }
//! }
//! ```
//!
//! - Without `[tokio::main]`
//!
//! ```no_run
//! # #[cfg(all(feature = "trace", feature = "grpc-tonic"))]
//! # {
//! use opentelemetry::{global, trace::Tracer};
//!
//! fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
//!     // Initialize OTLP exporter using gRPC (Tonic)
//!     let rt = tokio::runtime::Runtime::new()?;
//!     let tracer_provider = rt.block_on(async {
//!         let exporter = opentelemetry_otlp::SpanExporter::builder()
//!             .with_tonic()
//!             .build()
//!             .expect("Failed to create span exporter");
//!         opentelemetry_sdk::trace::SdkTracerProvider::builder()
//!             .with_batch_exporter(exporter)
//!             .build()
//!     });
//!
//!     // Set it as the global provider
//!     global::set_tracer_provider(tracer_provider);
//!
//!     // Get a tracer and create spans
//!     let tracer = global::tracer("my_tracer");
//!     tracer.in_span("doing_work", |_cx| {
//!         // Your application logic here...
//!     });
//!
//!     // Ensure the runtime (`rt`) remains active until the program ends
//!     Ok(())
//! # }
//! }
//! ```
//!
//! ## Using with Jaeger
//!
//! Jaeger natively supports the OTLP protocol, making it easy to send traces directly:
//!
//! ```shell
//! $ docker run -p 16686:16686 -p 4317:4317 -e COLLECTOR_OTLP_ENABLED=true jaegertracing/all-in-one:latest
//! ```
//!
//! After running your application configured with the OTLP exporter, view traces at:
//! `http://localhost:16686`
//!
//! ## Using with Prometheus
//!
//! Prometheus natively supports accepting metrics via the OTLP protocol
//! (HTTP/protobuf). You can [run
//! Prometheus](https://prometheus.io/docs/prometheus/latest/installation/) with
//! the following command:
//!
//! ```shell
//! docker run -p 9090:9090 -v ./prometheus.yml:/etc/prometheus/prometheus.yml prom/prometheus --config.file=/etc/prometheus/prometheus.yml --web.enable-otlp-receiver
//! ```
//!
//! (An empty prometheus.yml file is sufficient for this example.)
//!
//! Modify your application to export metrics via OTLP:
//!
//! ```no_run
//! # #[cfg(all(feature = "metrics", feature = "http-proto"))]
//! # {
//! use opentelemetry::global;
//! use opentelemetry::metrics::Meter;
//! use opentelemetry::KeyValue;
//! use opentelemetry_otlp::Protocol;
//! use opentelemetry_otlp::WithExportConfig;
//!
//! fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
//!     // Initialize OTLP exporter using HTTP binary protocol
//!     let exporter = opentelemetry_otlp::MetricExporter::builder()
//!         .with_http()
//!         .with_protocol(Protocol::HttpBinary)
//!         .with_endpoint("http://localhost:9090/api/v1/otlp/v1/metrics")
//!         .build()?;
//!
//!     // Create a meter provider with the OTLP Metric exporter
//!     let meter_provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
//!         .with_periodic_exporter(exporter)
//!         .build();
//!     global::set_meter_provider(meter_provider.clone());
//!
//!     // Get a meter
//!     let meter = global::meter("my_meter");
//!
//!     // Create a metric
//!     let counter = meter.u64_counter("my_counter").build();
//!     counter.add(1, &[KeyValue::new("key", "value")]);
//!
//!     // Shutdown the meter provider. This will trigger an export of all metrics.
//!     meter_provider.shutdown()?;
//!
//!     Ok(())
//! # }
//! }
//! ```
//!
//! After running your application configured with the OTLP exporter, view metrics at:
//! `http://localhost:9090`
//!
//! # Environment Variables
//!
//! The OTLP exporter respects the following environment variables, as defined by the
//! [OpenTelemetry specification]. Programmatic configuration via builder methods
//! takes precedence over environment variables. Signal-specific variables take
//! precedence over the generic `OTEL_EXPORTER_OTLP_*` variables.
//!
//! [OpenTelemetry specification]: https://opentelemetry.io/docs/specs/otel/protocol/exporter/
//!
//! ## General (all signals)
//!
//! | Variable | Description | Default |
//! |---|---|---|
//! | `OTEL_EXPORTER_OTLP_ENDPOINT` | Target URL for the exporter. For HTTP, signal paths (`/v1/traces`, `/v1/metrics`, `/v1/logs`) are appended automatically. | `http://localhost:4318` (HTTP), `http://localhost:4317` (gRPC) |
//! | `OTEL_EXPORTER_OTLP_PROTOCOL` | Transport protocol. Valid values: `grpc`, `http/protobuf`, `http/json`. Requires the corresponding crate feature. | Feature-dependent |
//! | `OTEL_EXPORTER_OTLP_TIMEOUT` | Maximum wait time (in milliseconds) for the backend to process each batch. | `10000` |
//! | `OTEL_EXPORTER_OTLP_HEADERS` | Key-value pairs for request headers. Format: `key1=value1,key2=value2`. Values are URL-decoded. | (none) |
//! | `OTEL_EXPORTER_OTLP_COMPRESSION` | Compression algorithm. Valid values: `gzip`, `zstd`. | (none) |
//!
//! ## Traces
//!
//! | Variable | Description |
//! |---|---|
//! | `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | Signal-specific endpoint for trace exports. |
//! | `OTEL_EXPORTER_OTLP_TRACES_PROTOCOL` | Signal-specific protocol for trace exports. Valid values: `grpc`, `http/protobuf`, `http/json`. |
//! | `OTEL_EXPORTER_OTLP_TRACES_TIMEOUT` | Signal-specific timeout (in milliseconds) for trace exports. |
//! | `OTEL_EXPORTER_OTLP_TRACES_HEADERS` | Signal-specific headers for trace exports. |
//! | `OTEL_EXPORTER_OTLP_TRACES_COMPRESSION` | Signal-specific compression for trace exports. |
//!
//! ## Metrics
//!
//! | Variable | Description |
//! |---|---|
//! | `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` | Signal-specific endpoint for metrics exports. |
//! | `OTEL_EXPORTER_OTLP_METRICS_PROTOCOL` | Signal-specific protocol for metrics exports. Valid values: `grpc`, `http/protobuf`, `http/json`. |
//! | `OTEL_EXPORTER_OTLP_METRICS_TIMEOUT` | Signal-specific timeout (in milliseconds) for metrics exports. |
//! | `OTEL_EXPORTER_OTLP_METRICS_HEADERS` | Signal-specific headers for metrics exports. |
//! | `OTEL_EXPORTER_OTLP_METRICS_COMPRESSION` | Signal-specific compression for metrics exports. |
//! | `OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE` | Temporality preference for metrics. Valid values: `cumulative`, `delta`, `lowmemory` (case-insensitive). | `cumulative` |
//!
//! ## Logs
//!
//! | Variable | Description |
//! |---|---|
//! | `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT` | Signal-specific endpoint for log exports. |
//! | `OTEL_EXPORTER_OTLP_LOGS_PROTOCOL` | Signal-specific protocol for log exports. Valid values: `grpc`, `http/protobuf`, `http/json`. |
//! | `OTEL_EXPORTER_OTLP_LOGS_TIMEOUT` | Signal-specific timeout (in milliseconds) for log exports. |
//! | `OTEL_EXPORTER_OTLP_LOGS_HEADERS` | Signal-specific headers for log exports. |
//! | `OTEL_EXPORTER_OTLP_LOGS_COMPRESSION` | Signal-specific compression for log exports. |
//!
//! # Feature Flags
//! The following feature flags can enable exporters for different telemetry signals:
//!
//! * `trace`: Includes the trace exporters.
//! * `metrics`: Includes the metrics exporters.
//! * `logs`: Includes the logs exporters.
//!
//! The following feature flags generate additional code and types:
//! * `serialize`: Enables serialization support for type defined in this crate via `serde`.
//!
//! The following feature flags offer additional configurations on gRPC:
//!
//! For users using `tonic` as grpc layer:
//! * `grpc-tonic`: Use `tonic` as grpc layer.
//! * `gzip-tonic`: Use gzip compression for `tonic` grpc layer.
//! * `zstd-tonic`: Use zstd compression for `tonic` grpc layer.
//! * `tls-ring`: Enable rustls TLS support using ring for `tonic`.
//! * `tls-aws-lc`: Enable rustls TLS support using aws-lc for `tonic`.
//! * `tls-provider-agnostic`: Provider-agnostic TLS — enables TLS code paths without bundling a specific
//!   crypto provider. Use this when you install a `CryptoProvider` globally
//!   (e.g., via `rustls-openssl` for FIPS/OpenSSL environments).
//! * `tls` (deprecated): Use `tls-ring` or `tls-aws-lc` instead.
//! * `tls-roots`: Adds system trust roots to rustls-based gRPC clients using the rustls-native-certs crate (use with `tls-ring` or `tls-aws-lc`).
//! * `tls-webpki-roots`: Embeds Mozilla's trust roots to rustls-based gRPC clients using the webpki-roots crate (use with `tls-ring` or `tls-aws-lc`).
//!
//! The following feature flags offer additional configurations on http:
//!
//! * `http-proto`: Use http as transport layer, protobuf as body format. This feature is enabled by default.
//! * `gzip-http`: Use gzip compression for HTTP transport.
//! * `zstd-http`: Use zstd compression for HTTP transport.
//! * `reqwest-blocking-client`: Use reqwest blocking http client. This feature is enabled by default.
//! * `reqwest-client`: Use reqwest http client.
//! * `reqwest-rustls`: Use reqwest with TLS with system trust roots via `rustls-native-certs` crate.
//! * `reqwest-rustls-webpki-roots`: Use reqwest with TLS with Mozilla's trust roots via `webpki-roots` crate.
//!
//! The following feature flags enable experimental retry support:
//!
//! * `experimental-grpc-retry`: Enable automatic retry with exponential backoff for gRPC exports.
//!   Requires a Tokio runtime (`rt-tokio` SDK feature is enabled transitively).
//! * `experimental-http-retry`: Enable automatic retry with exponential backoff for HTTP exports.
//!   Requires a Tokio runtime (`rt-tokio` SDK feature is enabled transitively).
//!
//! # Full Configuration Reference
//!
//!
//! There are two layers of configuration for the OTLP exporter:
//!
//! 1. **Exporter configuration** – controls how telemetry is sent (endpoint, transport, headers, TLS, compression, timeout).
//!    Built via the signal-specific builder: [`SpanExporter::builder()`], [`MetricExporter::builder()`], [`LogExporter::builder()`].
//! 2. **Provider/SDK configuration** – controls how telemetry is collected and batched (sampling, batch size, resource, etc.).
//!    Built via [`opentelemetry_sdk::trace::SdkTracerProvider::builder()`],
//!    [`opentelemetry_sdk::metrics::SdkMeterProvider::builder()`], or
//!    [`opentelemetry_sdk::logs::SdkLoggerProvider::builder()`].
//!
//! **Configuration precedence** (highest wins):
//! 1. Programmatic configuration via builder methods — always wins when set
//! 2. Signal-specific environment variables (e.g. `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`) — used when no programmatic value is set
//! 3. Generic environment variables (e.g. `OTEL_EXPORTER_OTLP_ENDPOINT`) — used when neither programmatic nor signal-specific env var is set
//! 4. Built-in defaults — used when nothing else is configured
//!
//! ## gRPC (tonic) — all configuration options
//!
//! Requires the `grpc-tonic` feature. The methods below come from two traits:
//! - [`WithExportConfig`]: `with_endpoint`, `with_timeout` (shared with HTTP)
//! - [`WithTonicConfig`]: `with_metadata`, `with_compression`, `with_tls_config`, `with_channel`, `with_interceptor`
//!
//! The examples here use [`SpanExporter`], but the same builder methods are
//! available on [`MetricExporter`] and [`LogExporter`].
//!
//! ```no_run
//! # #[cfg(all(feature = "trace", feature = "grpc-tonic"))]
//! # {
//! use opentelemetry_otlp::{WithExportConfig, WithTonicConfig, Compression};
//! use std::time::Duration;
//! use tonic::metadata::MetadataMap;
//!
//! // ── gRPC metadata (custom request headers) ───────────────────────────────
//! // MetadataMap carries per-call key/value pairs sent as HTTP/2 headers.
//! // with_metadata() is additive: calling it multiple times merges entries.
//! let mut metadata = MetadataMap::with_capacity(3);
//! metadata.insert("x-host", "example.com".parse().unwrap());
//! metadata.insert("x-api-key", "secret".parse().unwrap());
//! metadata.insert_bin("trace-proto-bin", tonic::metadata::MetadataValue::from_bytes(b"[bin]"));
//!
//! let exporter = opentelemetry_otlp::SpanExporter::builder()
//!     .with_tonic()
//!     // Target gRPC endpoint. Defaults to http://localhost:4317.
//!     // Env var: OTEL_EXPORTER_OTLP_TRACES_ENDPOINT (or OTEL_EXPORTER_OTLP_ENDPOINT).
//!     .with_endpoint("http://my-collector:4317")
//!     // Per-export timeout. Defaults to 10 s.
//!     // Env var: OTEL_EXPORTER_OTLP_TRACES_TIMEOUT (or OTEL_EXPORTER_OTLP_TIMEOUT).
//!     .with_timeout(Duration::from_secs(5))
//!     // Custom gRPC metadata (auth tokens, routing headers, …).
//!     // Env var: OTEL_EXPORTER_OTLP_TRACES_HEADERS (or OTEL_EXPORTER_OTLP_HEADERS).
//!     .with_metadata(metadata)
//!     // Compression. Requires the `gzip-tonic` or `zstd-tonic` feature.
//!     // Env var: OTEL_EXPORTER_OTLP_TRACES_COMPRESSION (or OTEL_EXPORTER_OTLP_COMPRESSION).
//!     .with_compression(Compression::Gzip)
//!     .build()
//!     .expect("Failed to build SpanExporter");
//! # }
//! ```
//!
//! ### TLS (grpc-tonic)
//!
//! Requires the `tls-ring` or `tls-aws-lc` feature (plus optionally `tls-roots` or `tls-webpki-roots`
//! to load CA roots automatically).
//!
//! ```no_run
//! # #[cfg(all(feature = "trace", feature = "grpc-tonic", any(feature = "tls-ring", feature = "tls-aws-lc")))]
//! # {
//! use opentelemetry_otlp::{WithExportConfig, WithTonicConfig};
//! use opentelemetry_otlp::tonic_types::transport::ClientTlsConfig;
//!
//! let tls = ClientTlsConfig::new()
//!     .domain_name("my-collector.example.com")
//!     // Optionally verify the server with a CA certificate:
//!     // .ca_certificate(tonic::transport::Certificate::from_pem(CA_PEM))
//!     // Or present a client identity for mutual TLS (mTLS):
//!     // .identity(tonic::transport::Identity::from_pem(CERT_PEM, KEY_PEM))
//!     ;
//!
//! let exporter = opentelemetry_otlp::SpanExporter::builder()
//!     .with_tonic()
//!     .with_endpoint("https://my-collector.example.com:4317")
//!     .with_tls_config(tls)
//!     .build()
//!     .expect("Failed to build SpanExporter");
//! # }
//! ```
//!
//! ### Pre-built tonic channel
//!
//! Use `with_channel` when you need full control over the transport (e.g. Unix sockets,
//! custom load-balancing). **Note:** `with_channel` overrides any TLS config set via
//! `with_tls_config`, and you are responsible for matching the channel timeout to
//! the exporter timeout.
//!
//! ```no_run
//! # #[cfg(all(feature = "trace", feature = "grpc-tonic"))]
//! # {
//! use opentelemetry_otlp::{WithExportConfig, WithTonicConfig};
//! use std::time::Duration;
//!
//! let channel = tonic::transport::Channel::from_static("http://localhost:4317")
//!     .timeout(Duration::from_secs(5))
//!     .connect_lazy();
//!
//! let exporter = opentelemetry_otlp::SpanExporter::builder()
//!     .with_tonic()
//!     .with_channel(channel)
//!     .with_timeout(Duration::from_secs(5)) // keep in sync with channel timeout above
//!     .build()
//!     .expect("Failed to build SpanExporter");
//! # }
//! ```
//!
//! ### gRPC interceptors
//!
//! Use `with_interceptor` to modify every outbound gRPC request — useful for injecting
//! auth tokens or dynamic metadata. Only one interceptor can be set; chain multiple together
//! before passing them in.
//!
//! ```no_run
//! # #[cfg(all(feature = "trace", feature = "grpc-tonic"))]
//! # {
//! use opentelemetry_otlp::WithTonicConfig;
//! use tonic::{Request, Status};
//!
//! fn auth_interceptor(mut req: Request<()>) -> Result<Request<()>, Status> {
//!     req.metadata_mut().insert("authorization", "Bearer my-token".parse().unwrap());
//!     Ok(req)
//! }
//!
//! let exporter = opentelemetry_otlp::SpanExporter::builder()
//!     .with_tonic()
//!     .with_interceptor(auth_interceptor)
//!     .build()
//!     .expect("Failed to build SpanExporter");
//! # }
//! ```
//!
//! ### gRPC retry policy
//!
//! Requires the `experimental-grpc-retry` feature. When enabled, failed exports are retried
//! with exponential backoff and jitter. Without this feature, failed exports are not retried.
//!
//! ```no_run
//! # #[cfg(all(feature = "trace", feature = "experimental-grpc-retry"))]
//! # {
//! use opentelemetry_otlp::{WithTonicConfig, RetryPolicy};
//!
//! let exporter = opentelemetry_otlp::SpanExporter::builder()
//!     .with_tonic()
//!     .with_retry_policy(RetryPolicy {
//!         max_retries: 5,        // number of attempts after the first failure
//!         initial_delay_ms: 500, // delay before the first retry
//!         max_delay_ms: 30_000,  // cap on the delay between retries
//!         jitter_ms: 100,        // upper bound for random jitter added by the exporter
//!     })
//!     .build()
//!     .expect("Failed to build SpanExporter");
//! # }
//! ```
//!
//! ## HTTP — all configuration options
//!
//! Requires the `http-proto` (default) or `http-json` feature. The methods below come from:
//! - [`WithExportConfig`]: `with_endpoint`, `with_timeout`, `with_protocol`
//! - [`WithHttpConfig`]: `with_headers`, `with_compression`, `with_http_client`
//!
//! The examples here use [`SpanExporter`], but the same builder methods are
//! available on [`MetricExporter`] and [`LogExporter`].
//!
//! ```no_run
//! # #[cfg(all(feature = "trace", feature = "http-proto"))]
//! # {
//! use opentelemetry_otlp::{WithExportConfig, WithHttpConfig, Protocol, Compression};
//! use std::time::Duration;
//! use std::collections::HashMap;
//!
//! let exporter = opentelemetry_otlp::SpanExporter::builder()
//!     .with_http()
//!     // Target base URL. Defaults to http://localhost:4318.
//!     // The path /v1/traces (or /v1/metrics, /v1/logs) is appended automatically.
//!     // Env var: OTEL_EXPORTER_OTLP_TRACES_ENDPOINT (or OTEL_EXPORTER_OTLP_ENDPOINT).
//!     .with_endpoint("http://my-collector:4318")
//!     // Per-export timeout. Defaults to 10 s.
//!     // Env var: OTEL_EXPORTER_OTLP_TRACES_TIMEOUT (or OTEL_EXPORTER_OTLP_TIMEOUT).
//!     .with_timeout(Duration::from_secs(5))
//!     // Transport encoding. HttpBinary (protobuf) is the default.
//!     // HttpJson requires the `http-json` feature.
//!     // Env var: OTEL_EXPORTER_OTLP_PROTOCOL.
//!     .with_protocol(Protocol::HttpBinary)
//!     // Custom HTTP headers (auth tokens, routing headers, …).
//!     // Values are URL-decoded when read from environment variables.
//!     // Env var: OTEL_EXPORTER_OTLP_TRACES_HEADERS (or OTEL_EXPORTER_OTLP_HEADERS).
//!     .with_headers(HashMap::from([
//!         ("x-api-key".to_string(), "secret".to_string()),
//!     ]))
//!     // Compression. Requires the `gzip-http` or `zstd-http` feature.
//!     // Env var: OTEL_EXPORTER_OTLP_TRACES_COMPRESSION (or OTEL_EXPORTER_OTLP_COMPRESSION).
//!     .with_compression(Compression::Gzip)
//!     .build()
//!     .expect("Failed to build SpanExporter");
//! # }
//! ```
//!
//! ### Custom HTTP client
//!
//! By default the exporter uses a `reqwest` blocking client (`reqwest-blocking-client` feature,
//! enabled by default). Supply your own client to control TLS, proxies, connection pooling, etc.
//! The client must implement the [`opentelemetry_http::HttpClient`] trait.
//!
//! ```no_run
//! # #[cfg(all(feature = "trace", feature = "http-proto", feature = "reqwest-client"))]
//! # {
//! use opentelemetry_otlp::WithHttpConfig;
//!
//! // reqwest async client (requires the `reqwest-client` feature)
//! let http_client = reqwest::Client::builder()
//!     .timeout(std::time::Duration::from_secs(5))
//!     // .danger_accept_invalid_certs(true) // for testing only
//!     .build()
//!     .expect("Failed to build reqwest client");
//!
//! let exporter = opentelemetry_otlp::SpanExporter::builder()
//!     .with_http()
//!     .with_http_client(http_client)
//!     .build()
//!     .expect("Failed to build SpanExporter");
//! # }
//! ```
//!
//! ### HTTP retry policy
//!
//! Requires the `experimental-http-retry` feature. When enabled, failed exports are retried
//! with exponential backoff and jitter. Without this feature, failed exports are not retried.
//!
//! ```no_run
//! # #[cfg(all(feature = "trace", feature = "experimental-http-retry"))]
//! # {
//! use opentelemetry_otlp::{WithHttpConfig, RetryPolicy};
//!
//! let exporter = opentelemetry_otlp::SpanExporter::builder()
//!     .with_http()
//!     .with_retry_policy(RetryPolicy {
//!         max_retries: 5,        // number of attempts after the first failure
//!         initial_delay_ms: 500, // delay before the first retry
//!         max_delay_ms: 30_000,  // cap on the delay between retries
//!         jitter_ms: 100,        // upper bound for random jitter added by the exporter
//!     })
//!     .build()
//!     .expect("Failed to build SpanExporter");
//! # }
//! ```
//!
//! ## All three signals (Traces, Metrics, Logs)
//!
//! The same exporter configuration options apply to all three signals. The only differences are:
//! - The builder entry point: [`SpanExporter::builder()`], [`MetricExporter::builder()`], [`LogExporter::builder()`]
//! - The signal-specific environment variables (e.g. `OTEL_EXPORTER_OTLP_TRACES_*` vs `OTEL_EXPORTER_OTLP_METRICS_*`)
//! - Metrics has an additional `OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE` variable
//!
//! ```no_run
//! # #[cfg(all(feature = "trace", feature = "metrics", feature = "logs", feature = "grpc-tonic"))]
//! # {
//! use opentelemetry_otlp::{WithExportConfig, WithTonicConfig};
//! use opentelemetry_sdk::{
//!     trace::SdkTracerProvider,
//!     metrics::SdkMeterProvider,
//!     logs::SdkLoggerProvider,
//!     Resource,
//! };
//! use std::time::Duration;
//!
//! let resource = Resource::builder()
//!     .with_service_name("my-service")
//!     .build();
//!
//! // Traces
//! let span_exporter = opentelemetry_otlp::SpanExporter::builder()
//!     .with_tonic()
//!     .with_endpoint("http://my-collector:4317")
//!     .with_timeout(Duration::from_secs(5))
//!     .build()
//!     .expect("Failed to build SpanExporter");
//! let tracer_provider = SdkTracerProvider::builder()
//!     .with_resource(resource.clone())
//!     .with_batch_exporter(span_exporter)
//!     .build();
//!
//! // Metrics
//! let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
//!     .with_tonic()
//!     .with_endpoint("http://my-collector:4317")
//!     .with_timeout(Duration::from_secs(5))
//!     .build()
//!     .expect("Failed to build MetricExporter");
//! let meter_provider = SdkMeterProvider::builder()
//!     .with_resource(resource.clone())
//!     .with_periodic_exporter(metric_exporter)
//!     .build();
//!
//! // Logs
//! let log_exporter = opentelemetry_otlp::LogExporter::builder()
//!     .with_tonic()
//!     .with_endpoint("http://my-collector:4317")
//!     .with_timeout(Duration::from_secs(5))
//!     .build()
//!     .expect("Failed to build LogExporter");
//! let logger_provider = SdkLoggerProvider::builder()
//!     .with_resource(resource)
//!     .with_batch_exporter(log_exporter)
//!     .build();
//! # }
//! ```
#![warn(
    future_incompatible,
    missing_debug_implementations,
    missing_docs,
    nonstandard_style,
    rust_2018_idioms,
    unreachable_pub,
    unused
)]
#![allow(elided_lifetimes_in_paths)]
#![cfg_attr(docsrs, feature(doc_cfg), deny(rustdoc::broken_intra_doc_links))]
#![cfg_attr(test, deny(warnings))]

mod exporter;
#[cfg(feature = "logs")]
#[cfg(any(feature = "http-proto", feature = "http-json", feature = "grpc-tonic"))]
mod logs;
#[cfg(feature = "metrics")]
#[cfg(any(feature = "http-proto", feature = "http-json", feature = "grpc-tonic"))]
mod metric;
#[cfg(feature = "trace")]
#[cfg(any(feature = "http-proto", feature = "http-json", feature = "grpc-tonic"))]
mod span;

#[cfg(any(feature = "grpc-tonic", feature = "experimental-http-retry"))]
pub mod retry_classification;

/// Retry logic for exporting telemetry data.
#[cfg(any(feature = "grpc-tonic", feature = "experimental-http-retry"))]
pub mod retry;

pub use crate::exporter::Compression;
pub use crate::exporter::ExporterBuildError;
#[cfg(feature = "trace")]
#[cfg(any(feature = "http-proto", feature = "http-json", feature = "grpc-tonic"))]
pub use crate::span::{
    SpanExporter, SpanExporterBuilder, OTEL_EXPORTER_OTLP_TRACES_COMPRESSION,
    OTEL_EXPORTER_OTLP_TRACES_ENDPOINT, OTEL_EXPORTER_OTLP_TRACES_HEADERS,
    OTEL_EXPORTER_OTLP_TRACES_PROTOCOL, OTEL_EXPORTER_OTLP_TRACES_TIMEOUT,
};

#[cfg(feature = "metrics")]
#[cfg(any(feature = "http-proto", feature = "http-json", feature = "grpc-tonic"))]
pub use crate::metric::{
    MetricExporter, MetricExporterBuilder, OTEL_EXPORTER_OTLP_METRICS_COMPRESSION,
    OTEL_EXPORTER_OTLP_METRICS_ENDPOINT, OTEL_EXPORTER_OTLP_METRICS_HEADERS,
    OTEL_EXPORTER_OTLP_METRICS_PROTOCOL, OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE,
    OTEL_EXPORTER_OTLP_METRICS_TIMEOUT,
};

#[cfg(feature = "logs")]
#[cfg(any(feature = "http-proto", feature = "http-json", feature = "grpc-tonic"))]
pub use crate::logs::{
    LogExporter, LogExporterBuilder, OTEL_EXPORTER_OTLP_LOGS_COMPRESSION,
    OTEL_EXPORTER_OTLP_LOGS_ENDPOINT, OTEL_EXPORTER_OTLP_LOGS_HEADERS,
    OTEL_EXPORTER_OTLP_LOGS_PROTOCOL, OTEL_EXPORTER_OTLP_LOGS_TIMEOUT,
};

#[cfg(any(feature = "http-proto", feature = "http-json"))]
pub use crate::exporter::http::WithHttpConfig;

#[cfg(feature = "grpc-tonic")]
pub use crate::exporter::tonic::WithTonicConfig;

pub use crate::exporter::{
    WithExportConfig, OTEL_EXPORTER_OTLP_COMPRESSION, OTEL_EXPORTER_OTLP_ENDPOINT,
    OTEL_EXPORTER_OTLP_ENDPOINT_DEFAULT, OTEL_EXPORTER_OTLP_HEADERS, OTEL_EXPORTER_OTLP_PROTOCOL,
    OTEL_EXPORTER_OTLP_PROTOCOL_GRPC, OTEL_EXPORTER_OTLP_PROTOCOL_HTTP_JSON,
    OTEL_EXPORTER_OTLP_PROTOCOL_HTTP_PROTOBUF, OTEL_EXPORTER_OTLP_TIMEOUT,
    OTEL_EXPORTER_OTLP_TIMEOUT_DEFAULT,
};

#[cfg(any(
    feature = "experimental-http-retry",
    feature = "experimental-grpc-retry"
))]
pub use retry::RetryPolicy;

/// Type to indicate the builder does not have a client set.
#[derive(Debug, Default, Clone)]
pub struct NoExporterBuilderSet;

/// Type to hold the [TonicExporterBuilder] and indicate it has been set.
///
/// Allowing access to [TonicExporterBuilder] specific configuration methods.
#[cfg(feature = "grpc-tonic")]
// This is for clippy to work with only the grpc-tonic feature enabled
#[allow(unused)]
#[derive(Debug, Default)]
pub struct TonicExporterBuilderSet(TonicExporterBuilder);

/// Type to hold the [HttpExporterBuilder] and indicate it has been set.
///
/// Allowing access to [HttpExporterBuilder] specific configuration methods.
#[cfg(any(feature = "http-proto", feature = "http-json"))]
#[derive(Debug, Default)]
pub struct HttpExporterBuilderSet(HttpExporterBuilder);

#[cfg(any(feature = "http-proto", feature = "http-json"))]
pub use crate::exporter::http::HttpExporterBuilder;

#[cfg(feature = "grpc-tonic")]
pub use crate::exporter::tonic::TonicExporterBuilder;

#[cfg(feature = "serialize")]
use serde::{Deserialize, Serialize};

/// The communication protocol to use when exporting data.
#[cfg_attr(feature = "serialize", derive(Deserialize, Serialize))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Protocol {
    /// GRPC protocol
    #[cfg(feature = "grpc-tonic")]
    Grpc,
    /// HTTP protocol with binary protobuf
    #[cfg(feature = "http-proto")]
    HttpBinary,
    /// HTTP protocol with JSON payload
    #[cfg(feature = "http-json")]
    HttpJson,
}

#[cfg(any(feature = "grpc-tonic", feature = "http-proto", feature = "http-json"))]
impl Protocol {
    /// Attempts to parse a protocol from the `OTEL_EXPORTER_OTLP_PROTOCOL` environment variable.
    ///
    /// Returns `None` if:
    /// - The environment variable is not set
    /// - The value doesn't match a known protocol
    /// - The specified protocol's feature is not enabled
    pub fn from_env() -> Option<Self> {
        Self::parse_from_env_var(OTEL_EXPORTER_OTLP_PROTOCOL)
    }

    /// Attempts to parse a protocol from the given environment variable.
    ///
    /// Returns `None` if:
    /// - The environment variable is not set
    /// - The value doesn't match a known protocol
    /// - The specified protocol's feature is not enabled
    pub(crate) fn parse_from_env_var(env_var: &str) -> Option<Self> {
        use crate::exporter::{
            OTEL_EXPORTER_OTLP_PROTOCOL_GRPC, OTEL_EXPORTER_OTLP_PROTOCOL_HTTP_JSON,
            OTEL_EXPORTER_OTLP_PROTOCOL_HTTP_PROTOBUF,
        };

        let protocol = std::env::var(env_var).ok()?;

        match protocol.as_str() {
            OTEL_EXPORTER_OTLP_PROTOCOL_GRPC => {
                #[cfg(feature = "grpc-tonic")]
                {
                    Some(Protocol::Grpc)
                }
                #[cfg(not(feature = "grpc-tonic"))]
                {
                    opentelemetry::otel_warn!(
                        name: "Protocol.InvalidFeatureCombination",
                        message = format!("Protocol '{}' requested but 'grpc-tonic' feature is not enabled", OTEL_EXPORTER_OTLP_PROTOCOL_GRPC)
                    );
                    None
                }
            }
            OTEL_EXPORTER_OTLP_PROTOCOL_HTTP_PROTOBUF => {
                #[cfg(feature = "http-proto")]
                {
                    Some(Protocol::HttpBinary)
                }
                #[cfg(not(feature = "http-proto"))]
                {
                    opentelemetry::otel_warn!(
                        name: "Protocol.InvalidFeatureCombination",
                        message = format!("Protocol '{}' requested but 'http-proto' feature is not enabled", OTEL_EXPORTER_OTLP_PROTOCOL_HTTP_PROTOBUF)
                    );
                    None
                }
            }
            OTEL_EXPORTER_OTLP_PROTOCOL_HTTP_JSON => {
                #[cfg(feature = "http-json")]
                {
                    Some(Protocol::HttpJson)
                }
                #[cfg(not(feature = "http-json"))]
                {
                    opentelemetry::otel_warn!(
                        name: "Protocol.InvalidFeatureCombination",
                        message = format!("Protocol '{}' requested but 'http-json' feature is not enabled", OTEL_EXPORTER_OTLP_PROTOCOL_HTTP_JSON)
                    );
                    None
                }
            }
            _ => None,
        }
    }

    /// Returns the default protocol based on enabled features, without
    /// consulting environment variables.
    ///
    /// Priority order (first available wins):
    /// 1. http-json (if enabled)
    /// 2. http-proto (if enabled)
    /// 3. grpc-tonic (if enabled)
    pub(crate) fn feature_default() -> Self {
        #[cfg(feature = "http-json")]
        return Protocol::HttpJson;

        #[cfg(all(feature = "http-proto", not(feature = "http-json")))]
        return Protocol::HttpBinary;

        #[cfg(all(
            feature = "grpc-tonic",
            not(any(feature = "http-proto", feature = "http-json"))
        ))]
        return Protocol::Grpc;
    }
}

#[derive(Debug, Default)]
#[doc(hidden)]
/// Placeholder type when no exporter pipeline has been configured in telemetry pipeline.
pub struct NoExporterConfig(());

/// Re-exported types from the `tonic` crate.
#[cfg(feature = "grpc-tonic")]
pub mod tonic_types {
    /// Re-exported types from `tonic::metadata`.
    pub mod metadata {
        #[doc(no_inline)]
        pub use tonic::metadata::MetadataMap;
    }

    /// Re-exported types from `tonic::transport`.
    #[cfg(any(
        feature = "tls",
        feature = "tls-ring",
        feature = "tls-aws-lc",
        feature = "tls-provider-agnostic"
    ))]
    pub mod transport {
        #[doc(no_inline)]
        pub use tonic::transport::{Certificate, ClientTlsConfig, Identity};
    }
}
