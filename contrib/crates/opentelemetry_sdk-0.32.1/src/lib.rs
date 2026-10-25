//! Implements the [`SDK`] component of [OpenTelemetry].
//!
//! *[Supported Rust Versions](#supported-rust-versions)*
//!
//! [`SDK`]: https://opentelemetry.io/docs/specs/otel/overview/#sdk
//! [OpenTelemetry]: https://opentelemetry.io/docs/what-is-opentelemetry/
//! [msrv]: #supported-rust-versions
//!
//! # Getting Started
//!
//! ```no_run
//! # #[cfg(feature = "trace")]
//! # {
//! use opentelemetry::{global, trace::{Tracer, TracerProvider}};
//! use opentelemetry_sdk::trace::SdkTracerProvider;
//!
//! fn main() {
//!     // Choose an exporter like `opentelemetry_stdout::SpanExporter`
//!     # fn example<T: opentelemetry_sdk::trace::SpanExporter + 'static>(new_exporter: impl Fn() -> T) {
//!     let exporter = new_exporter();
//!
//!     // Create a new trace pipeline that prints to stdout
//!     let provider = SdkTracerProvider::builder()
//!         .with_simple_exporter(exporter)
//!         .build();
//!     let tracer = provider.tracer("readme_example");
//!
//!     tracer.in_span("doing_work", |cx| {
//!         // Traced app logic here...
//!     });
//!
//!     // Shutdown trace pipeline
//!     provider.shutdown().expect("TracerProvider should shutdown successfully")
//!     # }
//! }
//! # }
//! ```
//!
//! See the [examples] directory for different integration patterns.
//!
//! See the API [`trace`] module docs for more information on creating and managing
//! spans.
//!
//! [examples]: https://github.com/open-telemetry/opentelemetry-rust/tree/main/examples
//! [`trace`]: https://docs.rs/opentelemetry/latest/opentelemetry/trace/index.html
//!
//! # Metrics
//!
//! ### Creating instruments and recording measurements
//!
//! ```
//! # #[cfg(feature = "metrics")]
//! # {
//! use opentelemetry::{global, KeyValue};
//!
//! // get a meter from a provider
//! let meter = global::meter("my_service");
//!
//! // create an instrument
//! let counter = meter.u64_counter("my_counter").build();
//!
//! // record a measurement
//! counter.add(1, &[KeyValue::new("http.client_ip", "83.164.160.102")]);
//! # }
//! ```
//!
//! See the [examples] directory for different integration patterns.
//!
//! See the API [`metrics`] module docs for more information on creating and
//! managing instruments.
//!
//! [examples]: https://github.com/open-telemetry/opentelemetry-rust/tree/main/examples
//! [`metrics`]: https://docs.rs/opentelemetry/latest/opentelemetry/metrics/index.html
//!
//! ## Environment Variables
//!
//! The SDK respects the following environment variables, as defined by the
//! [OpenTelemetry specification]. Programmatic configuration via builder methods
//! takes precedence over environment variables.
//!
//! [OpenTelemetry specification]: https://opentelemetry.io/docs/specs/otel/configuration/sdk-environment-variables/
//!
//! ### General / Resource
//!
//! | Variable | Description | Default |
//! |---|---|---|
//! | `OTEL_SERVICE_NAME` | Sets the value of the `service.name` resource attribute. Takes priority over `service.name` in `OTEL_RESOURCE_ATTRIBUTES`. | `unknown_service:<process_name>` |
//! | `OTEL_RESOURCE_ATTRIBUTES` | Key-value pairs to be used as resource attributes. Format: `key1=value1,key2=value2`. | (none) |
//!
//! ### Trace: Sampler
//!
//! | Variable | Description | Default |
//! |---|---|---|
//! | `OTEL_TRACES_SAMPLER` | Sampler to use. Valid values: `always_on`, `always_off`, `traceidratio`, `parentbased_always_on`, `parentbased_always_off`, `parentbased_traceidratio`. | `parentbased_always_on` |
//! | `OTEL_TRACES_SAMPLER_ARG` | Argument for the sampler. Used when `OTEL_TRACES_SAMPLER` is `traceidratio` or `parentbased_traceidratio`. Must be a float between 0.0 and 1.0. | `1.0` |
//!
//! ### Trace: Span Limits
//!
//! | Variable | Description | Default |
//! |---|---|---|
//! | `OTEL_SPAN_ATTRIBUTE_COUNT_LIMIT` | Maximum number of attributes allowed on a span. | `128` |
//! | `OTEL_SPAN_EVENT_COUNT_LIMIT` | Maximum number of events allowed on a span. | `128` |
//! | `OTEL_SPAN_LINK_COUNT_LIMIT` | Maximum number of links allowed on a span. | `128` |
//!
//! ### Trace: Batch Span Processor (BSP)
//!
//! | Variable | Description | Default |
//! |---|---|---|
//! | `OTEL_BSP_SCHEDULE_DELAY` | Delay interval (in milliseconds) between two consecutive exports. | `5000` |
//! | `OTEL_BSP_MAX_QUEUE_SIZE` | Maximum queue size. | `2048` |
//! | `OTEL_BSP_MAX_EXPORT_BATCH_SIZE` | Maximum batch size. Must be less than or equal to `OTEL_BSP_MAX_QUEUE_SIZE`. | `512` |
//! | `OTEL_BSP_MAX_CONCURRENT_EXPORTS` | Maximum number of concurrent exports. Honored by `span_processor_with_async_runtime::BatchSpanProcessor`; thread-based `BatchSpanProcessor` exports serially. For concurrent exports, enable `experimental_trace_batch_span_processor_with_async_runtime` and use the async-runtime processor. | `1` |
//!
//! ### Logs: Batch Log Record Processor (BLRP)
//!
//! | Variable | Description | Default |
//! |---|---|---|
//! | `OTEL_BLRP_SCHEDULE_DELAY` | Delay interval (in milliseconds) between two consecutive exports. | `1000` |
//! | `OTEL_BLRP_MAX_QUEUE_SIZE` | Maximum queue size. | `2048` |
//! | `OTEL_BLRP_MAX_EXPORT_BATCH_SIZE` | Maximum batch size. Must be less than or equal to `OTEL_BLRP_MAX_QUEUE_SIZE`. | `512` |
//!
//! ### Metrics: Periodic Metric Reader
//!
//! | Variable | Description | Default |
//! |---|---|---|
//! | `OTEL_METRIC_EXPORT_INTERVAL` | Interval (in milliseconds) between metrics exports. | `60000` |
//!
//! ## Crate Feature Flags
//!
//! The following feature flags can used to control the telemetry signals to use:
//!
//! * `trace`: Includes the trace SDK (enabled by default).
//! * `metrics`: Includes the metrics SDK (enabled by default).
//! * `logs`: Includes the logs SDK (enabled by default).
//!
//! For `trace` the following feature flags are available:
//!
//! * `jaeger_remote_sampler`: Enables the [Jaeger remote sampler](https://www.jaegertracing.io/docs/1.53/sampling/).
//!
//!
//! Support for recording and exporting telemetry asynchronously and perform
//! metrics aggregation can be added via the following flags:
//!
//! * `experimental_async_runtime`: Enables the experimental `Runtime` trait and related functionality.
//! * `rt-tokio`: Spawn telemetry tasks using [tokio]'s multi-thread runtime.
//! * `rt-tokio-current-thread`: Spawn telemetry tasks on a separate runtime so that the main runtime won't be blocked.
//!
//! [tokio]: https://crates.io/crates/tokio
#![warn(
    future_incompatible,
    missing_debug_implementations,
    missing_docs,
    nonstandard_style,
    rust_2018_idioms,
    unreachable_pub,
    unused
)]
#![allow(clippy::needless_doctest_main)]
#![cfg_attr(docsrs, feature(doc_cfg), deny(rustdoc::broken_intra_doc_links))]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/open-telemetry/opentelemetry-rust/main/assets/logo.svg"
)]
#![cfg_attr(test, deny(warnings))]

pub(crate) mod growable_array;

#[cfg(feature = "logs")]
#[cfg_attr(docsrs, doc(cfg(feature = "logs")))]
pub mod logs;
#[cfg(feature = "metrics")]
#[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
pub mod metrics;
#[cfg(feature = "trace")]
#[cfg_attr(docsrs, doc(cfg(feature = "trace")))]
pub mod propagation;
pub mod resource;
#[cfg(feature = "experimental_async_runtime")]
pub mod runtime;
#[cfg(any(feature = "testing", test))]
#[cfg_attr(docsrs, doc(cfg(any(feature = "testing", test))))]
pub mod testing;

#[cfg(feature = "trace")]
#[cfg_attr(docsrs, doc(cfg(feature = "trace")))]
pub mod trace;

#[doc(hidden)]
pub mod util;

#[doc(inline)]
pub use resource::Resource;

pub mod error;
pub use error::ExportError;
