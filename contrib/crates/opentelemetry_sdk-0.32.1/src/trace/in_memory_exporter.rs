use crate::error::{OTelSdkError, OTelSdkResult};
use crate::resource::Resource;
use crate::trace::{SpanData, SpanExporter};
use std::sync::{atomic::AtomicBool, Arc, Mutex};
use std::time::Duration;

/// An in-memory span exporter that stores span data in memory.
///
/// This exporter is useful for testing and debugging purposes. It stores
/// span data in a `Vec<SpanData>`. Spans can be retrieved
/// using the `get_finished_spans` method.
/// # Example
/// ```
///# use opentelemetry::trace::{SpanKind, TraceContextExt};
///# use opentelemetry::{global, trace::Tracer, Context};
///# use opentelemetry_sdk::propagation::TraceContextPropagator;
///# use opentelemetry_sdk::runtime;
///# use opentelemetry_sdk::trace::InMemorySpanExporterBuilder;
///# use opentelemetry_sdk::trace::{BatchSpanProcessor, SdkTracerProvider};
///
///# #[tokio::main]
///# async fn main() {
///     let exporter = InMemorySpanExporterBuilder::new().build();
///     let provider = SdkTracerProvider::builder()
///         .with_span_processor(BatchSpanProcessor::builder(exporter.clone()).build())
///         .build();
///
///     global::set_tracer_provider(provider.clone());
///
///     let tracer = global::tracer("example/in_memory_exporter");
///     let span = tracer
///         .span_builder("say hello")
///         .with_kind(SpanKind::Server)
///         .start(&tracer);
///
///     let cx = Context::current_with_span(span);
///     cx.span().add_event("handling this...", Vec::new());
///     cx.span().end();
///
///     if let Err(e) = provider.force_flush() {
///         println!("{:?}", e)
///     }
///     let spans = exporter.get_finished_spans().unwrap();
///     for span in spans {
///         println!("{:?}", span)
///     }
///# }
/// ```
#[derive(Clone, Debug)]
pub struct InMemorySpanExporter {
    spans: Arc<Mutex<Vec<SpanData>>>,
    resource: Arc<Mutex<Resource>>,
    should_reset_on_shutdown: bool,
    shutdown_called: Arc<AtomicBool>,
}

impl Default for InMemorySpanExporter {
    fn default() -> Self {
        InMemorySpanExporterBuilder::new().build()
    }
}

/// Builder for [`InMemorySpanExporter`].
/// # Example
/// ```
///# use opentelemetry_sdk::trace::InMemorySpanExporterBuilder;
///
/// let exporter = InMemorySpanExporterBuilder::new().build();
/// ```
#[derive(Clone, Debug)]
pub struct InMemorySpanExporterBuilder {
    reset_on_shutdown: bool,
}

impl Default for InMemorySpanExporterBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemorySpanExporterBuilder {
    /// Creates a new instance of the `InMemorySpanExporterBuilder`.
    pub fn new() -> Self {
        Self {
            reset_on_shutdown: true,
        }
    }

    /// Creates a new instance of the `InMemorySpanExporter`.
    pub fn build(&self) -> InMemorySpanExporter {
        InMemorySpanExporter {
            spans: Arc::new(Mutex::new(Vec::new())),
            resource: Arc::new(Mutex::new(Resource::builder().build())),
            should_reset_on_shutdown: self.reset_on_shutdown,
            shutdown_called: Arc::new(AtomicBool::new(false)),
        }
    }

    /// If set, the records will not be [`InMemorySpanExporter::reset`] on shutdown.
    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn keep_records_on_shutdown(self) -> Self {
        Self {
            reset_on_shutdown: false,
        }
    }
}

impl InMemorySpanExporter {
    /// Returns true if shutdown was called.
    pub fn is_shutdown_called(&self) -> bool {
        self.shutdown_called
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Returns the finished span as a vector of `SpanData`.
    ///
    /// # Errors
    ///
    /// Returns an `OTelSdkError`.
    ///
    /// # Example
    ///
    /// ```
    /// # use opentelemetry_sdk::trace::InMemorySpanExporter;
    ///
    /// let exporter = InMemorySpanExporter::default();
    /// let finished_spans = exporter.get_finished_spans().unwrap();
    /// ```
    pub fn get_finished_spans(&self) -> Result<Vec<SpanData>, OTelSdkError> {
        let spans = self
            .spans
            .lock()
            .map(|spans_guard| spans_guard.iter().cloned().collect())
            .map_err(OTelSdkError::from)?;
        Ok(spans)
    }

    /// Clears the internal storage of finished spans.
    ///
    /// # Example
    ///
    /// ```
    /// # use opentelemetry_sdk::trace::InMemorySpanExporter;
    ///
    /// let exporter = InMemorySpanExporter::default();
    /// exporter.reset();
    /// ```
    pub fn reset(&self) {
        let _ = self.spans.lock().map(|mut spans_guard| spans_guard.clear());
    }
}

impl SpanExporter for InMemorySpanExporter {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        let result = self
            .spans
            .lock()
            .map(|mut spans_guard| spans_guard.append(&mut batch.clone()))
            .map_err(|err| OTelSdkError::InternalFailure(format!("Failed to lock spans: {err:?}")));
        result
    }

    fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
        self.shutdown_called
            .store(true, std::sync::atomic::Ordering::Relaxed);
        if self.should_reset_on_shutdown {
            self.reset();
        }
        Ok(())
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.resource
            .lock()
            .map(|mut res_guard| *res_guard = resource.clone())
            .expect("Resource lock poisoned");
    }
}
