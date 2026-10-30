//! # OpenTelemetry Span Processor Interface
//!
//! Span processor is an interface which allows hooks for span start and end method
//! invocations. The span processors are invoked only when
//! [`is_recording`] is true.
//!
//! Built-in span processors are responsible for batching and conversion of spans to
//! exportable representation and passing batches to exporters.
//!
//! Span processors can be registered directly on SDK [`TracerProvider`] and they are
//! invoked in the same order as they were registered.
//!
//! All `Tracer` instances created by a `TracerProvider` share the same span processors.
//! Changes to this collection reflect in all `Tracer` instances.
//!
//! The following diagram shows `SpanProcessor`'s relationship to other components
//! in the SDK:
//!
//! ```ascii
//!   +-----+--------------+   +-----------------------+   +-------------------+
//!   |     |              |   |                       |   |                   |
//!   |     |              |   | (Batch)SpanProcessor  |   |    SpanExporter   |
//!   |     |              +---> (Simple)SpanProcessor +--->  (OTLPExporter)   |
//!   |     |              |   |                       |   |                   |
//!   | SDK | Tracer.span()|   +-----------------------+   +-------------------+
//!   |     | Span.end()   |
//!   |     |              |
//!   |     |              |
//!   |     |              |
//!   |     |              |
//!   +-----+--------------+
//! ```
//!
//! [`is_recording`]: opentelemetry::trace::Span::is_recording()
//! [`TracerProvider`]: opentelemetry::trace::TracerProvider

use crate::error::{OTelSdkError, OTelSdkResult};
use crate::resource::Resource;
use crate::trace::Span;
use crate::trace::{SpanData, SpanExporter};
use opentelemetry::Context;
use opentelemetry::{otel_debug, otel_error, otel_warn};
use std::cmp::min;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::{env, str::FromStr, time::Duration};

use std::sync::atomic::AtomicBool;
use std::thread;
use std::time::Instant;

/// Delay interval between two consecutive exports.
pub(crate) const OTEL_BSP_SCHEDULE_DELAY: &str = "OTEL_BSP_SCHEDULE_DELAY";
/// Default delay interval between two consecutive exports.
pub(crate) const OTEL_BSP_SCHEDULE_DELAY_DEFAULT: Duration = Duration::from_millis(5_000);
/// Maximum queue size
pub(crate) const OTEL_BSP_MAX_QUEUE_SIZE: &str = "OTEL_BSP_MAX_QUEUE_SIZE";
/// Default maximum queue size
pub(crate) const OTEL_BSP_MAX_QUEUE_SIZE_DEFAULT: usize = 2_048;
/// Maximum batch size, must be less than or equal to OTEL_BSP_MAX_QUEUE_SIZE
pub(crate) const OTEL_BSP_MAX_EXPORT_BATCH_SIZE: &str = "OTEL_BSP_MAX_EXPORT_BATCH_SIZE";
/// Default maximum batch size
pub(crate) const OTEL_BSP_MAX_EXPORT_BATCH_SIZE_DEFAULT: usize = 512;
/// Maximum allowed time to export data.
pub(crate) const OTEL_BSP_EXPORT_TIMEOUT: &str = "OTEL_BSP_EXPORT_TIMEOUT";
/// Default maximum allowed time to export data.
pub(crate) const OTEL_BSP_EXPORT_TIMEOUT_DEFAULT: Duration = Duration::from_millis(30_000);
pub(crate) const OTEL_BSP_MAX_CONCURRENT_EXPORTS: &str = "OTEL_BSP_MAX_CONCURRENT_EXPORTS";
/// Default max concurrent exports for BSP
pub(crate) const OTEL_BSP_MAX_CONCURRENT_EXPORTS_DEFAULT: usize = 1;

/// `SpanProcessor` is an interface which allows hooks for span start and end
/// method invocations. The span processors are invoked only when is_recording
/// is true.
pub trait SpanProcessor: Send + Sync + std::fmt::Debug {
    /// `on_start` is called when a `Span` is started.  This method is called
    /// synchronously on the thread that started the span, therefore it should
    /// not block or throw exceptions.
    fn on_start(&self, span: &mut Span, cx: &Context);

    /// `on_end` is called after a `Span` is ended (i.e., the end timestamp is
    /// already set). This method is called synchronously within the `Span::end`
    /// API, therefore it should not block or throw an exception.
    ///
    /// # Accessing Context
    ///
    /// **Important**: Do not rely on [`Context::current()`] in `on_end`. When `on_end`
    /// is called during span cleanup, `Context::current()` returns whatever context
    /// happens to be active at that moment, which is typically unrelated to the span
    /// being ended. Contexts can be activated in any order and are not necessarily
    /// hierarchical.
    ///
    /// **Best Practice**: Extract any needed context information in [`on_start`]
    /// and store it as span attributes. This ensures the information is available
    /// in the [`SpanData`] passed to `on_end`.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// impl SpanProcessor for MyProcessor {
    ///     fn on_start(&self, span: &mut Span, cx: &Context) {
    ///         // Extract baggage and store as span attribute
    ///         if let Some(value) = cx.baggage().get("my-key") {
    ///             span.set_attribute(KeyValue::new("my-key", value.to_string()));
    ///         }
    ///     }
    ///
    ///     fn on_end(&self, span: SpanData) {
    ///         // Access the attribute stored in on_start
    ///         let my_value = span.attributes.iter()
    ///             .find(|kv| kv.key.as_str() == "my-key");
    ///     }
    /// }
    /// ```
    ///
    /// [`on_start`]: SpanProcessor::on_start
    /// [`Context::current()`]: opentelemetry::Context::current
    ///
    /// TODO - This method should take reference to `SpanData`
    fn on_end(&self, span: SpanData);
    /// Force the spans lying in the cache to be exported.
    fn force_flush(&self) -> OTelSdkResult;
    /// Shuts down the processor. Called when SDK is shut down. This is an
    /// opportunity for processors to do any cleanup required.
    ///
    /// Implementation should make sure shutdown can be called multiple times.
    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult;
    /// shutdown the processor with a default timeout.
    fn shutdown(&self) -> OTelSdkResult {
        self.shutdown_with_timeout(Duration::from_secs(5))
    }
    /// Set the resource for the span processor.
    fn set_resource(&mut self, _resource: &Resource) {}
}

/// A [SpanProcessor] that passes finished spans to the configured
/// `SpanExporter`, as soon as they are finished, without any batching. This is
/// typically useful for debugging and testing. For scenarios requiring higher
/// performance/throughput, consider using [BatchSpanProcessor].
/// Spans are exported synchronously
/// in the same thread that emits the log record.
/// When using this processor with the OTLP Exporter, the following exporter
/// features are supported:
/// - `grpc-tonic`: This requires TracerProvider to be created within a tokio
///   runtime. Spans can be emitted from any thread, including tokio runtime
///   threads.
/// - `reqwest-blocking-client`: TracerProvider may be created anywhere, but
///   spans must be emitted from a non-tokio runtime thread.
/// - `reqwest-client`: TracerProvider may be created anywhere, but spans must be
///   emitted from a tokio runtime thread.
///
/// The OTLP HTTP exporter chooses its default HTTP client from enabled crate
/// features. That choice is not processor-aware. If you enable async HTTP
/// clients such as `reqwest-client` or `hyper-client`, ensure this processor is
/// only used from a thread where those clients can run.
#[derive(Debug)]
pub struct SimpleSpanProcessor<T: SpanExporter> {
    exporter: Mutex<T>,
}

impl<T: SpanExporter> SimpleSpanProcessor<T> {
    /// Create a new [SimpleSpanProcessor] using the provided exporter.
    pub fn new(exporter: T) -> Self {
        Self {
            exporter: Mutex::new(exporter),
        }
    }
}

impl<T: SpanExporter> SpanProcessor for SimpleSpanProcessor<T> {
    fn on_start(&self, _span: &mut Span, _cx: &Context) {
        // Ignored
    }

    fn on_end(&self, span: SpanData) {
        if !span.span_context.is_sampled() {
            return;
        }

        let result = self
            .exporter
            .lock()
            .map_err(|_| OTelSdkError::InternalFailure("SimpleSpanProcessor mutex poison".into()))
            .and_then(|exporter| futures_executor::block_on(exporter.export(vec![span])));

        if let Err(err) = result {
            // TODO: check error type, and log `error` only if the error is user-actionable, else log `debug`
            otel_debug!(
                name: "SimpleProcessor.OnEnd.Error",
                reason = format!("{:?}", err)
            );
        }
    }

    fn force_flush(&self) -> OTelSdkResult {
        // Nothing to flush for simple span processor.
        Ok(())
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        if let Ok(exporter) = self.exporter.lock() {
            exporter.shutdown_with_timeout(timeout)
        } else {
            Err(OTelSdkError::InternalFailure(
                "SimpleSpanProcessor mutex poison at shutdown".into(),
            ))
        }
    }

    fn set_resource(&mut self, resource: &Resource) {
        if let Ok(mut exporter) = self.exporter.lock() {
            exporter.set_resource(resource);
        }
    }
}

/// The `BatchSpanProcessor` collects finished spans in a buffer and exports them
/// in batches to the configured `SpanExporter`. This processor is ideal for
/// high-throughput environments, as it minimizes the overhead of exporting spans
/// individually. It uses a **dedicated background thread** to manage and export spans
/// asynchronously, ensuring that the application's main execution flow is not blocked.
///
/// When using this processor with the OTLP Exporter, the following exporter
/// features are supported:
/// - `grpc-tonic`: This requires `TracerProvider` to be created within a tokio
///   runtime.
/// - `reqwest-blocking-client`: Works with a regular `main` or `tokio::main`.
///
/// In other words, async HTTP clients like `reqwest-client` and `hyper-client`
/// are not supported by this default processor. The OTLP HTTP exporter chooses
/// its default HTTP client from enabled crate features and cannot tell which
/// processor will drive it. If your dependency graph enables async HTTP client
/// features, either pass an explicit blocking client for this processor or use
/// the experimental async-runtime batch span processor.
///
/// # Example
///
/// This example demonstrates how to configure and use the `BatchSpanProcessor`
/// with a custom configuration. Note that a dedicated thread is used internally
/// to manage the export process.
///
/// ```rust
/// # #[cfg(feature = "testing")]
/// # {
/// use opentelemetry::global;
/// use opentelemetry_sdk::trace::{
///     BatchSpanProcessor, BatchConfigBuilder, SdkTracerProvider, InMemorySpanExporter,
/// };
/// use opentelemetry::trace::Tracer as _;
/// use opentelemetry::trace::Span;
/// use std::time::Duration;
///
/// // Step 1: Create an exporter (e.g., an In-Memory Exporter for demonstration).
/// let exporter = InMemorySpanExporter::default();
///
/// // Step 2: Configure the BatchSpanProcessor.
/// let batch_processor = BatchSpanProcessor::builder(exporter)
///     .with_batch_config(
///         BatchConfigBuilder::default()
///             .with_max_queue_size(1024) // Buffer up to 1024 spans.
///             .with_max_export_batch_size(256) // Export in batches of up to 256 spans.
///             .with_scheduled_delay(Duration::from_secs(5)) // Export every 5 seconds.
///             .build(),
///     )
///     .build();
///
/// // Step 3: Set up a TracerProvider with the configured processor.
/// let provider = SdkTracerProvider::builder()
///     .with_span_processor(batch_processor)
///     .build();
/// global::set_tracer_provider(provider.clone());
///
/// // Step 4: Create spans and record operations.
/// let tracer = global::tracer("example-tracer");
/// let mut span = tracer.start("example-span");
/// span.end(); // Mark the span as completed.
///
/// // Step 5: Ensure all spans are flushed before exiting.
/// provider.shutdown();
/// # }
/// ```
use std::sync::mpsc::sync_channel;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::mpsc::SyncSender;

/// Messages exchanged between the main thread and the background thread.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
enum BatchMessage {
    //ExportSpan(SpanData),
    ExportSpan(Arc<AtomicBool>),
    ForceFlush(SyncSender<OTelSdkResult>),
    Shutdown(SyncSender<OTelSdkResult>),
    SetResource(Arc<Resource>),
}

/// The `BatchSpanProcessor` collects finished spans in a buffer and exports them
/// in batches to the configured `SpanExporter`. This processor is ideal for
/// high-throughput environments, as it minimizes the overhead of exporting spans
/// individually. It uses a **dedicated background thread** to manage and export spans
/// asynchronously, ensuring that the application's main execution flow is not blocked.
///
/// This processor supports the following configurations:
/// - **Queue size**: Maximum number of spans that can be buffered.
/// - **Batch size**: Maximum number of spans to include in a single export.
/// - **Scheduled delay**: Frequency at which the batch is exported.
///
/// When using this processor with the OTLP Exporter, the following exporter
/// features are supported:
/// - `grpc-tonic`: Requires `TracerProvider` to be created within a tokio runtime.
/// - `reqwest-blocking-client`: Works with a regular `main` or `tokio::main`.
///
/// In other words, async HTTP clients like `reqwest-client` and `hyper-client`
/// are not supported by this default processor. The OTLP HTTP exporter chooses
/// its default HTTP client from enabled crate features and cannot tell which
/// processor will drive it. If your dependency graph enables async HTTP client
/// features, either pass an explicit blocking client for this processor or use
/// the experimental async-runtime batch span processor.
///
/// `BatchSpanProcessor` buffers spans in memory and exports them in batches. An
/// export is triggered when `max_export_batch_size` is reached or every
/// `scheduled_delay` milliseconds. Users can explicitly trigger an export using
/// the `force_flush` method. Shutdown also triggers an export of all buffered
/// spans and is recommended to be called before the application exits to ensure
/// all buffered spans are exported.
///
/// **Warning**: When using tokio's current-thread runtime, `shutdown()`, which
/// is a blocking call ,should not be called from your main thread. This can
/// cause deadlock. Instead, call `shutdown()` from a separate thread or use
/// tokio's `spawn_blocking`.
///
#[derive(Debug)]
pub struct BatchSpanProcessor {
    span_sender: SyncSender<SpanData>, // Data channel to store spans
    message_sender: SyncSender<BatchMessage>, // Control channel to store control messages.
    handle: Mutex<Option<thread::JoinHandle<()>>>,
    forceflush_timeout: Duration,
    export_span_message_sent: Arc<AtomicBool>,
    current_batch_size: Arc<AtomicUsize>,
    max_export_batch_size: usize,
    dropped_spans_count: AtomicUsize,
    max_queue_size: usize,
}

impl BatchSpanProcessor {
    /// Creates a new instance of `BatchSpanProcessor`.
    pub fn new<E>(
        mut exporter: E,
        config: BatchConfig,
        //max_queue_size: usize,
        //scheduled_delay: Duration,
        //shutdown_timeout: Duration,
    ) -> Self
    where
        E: SpanExporter + Send + 'static,
    {
        let (span_sender, span_receiver) = sync_channel::<SpanData>(config.max_queue_size);
        let (message_sender, message_receiver) = sync_channel::<BatchMessage>(64); // Is this a reasonable bound?
        let max_queue_size = config.max_queue_size;
        let max_export_batch_size = config.max_export_batch_size;
        let current_batch_size = Arc::new(AtomicUsize::new(0));
        let current_batch_size_for_thread = current_batch_size.clone();

        let handle = thread::Builder::new()
            .name("OpenTelemetry.Traces.BatchProcessor".to_string())
            .spawn(move || {
                let _suppress_guard = Context::enter_telemetry_suppressed_scope();
                otel_debug!(
                    name: "BatchSpanProcessor.ThreadStarted",
                    interval_in_millisecs = config.scheduled_delay.as_millis(),
                    max_export_batch_size = config.max_export_batch_size,
                    max_queue_size = config.max_queue_size,
                );
                let mut spans = Vec::with_capacity(config.max_export_batch_size);
                let mut last_export_time = Instant::now();
                let current_batch_size = current_batch_size_for_thread;
                loop {
                    let remaining_time_option = config
                        .scheduled_delay
                        .checked_sub(last_export_time.elapsed());
                    let remaining_time = match remaining_time_option {
                        Some(remaining_time) => remaining_time,
                        None => config.scheduled_delay,
                    };
                    match message_receiver.recv_timeout(remaining_time) {
                        Ok(message) => match message {
                            BatchMessage::ExportSpan(export_span_message_sent) => {
                                // Reset the export span message sent flag now it has has been processed.
                                export_span_message_sent.store(false, Ordering::Relaxed);
                                otel_debug!(
                                    name: "BatchSpanProcessor.ExportingDueToBatchSize",
                                );
                                let _ = Self::get_spans_and_export(
                                    &span_receiver,
                                    &exporter,
                                    &mut spans,
                                    &mut last_export_time,
                                    &current_batch_size,
                                    &config,
                                );
                            }
                            BatchMessage::ForceFlush(sender) => {
                                otel_debug!(name: "BatchSpanProcessor.ExportingDueToForceFlush");
                                let result = Self::get_spans_and_export(
                                    &span_receiver,
                                    &exporter,
                                    &mut spans,
                                    &mut last_export_time,
                                    &current_batch_size,
                                    &config,
                                );
                                let _ = sender.send(result);
                            }
                            BatchMessage::Shutdown(sender) => {
                                otel_debug!(name: "BatchSpanProcessor.ExportingDueToShutdown");
                                let result = Self::get_spans_and_export(
                                    &span_receiver,
                                    &exporter,
                                    &mut spans,
                                    &mut last_export_time,
                                    &current_batch_size,
                                    &config,
                                );
                                let _ = exporter.shutdown();
                                let _ = sender.send(result);

                                otel_debug!(
                                    name: "BatchSpanProcessor.ThreadExiting",
                                    reason = "ShutdownRequested"
                                );
                                //
                                // break out the loop and return from the current background thread.
                                //
                                break;
                            }
                            BatchMessage::SetResource(resource) => {
                                exporter.set_resource(&resource);
                            }
                        },
                        Err(RecvTimeoutError::Timeout) => {
                            otel_debug!(
                                name: "BatchSpanProcessor.ExportingDueToTimer",
                            );

                            let _ = Self::get_spans_and_export(
                                &span_receiver,
                                &exporter,
                                &mut spans,
                                &mut last_export_time,
                                &current_batch_size,
                                &config,
                            );
                        }
                        Err(RecvTimeoutError::Disconnected) => {
                            // Channel disconnected, only thing to do is break
                            // out (i.e exit the thread)
                            otel_debug!(
                                name: "BatchSpanProcessor.ThreadExiting",
                                reason = "MessageSenderDisconnected"
                            );
                            break;
                        }
                    }
                }
                otel_debug!(
                    name: "BatchSpanProcessor.ThreadStopped"
                );
            })
            .expect("Failed to spawn thread"); //TODO: Handle thread spawn failure

        Self {
            span_sender,
            message_sender,
            handle: Mutex::new(Some(handle)),
            forceflush_timeout: Duration::from_secs(5), // TODO: make this configurable
            dropped_spans_count: AtomicUsize::new(0),
            max_queue_size,
            export_span_message_sent: Arc::new(AtomicBool::new(false)),
            current_batch_size,
            max_export_batch_size,
        }
    }

    /// builder
    pub fn builder<E>(exporter: E) -> BatchSpanProcessorBuilder<E>
    where
        E: SpanExporter + Send + 'static,
    {
        BatchSpanProcessorBuilder {
            exporter,
            config: BatchConfig::default(),
        }
    }

    // This method gets up to `max_export_batch_size` amount of spans from the channel and exports them.
    // It returns the result of the export operation.
    // It expects the spans vec to be empty when it's called.
    #[inline]
    fn get_spans_and_export<E>(
        spans_receiver: &Receiver<SpanData>,
        exporter: &E,
        spans: &mut Vec<SpanData>,
        last_export_time: &mut Instant,
        current_batch_size: &AtomicUsize,
        config: &BatchConfig,
    ) -> OTelSdkResult
    where
        E: SpanExporter + Send + Sync + 'static,
    {
        let target = current_batch_size.load(Ordering::Acquire); // `target` is used to determine the stopping criteria for exporting spans.
        let mut result = OTelSdkResult::Ok(());
        let mut total_exported_spans: usize = 0;

        while target > 0 && total_exported_spans < target {
            let batch_limit = config
                .max_export_batch_size
                .min(target - total_exported_spans);

            // Get up to the remaining target batch size from the channel and push them to the spans vec
            while let Ok(span) = spans_receiver.try_recv() {
                spans.push(span);
                if spans.len() == batch_limit {
                    break;
                }
            }

            let count_of_spans = spans.len(); // Count of spans that will be exported
            if count_of_spans == 0 {
                break;
            }
            total_exported_spans += count_of_spans;

            result = Self::export_batch_sync(exporter, spans, last_export_time); // This method clears the spans vec after exporting

            current_batch_size.fetch_sub(count_of_spans, Ordering::AcqRel);
        }
        result
    }

    #[allow(clippy::vec_box)]
    fn export_batch_sync<E>(
        exporter: &E,
        batch: &mut Vec<SpanData>,
        last_export_time: &mut Instant,
    ) -> OTelSdkResult
    where
        E: SpanExporter + ?Sized,
    {
        *last_export_time = Instant::now();

        if batch.is_empty() {
            return OTelSdkResult::Ok(());
        }

        // Splitting off batch clears the existing batch capacity, and is ready
        // for re-use in the next export. The newly returned vec! from split_off
        // is passed to the exporter.
        // TODO: Compared to Logs, this requires new allocation for vec for
        // every export. See if this can be optimized by
        // *not* requiring ownership in the exporter.
        let export = exporter.export(batch.split_off(0));
        let export_result = futures_executor::block_on(export);

        match export_result {
            Ok(_) => OTelSdkResult::Ok(()),
            Err(err) => {
                otel_error!(
                    name: "BatchSpanProcessor.ExportError",
                    error = format!("{}", err)
                );
                OTelSdkResult::Err(err)
            }
        }
    }
}

impl SpanProcessor for BatchSpanProcessor {
    /// Handles span start.
    fn on_start(&self, _span: &mut Span, _cx: &Context) {
        // Ignored
    }

    /// Handles span end.
    fn on_end(&self, span: SpanData) {
        let result = self.span_sender.try_send(span);

        // match for result and handle each separately
        match result {
            Ok(_) => {
                // Successfully sent the span to the data channel.
                // Increment the current batch size and check if it has reached
                // the max export batch size.
                if self.current_batch_size.fetch_add(1, Ordering::AcqRel) + 1
                    >= self.max_export_batch_size
                {
                    // Check if the a control message for exporting spans is
                    // already sent to the worker thread. If not, send a control
                    // message to export spans. `export_span_message_sent` is set
                    // to false ONLY when the worker thread has processed the
                    // control message.

                    if !self.export_span_message_sent.load(Ordering::Relaxed) {
                        // This is a cost-efficient check as atomic load
                        // operations do not require exclusive access to cache
                        // line. Perform atomic swap to
                        // `export_span_message_sent` ONLY when the atomic load
                        // operation above returns false. Atomic
                        // swap/compare_exchange operations require exclusive
                        // access to cache line on most processor architectures.
                        // We could have used compare_exchange as well here, but
                        // it's more verbose than swap.
                        if !self.export_span_message_sent.swap(true, Ordering::Relaxed) {
                            match self.message_sender.try_send(BatchMessage::ExportSpan(
                                self.export_span_message_sent.clone(),
                            )) {
                                Ok(_) => {
                                    // Control message sent successfully.
                                }
                                Err(_err) => {
                                    // TODO: Log error If the control message
                                    // could not be sent, reset the
                                    // `export_span_message_sent` flag.
                                    self.export_span_message_sent
                                        .store(false, Ordering::Relaxed);
                                }
                            }
                        }
                    }
                }
            }
            Err(std::sync::mpsc::TrySendError::Full(_)) => {
                // Increment dropped spans count. The first time we have to drop
                // a span, emit a warning.
                if self.dropped_spans_count.fetch_add(1, Ordering::Relaxed) == 0 {
                    otel_warn!(name: "BatchSpanProcessor.SpanDroppingStarted",
                        message = "BatchSpanProcessor dropped a Span due to queue full. No further log will be emitted for further drops until Shutdown. During Shutdown time, a log will be emitted with exact count of total spans dropped.");
                }
            }
            Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
                // Given background thread is the only receiver, and it's
                // disconnected, it indicates the thread is shutdown
                otel_warn!(
                    name: "BatchSpanProcessor.OnEnd.AfterShutdown",
                    message = "Spans are being emitted even after Shutdown. This indicates incorrect lifecycle management of TracerProvider in application. Spans will not be exported."
                );
            }
        }
    }

    /// Flushes all pending spans.
    fn force_flush(&self) -> OTelSdkResult {
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        match self
            .message_sender
            .try_send(BatchMessage::ForceFlush(sender))
        {
            Ok(_) => receiver
                .recv_timeout(self.forceflush_timeout)
                .map_err(|err| {
                    if err == std::sync::mpsc::RecvTimeoutError::Timeout {
                        OTelSdkError::Timeout(self.forceflush_timeout)
                    } else {
                        OTelSdkError::InternalFailure(format!("{err}"))
                    }
                })?,
            Err(std::sync::mpsc::TrySendError::Full(_)) => {
                // If the control message could not be sent, emit a warning.
                otel_debug!(
                    name: "BatchSpanProcessor.ForceFlush.ControlChannelFull",
                    message = "Control message to flush the worker thread could not be sent as the control channel is full. This can occur if user repeatedly calls force_flush/shutdown without finishing the previous call."
                );
                Err(OTelSdkError::InternalFailure("ForceFlush cannot be performed as Control channel is full. This can occur if user repeatedly calls force_flush/shutdown without finishing the previous call.".into()))
            }
            Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
                // Given background thread is the only receiver, and it's
                // disconnected, it indicates the thread is shutdown
                otel_debug!(
                    name: "BatchSpanProcessor.ForceFlush.AlreadyShutdown",
                    message = "ForceFlush invoked after Shutdown. This will not perform Flush and indicates a incorrect lifecycle management in Application."
                );

                Err(OTelSdkError::AlreadyShutdown)
            }
        }
    }

    /// Shuts down the processor.
    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        let dropped_spans = self.dropped_spans_count.load(Ordering::Relaxed);
        let max_queue_size = self.max_queue_size;
        if dropped_spans > 0 {
            otel_warn!(
                name: "BatchSpanProcessor.SpansDropped",
                dropped_span_count = dropped_spans,
                max_queue_size = max_queue_size,
                message = "Spans were dropped due to a queue being full. The count represents the total count of spans dropped in the lifetime of this BatchSpanProcessor. Consider increasing the queue size and/or decrease delay between intervals."
            );
        }

        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        match self.message_sender.try_send(BatchMessage::Shutdown(sender)) {
            Ok(_) => {
                receiver
                    .recv_timeout(timeout)
                    .map(|_| {
                        // join the background thread after receiving back the
                        // shutdown signal
                        if let Some(handle) = self.handle.lock().unwrap().take() {
                            handle.join().unwrap();
                        }
                        OTelSdkResult::Ok(())
                    })
                    .map_err(|err| match err {
                        std::sync::mpsc::RecvTimeoutError::Timeout => {
                            otel_error!(
                                name: "BatchSpanProcessor.Shutdown.Timeout",
                                message = "BatchSpanProcessor shutdown timing out."
                            );
                            OTelSdkError::Timeout(timeout)
                        }
                        _ => {
                            otel_error!(
                                name: "BatchSpanProcessor.Shutdown.Error",
                                error = format!("{}", err)
                            );
                            OTelSdkError::InternalFailure(format!("{err}"))
                        }
                    })?
            }
            Err(std::sync::mpsc::TrySendError::Full(_)) => {
                // If the control message could not be sent, emit a warning.
                otel_debug!(
                    name: "BatchSpanProcessor.Shutdown.ControlChannelFull",
                    message = "Control message to shutdown the worker thread could not be sent as the control channel is full. This can occur if user repeatedly calls force_flush/shutdown without finishing the previous call."
                );
                Err(OTelSdkError::InternalFailure("Shutdown cannot be performed as Control channel is full. This can occur if user repeatedly calls force_flush/shutdown without finishing the previous call.".into()))
            }
            Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
                // Given background thread is the only receiver, and it's
                // disconnected, it indicates the thread is shutdown
                otel_debug!(
                    name: "BatchSpanProcessor.Shutdown.AlreadyShutdown",
                    message = "Shutdown is being invoked more than once. This is noop, but indicates a potential issue in the application's lifecycle management."
                );

                Err(OTelSdkError::AlreadyShutdown)
            }
        }
    }

    /// Set the resource for the processor.
    fn set_resource(&mut self, resource: &Resource) {
        let resource = Arc::new(resource.clone());
        let _ = self
            .message_sender
            .try_send(BatchMessage::SetResource(resource));
    }
}

/// Builder for `BatchSpanProcessorDedicatedThread`.
#[derive(Debug, Default)]
pub struct BatchSpanProcessorBuilder<E>
where
    E: SpanExporter + Send + 'static,
{
    exporter: E,
    config: BatchConfig,
}

impl<E> BatchSpanProcessorBuilder<E>
where
    E: SpanExporter + Send + 'static,
{
    /// Set the BatchConfig for [BatchSpanProcessorBuilder]
    pub fn with_batch_config(self, config: BatchConfig) -> Self {
        BatchSpanProcessorBuilder { config, ..self }
    }

    /// Build a new instance of `BatchSpanProcessor`.
    pub fn build(self) -> BatchSpanProcessor {
        BatchSpanProcessor::new(self.exporter, self.config)
    }
}

/// Batch span processor configuration.
/// Use [`BatchConfigBuilder`] to configure your own instance of [`BatchConfig`].
#[derive(Debug)]
pub struct BatchConfig {
    /// The maximum queue size to buffer spans for delayed processing. If the
    /// queue gets full it drops the spans. The default value of is 2048.
    pub(crate) max_queue_size: usize,

    /// The delay interval in milliseconds between two consecutive processing
    /// of batches. The default value is 5 seconds.
    pub(crate) scheduled_delay: Duration,

    #[allow(dead_code)]
    /// The maximum number of spans to process in a single batch. If there are
    /// more than one batch worth of spans then it processes multiple batches
    /// of spans one batch after the other without any delay. The default value
    /// is 512.
    pub(crate) max_export_batch_size: usize,

    #[allow(dead_code)]
    /// The maximum duration to export a batch of data.
    pub(crate) max_export_timeout: Duration,

    #[allow(dead_code)]
    pub(crate) max_concurrent_exports: usize,
}

impl Default for BatchConfig {
    fn default() -> Self {
        BatchConfigBuilder::default().build()
    }
}

/// A builder for creating [`BatchConfig`] instances.
#[derive(Debug)]
pub struct BatchConfigBuilder {
    max_queue_size: usize,
    scheduled_delay: Duration,
    max_export_batch_size: usize,
    max_export_timeout: Duration,
    max_concurrent_exports: usize,
}

impl Default for BatchConfigBuilder {
    /// Create a new [`BatchConfigBuilder`] initialized with default batch config values as per the specs.
    /// The values are overriden by environment variables if set.
    /// The supported environment variables are:
    /// * `OTEL_BSP_MAX_QUEUE_SIZE`
    /// * `OTEL_BSP_SCHEDULE_DELAY`
    /// * `OTEL_BSP_MAX_EXPORT_BATCH_SIZE`
    /// * `OTEL_BSP_EXPORT_TIMEOUT`
    /// * `OTEL_BSP_MAX_CONCURRENT_EXPORTS`
    ///
    /// Note: Programmatic configuration overrides any value set via the environment variable.
    fn default() -> Self {
        BatchConfigBuilder {
            max_queue_size: OTEL_BSP_MAX_QUEUE_SIZE_DEFAULT,
            scheduled_delay: OTEL_BSP_SCHEDULE_DELAY_DEFAULT,
            max_export_batch_size: OTEL_BSP_MAX_EXPORT_BATCH_SIZE_DEFAULT,
            max_export_timeout: OTEL_BSP_EXPORT_TIMEOUT_DEFAULT,
            max_concurrent_exports: OTEL_BSP_MAX_CONCURRENT_EXPORTS_DEFAULT,
        }
        .init_from_env_vars()
    }
}

impl BatchConfigBuilder {
    /// Set max_queue_size for [`BatchConfigBuilder`].
    /// It's the maximum queue size to buffer spans for delayed processing.
    /// If the queue gets full it will drops the spans.
    /// The default value is 2048.
    ///
    /// Corresponding environment variable: `OTEL_BSP_MAX_QUEUE_SIZE`.
    ///
    /// Note: Programmatically setting this will override any value set via the environment variable.
    pub fn with_max_queue_size(mut self, max_queue_size: usize) -> Self {
        self.max_queue_size = max_queue_size;
        self
    }

    /// Set max_export_batch_size for [`BatchConfigBuilder`].
    /// It's the maximum number of spans to process in a single batch. If there are
    /// more than one batch worth of spans then it processes multiple batches
    /// of spans one batch after the other without any delay. The default value
    /// is 512.
    ///
    /// Corresponding environment variable: `OTEL_BSP_MAX_EXPORT_BATCH_SIZE`.
    ///
    /// Note: Programmatically setting this will override any value set via the environment variable.
    pub fn with_max_export_batch_size(mut self, max_export_batch_size: usize) -> Self {
        self.max_export_batch_size = max_export_batch_size;
        self
    }

    #[cfg(feature = "experimental_trace_batch_span_processor_with_async_runtime")]
    /// Set max_concurrent_exports for [`BatchConfigBuilder`].
    ///
    /// This value is honored by
    /// `span_processor_with_async_runtime::BatchSpanProcessor`, where it limits
    /// the number of concurrent export tasks.
    ///
    /// The thread-based `BatchSpanProcessor` exports serially and ignores this
    /// setting.
    ///
    /// Corresponding environment variable: `OTEL_BSP_MAX_CONCURRENT_EXPORTS`.
    ///
    /// For concurrent exports, enable
    /// `experimental_trace_batch_span_processor_with_async_runtime` and use the
    /// async-runtime processor.
    ///
    /// Note: Programmatically setting this will override any value set via the environment variable.
    pub fn with_max_concurrent_exports(mut self, max_concurrent_exports: usize) -> Self {
        self.max_concurrent_exports = max_concurrent_exports;
        self
    }

    /// Set scheduled_delay_duration for [`BatchConfigBuilder`].
    /// It's the delay interval in milliseconds between two consecutive processing of batches.
    /// The default value is 5000 milliseconds.
    ///
    /// Corresponding environment variable: `OTEL_BSP_SCHEDULE_DELAY`.
    ///
    /// Note: Programmatically setting this will override any value set via the environment variable.
    pub fn with_scheduled_delay(mut self, scheduled_delay: Duration) -> Self {
        self.scheduled_delay = scheduled_delay;
        self
    }

    /// Set max_export_timeout for [`BatchConfigBuilder`].
    /// It's the maximum duration to export a batch of data.
    /// The The default value is 30000 milliseconds.
    ///
    /// Corresponding environment variable: `OTEL_BSP_EXPORT_TIMEOUT`.
    ///
    /// Note: Programmatically setting this will override any value set via the environment variable.
    #[cfg(feature = "experimental_trace_batch_span_processor_with_async_runtime")]
    pub fn with_max_export_timeout(mut self, max_export_timeout: Duration) -> Self {
        self.max_export_timeout = max_export_timeout;
        self
    }

    /// Builds a `BatchConfig` enforcing the following invariants:
    /// * `max_export_batch_size` must be less than or equal to `max_queue_size`.
    pub fn build(self) -> BatchConfig {
        // max export batch size must be less or equal to max queue size.
        // we set max export batch size to max queue size if it's larger than max queue size.
        let max_export_batch_size = min(self.max_export_batch_size, self.max_queue_size);

        BatchConfig {
            max_queue_size: self.max_queue_size,
            scheduled_delay: self.scheduled_delay,
            max_export_timeout: self.max_export_timeout,
            max_concurrent_exports: self.max_concurrent_exports,
            max_export_batch_size,
        }
    }

    fn init_from_env_vars(mut self) -> Self {
        if let Some(max_concurrent_exports) = env::var(OTEL_BSP_MAX_CONCURRENT_EXPORTS)
            .ok()
            .and_then(|max_concurrent_exports| usize::from_str(&max_concurrent_exports).ok())
        {
            self.max_concurrent_exports = max_concurrent_exports;
        }

        if let Some(max_queue_size) = env::var(OTEL_BSP_MAX_QUEUE_SIZE)
            .ok()
            .and_then(|queue_size| usize::from_str(&queue_size).ok())
        {
            self.max_queue_size = max_queue_size;
        }

        if let Some(scheduled_delay) = env::var(OTEL_BSP_SCHEDULE_DELAY)
            .ok()
            .and_then(|delay| u64::from_str(&delay).ok())
        {
            self.scheduled_delay = Duration::from_millis(scheduled_delay);
        }

        if let Some(max_export_batch_size) = env::var(OTEL_BSP_MAX_EXPORT_BATCH_SIZE)
            .ok()
            .and_then(|batch_size| usize::from_str(&batch_size).ok())
        {
            self.max_export_batch_size = max_export_batch_size;
        }

        // max export batch size must be less or equal to max queue size.
        // we set max export batch size to max queue size if it's larger than max queue size.
        if self.max_export_batch_size > self.max_queue_size {
            self.max_export_batch_size = self.max_queue_size;
        }

        if let Some(max_export_timeout) = env::var(OTEL_BSP_EXPORT_TIMEOUT)
            .ok()
            .and_then(|timeout| u64::from_str(&timeout).ok())
        {
            self.max_export_timeout = Duration::from_millis(max_export_timeout);
        }

        self
    }
}

#[cfg(all(test, feature = "testing", feature = "trace"))]
mod tests {
    // cargo test trace::span_processor::tests:: --features=testing
    use super::{
        BatchSpanProcessor, SimpleSpanProcessor, SpanProcessor, OTEL_BSP_EXPORT_TIMEOUT,
        OTEL_BSP_MAX_EXPORT_BATCH_SIZE, OTEL_BSP_MAX_QUEUE_SIZE, OTEL_BSP_MAX_QUEUE_SIZE_DEFAULT,
        OTEL_BSP_SCHEDULE_DELAY, OTEL_BSP_SCHEDULE_DELAY_DEFAULT,
    };
    use crate::error::OTelSdkResult;
    use crate::testing::trace::new_test_export_span_data;
    use crate::trace::span_processor::{
        OTEL_BSP_EXPORT_TIMEOUT_DEFAULT, OTEL_BSP_MAX_CONCURRENT_EXPORTS,
        OTEL_BSP_MAX_CONCURRENT_EXPORTS_DEFAULT, OTEL_BSP_MAX_EXPORT_BATCH_SIZE_DEFAULT,
    };
    use crate::trace::InMemorySpanExporterBuilder;
    use crate::trace::{BatchConfig, BatchConfigBuilder, SpanEvents, SpanLinks};
    use crate::trace::{SpanData, SpanExporter};
    use opentelemetry::trace::{SpanContext, SpanId, SpanKind, Status};
    use std::fmt::Debug;
    use std::time::Duration;

    #[test]
    fn simple_span_processor_on_end_calls_export() {
        let exporter = InMemorySpanExporterBuilder::new().build();
        let processor = SimpleSpanProcessor::new(exporter.clone());
        let span_data = new_test_export_span_data();
        processor.on_end(span_data.clone());
        assert_eq!(exporter.get_finished_spans().unwrap()[0], span_data);
        let _result = processor.shutdown();
    }

    #[test]
    fn simple_span_processor_on_end_skips_export_if_not_sampled() {
        let exporter = InMemorySpanExporterBuilder::new().build();
        let processor = SimpleSpanProcessor::new(exporter.clone());
        let unsampled = SpanData {
            span_context: SpanContext::empty_context(),
            parent_span_id: SpanId::INVALID,
            parent_span_is_remote: false,
            span_kind: SpanKind::Internal,
            name: "opentelemetry".into(),
            start_time: opentelemetry::time::now(),
            end_time: opentelemetry::time::now(),
            attributes: Vec::new(),
            dropped_attributes_count: 0,
            events: SpanEvents::default(),
            links: SpanLinks::default(),
            status: Status::Unset,
            instrumentation_scope: Default::default(),
        };
        processor.on_end(unsampled);
        assert!(exporter.get_finished_spans().unwrap().is_empty());
    }

    #[test]
    fn simple_span_processor_shutdown_calls_shutdown() {
        let exporter = InMemorySpanExporterBuilder::new().build();
        let processor = SimpleSpanProcessor::new(exporter.clone());
        let span_data = new_test_export_span_data();
        processor.on_end(span_data.clone());
        assert!(!exporter.get_finished_spans().unwrap().is_empty());
        let _result = processor.shutdown();
        // Assume shutdown is called by ensuring spans are empty in the exporter
        assert!(exporter.get_finished_spans().unwrap().is_empty());
    }

    #[test]
    fn test_default_const_values() {
        assert_eq!(OTEL_BSP_MAX_QUEUE_SIZE, "OTEL_BSP_MAX_QUEUE_SIZE");
        assert_eq!(OTEL_BSP_MAX_QUEUE_SIZE_DEFAULT, 2048);
        assert_eq!(OTEL_BSP_SCHEDULE_DELAY, "OTEL_BSP_SCHEDULE_DELAY");
        assert_eq!(OTEL_BSP_SCHEDULE_DELAY_DEFAULT.as_millis(), 5000);
        assert_eq!(
            OTEL_BSP_MAX_EXPORT_BATCH_SIZE,
            "OTEL_BSP_MAX_EXPORT_BATCH_SIZE"
        );
        assert_eq!(OTEL_BSP_MAX_EXPORT_BATCH_SIZE_DEFAULT, 512);
        assert_eq!(OTEL_BSP_EXPORT_TIMEOUT, "OTEL_BSP_EXPORT_TIMEOUT");
        assert_eq!(OTEL_BSP_EXPORT_TIMEOUT_DEFAULT.as_millis(), 30000);
    }

    #[test]
    fn test_default_batch_config_adheres_to_specification() {
        let env_vars = vec![
            OTEL_BSP_SCHEDULE_DELAY,
            OTEL_BSP_EXPORT_TIMEOUT,
            OTEL_BSP_MAX_QUEUE_SIZE,
            OTEL_BSP_MAX_EXPORT_BATCH_SIZE,
            OTEL_BSP_MAX_CONCURRENT_EXPORTS,
        ];

        let config = temp_env::with_vars_unset(env_vars, BatchConfig::default);

        assert_eq!(
            config.max_concurrent_exports,
            OTEL_BSP_MAX_CONCURRENT_EXPORTS_DEFAULT
        );
        assert_eq!(config.scheduled_delay, OTEL_BSP_SCHEDULE_DELAY_DEFAULT);
        assert_eq!(config.max_export_timeout, OTEL_BSP_EXPORT_TIMEOUT_DEFAULT);
        assert_eq!(config.max_queue_size, OTEL_BSP_MAX_QUEUE_SIZE_DEFAULT);
        assert_eq!(
            config.max_export_batch_size,
            OTEL_BSP_MAX_EXPORT_BATCH_SIZE_DEFAULT
        );
    }

    #[test]
    fn test_code_based_config_overrides_env_vars() {
        let env_vars = vec![
            (OTEL_BSP_EXPORT_TIMEOUT, Some("60000")),
            (OTEL_BSP_MAX_CONCURRENT_EXPORTS, Some("5")),
            (OTEL_BSP_MAX_EXPORT_BATCH_SIZE, Some("1024")),
            (OTEL_BSP_MAX_QUEUE_SIZE, Some("4096")),
            (OTEL_BSP_SCHEDULE_DELAY, Some("2000")),
        ];

        temp_env::with_vars(env_vars, || {
            let config = BatchConfigBuilder::default()
                .with_max_export_batch_size(512)
                .with_max_queue_size(2048)
                .with_scheduled_delay(Duration::from_millis(1000));
            #[cfg(feature = "experimental_trace_batch_span_processor_with_async_runtime")]
            let config = {
                config
                    .with_max_concurrent_exports(10)
                    .with_max_export_timeout(Duration::from_millis(2000))
            };
            let config = config.build();

            assert_eq!(config.max_export_batch_size, 512);
            assert_eq!(config.max_queue_size, 2048);
            assert_eq!(config.scheduled_delay, Duration::from_millis(1000));
            #[cfg(feature = "experimental_trace_batch_span_processor_with_async_runtime")]
            {
                assert_eq!(config.max_concurrent_exports, 10);
                assert_eq!(config.max_export_timeout, Duration::from_millis(2000));
            }
        });
    }

    #[test]
    fn test_batch_config_configurable_by_env_vars() {
        let env_vars = vec![
            (OTEL_BSP_SCHEDULE_DELAY, Some("2000")),
            (OTEL_BSP_EXPORT_TIMEOUT, Some("60000")),
            (OTEL_BSP_MAX_QUEUE_SIZE, Some("4096")),
            (OTEL_BSP_MAX_EXPORT_BATCH_SIZE, Some("1024")),
        ];

        let config = temp_env::with_vars(env_vars, BatchConfig::default);

        assert_eq!(config.scheduled_delay, Duration::from_millis(2000));
        assert_eq!(config.max_export_timeout, Duration::from_millis(60000));
        assert_eq!(config.max_queue_size, 4096);
        assert_eq!(config.max_export_batch_size, 1024);
    }

    #[test]
    fn test_batch_config_max_export_batch_size_validation() {
        let env_vars = vec![
            (OTEL_BSP_MAX_QUEUE_SIZE, Some("256")),
            (OTEL_BSP_MAX_EXPORT_BATCH_SIZE, Some("1024")),
        ];

        let config = temp_env::with_vars(env_vars, BatchConfig::default);

        assert_eq!(config.max_queue_size, 256);
        assert_eq!(config.max_export_batch_size, 256);
        assert_eq!(config.scheduled_delay, OTEL_BSP_SCHEDULE_DELAY_DEFAULT);
        assert_eq!(config.max_export_timeout, OTEL_BSP_EXPORT_TIMEOUT_DEFAULT);
    }

    #[test]
    fn test_batch_config_with_fields() {
        let batch = BatchConfigBuilder::default()
            .with_max_export_batch_size(10)
            .with_scheduled_delay(Duration::from_millis(10))
            .with_max_queue_size(10);
        #[cfg(feature = "experimental_trace_batch_span_processor_with_async_runtime")]
        let batch = {
            batch
                .with_max_concurrent_exports(10)
                .with_max_export_timeout(Duration::from_millis(10))
        };
        let batch = batch.build();
        assert_eq!(batch.max_export_batch_size, 10);
        assert_eq!(batch.scheduled_delay, Duration::from_millis(10));
        assert_eq!(batch.max_queue_size, 10);
        #[cfg(feature = "experimental_trace_batch_span_processor_with_async_runtime")]
        {
            assert_eq!(batch.max_concurrent_exports, 10);
            assert_eq!(batch.max_export_timeout, Duration::from_millis(10));
        }
    }

    // Helper function to create a default test span
    fn create_test_span(name: &str) -> SpanData {
        SpanData {
            span_context: SpanContext::empty_context(),
            parent_span_id: SpanId::INVALID,
            parent_span_is_remote: false,
            span_kind: SpanKind::Internal,
            name: name.to_string().into(),
            start_time: opentelemetry::time::now(),
            end_time: opentelemetry::time::now(),
            attributes: Vec::new(),
            dropped_attributes_count: 0,
            events: SpanEvents::default(),
            links: SpanLinks::default(),
            status: Status::Unset,
            instrumentation_scope: Default::default(),
        }
    }

    use crate::Resource;
    use opentelemetry::{Key, KeyValue, Value};
    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex,
        },
        time::Instant,
    };

    // Mock exporter to test functionality
    #[derive(Debug)]
    struct MockSpanExporter {
        exported_spans: Arc<Mutex<Vec<SpanData>>>,
        exported_resource: Arc<Mutex<Option<Resource>>>,
    }

    impl MockSpanExporter {
        fn new() -> Self {
            Self {
                exported_spans: Arc::new(Mutex::new(Vec::new())),
                exported_resource: Arc::new(Mutex::new(None)),
            }
        }
    }

    impl SpanExporter for MockSpanExporter {
        async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
            let exported_spans = self.exported_spans.clone();
            exported_spans.lock().unwrap().extend(batch);
            Ok(())
        }

        fn shutdown(&self) -> OTelSdkResult {
            Ok(())
        }
        fn set_resource(&mut self, resource: &Resource) {
            let mut exported_resource = self.exported_resource.lock().unwrap();
            *exported_resource = Some(resource.clone());
        }
    }

    #[test]
    fn batchspanprocessor_handles_on_end() {
        let exporter = MockSpanExporter::new();
        let exporter_shared = exporter.exported_spans.clone();
        let config = BatchConfigBuilder::default()
            .with_max_queue_size(10)
            .with_max_export_batch_size(10)
            .with_scheduled_delay(Duration::from_secs(5))
            .build();
        let processor = BatchSpanProcessor::new(exporter, config);

        let test_span = create_test_span("test_span");
        processor.on_end(test_span.clone());

        // Wait for flush interval to ensure the span is processed
        std::thread::sleep(Duration::from_secs(6));

        let exported_spans = exporter_shared.lock().unwrap();
        assert_eq!(exported_spans.len(), 1);
        assert_eq!(exported_spans[0].name, "test_span");
    }

    #[test]
    fn batchspanprocessor_force_flush() {
        let exporter = MockSpanExporter::new();
        let exporter_shared = exporter.exported_spans.clone(); // Shared access to verify exported spans
        let config = BatchConfigBuilder::default()
            .with_max_queue_size(10)
            .with_max_export_batch_size(10)
            .with_scheduled_delay(Duration::from_secs(5))
            .build();
        let processor = BatchSpanProcessor::new(exporter, config);

        // Create a test span and send it to the processor
        let test_span = create_test_span("force_flush_span");
        processor.on_end(test_span.clone());

        // Call force_flush to immediately export the spans
        let flush_result = processor.force_flush();
        assert!(flush_result.is_ok(), "Force flush failed unexpectedly");

        // Verify the exported spans in the mock exporter
        let exported_spans = exporter_shared.lock().unwrap();
        assert_eq!(
            exported_spans.len(),
            1,
            "Unexpected number of exported spans"
        );
        assert_eq!(exported_spans[0].name, "force_flush_span");
    }

    #[test]
    fn batchspanprocessor_does_not_overdrain_unaccounted_spans() {
        let exporter = MockSpanExporter::new();
        let exported_spans = exporter.exported_spans.clone();
        let (sender, receiver) = std::sync::mpsc::sync_channel(4);
        let current_batch_size = AtomicUsize::new(1);
        let config = BatchConfigBuilder::default()
            .with_max_queue_size(4)
            .with_max_export_batch_size(4)
            .build();
        let mut spans = Vec::with_capacity(config.max_export_batch_size);
        let mut last_export_time = Instant::now();

        sender.send(create_test_span("counted")).unwrap();
        sender.send(create_test_span("unaccounted")).unwrap();

        let result = BatchSpanProcessor::get_spans_and_export(
            &receiver,
            &exporter,
            &mut spans,
            &mut last_export_time,
            &current_batch_size,
            &config,
        );

        assert!(result.is_ok(), "export should succeed");
        assert_eq!(
            current_batch_size.load(Ordering::Relaxed),
            0,
            "helper should only subtract the counted span"
        );
        assert_eq!(
            exported_spans.lock().unwrap().len(),
            1,
            "helper should export at most the target batch size snapshot"
        );
        assert!(
            receiver.try_recv().is_ok(),
            "one span should remain queued for a later export cycle"
        );
    }

    #[test]
    fn batchspanprocessor_shutdown() {
        // Setup exporter and processor - following the same pattern as test_batch_shutdown from logs
        let exporter = InMemorySpanExporterBuilder::new()
            .keep_records_on_shutdown()
            .build();
        let processor = BatchSpanProcessor::new(exporter.clone(), BatchConfig::default());

        let record = create_test_span("test_span");

        processor.on_end(record);
        processor.force_flush().unwrap();
        processor.shutdown().unwrap();

        // todo: expect to see errors here. How should we assert this?
        processor.on_end(create_test_span("after_shutdown_span"));

        assert_eq!(1, exporter.get_finished_spans().unwrap().len());
        assert!(exporter.is_shutdown_called());
    }

    #[test]
    fn batchspanprocessor_handles_dropped_spans() {
        // This test verifies that BSP drops spans when the queue is full.

        #[derive(Debug)]
        struct SlowExporter {
            exported_count: Arc<std::sync::atomic::AtomicUsize>,
        }

        impl SpanExporter for SlowExporter {
            async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
                // Simulate slow export
                std::thread::sleep(Duration::from_millis(50));
                self.exported_count
                    .fetch_add(batch.len(), Ordering::Relaxed);
                Ok(())
            }

            fn shutdown(&self) -> OTelSdkResult {
                Ok(())
            }

            fn set_resource(&mut self, _resource: &Resource) {}
        }

        let exported_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let exporter = SlowExporter {
            exported_count: exported_count.clone(),
        };

        let max_queue_size = 10;
        let config = BatchConfigBuilder::default()
            .with_max_queue_size(max_queue_size)
            .with_max_export_batch_size(5)
            .with_scheduled_delay(Duration::from_millis(10))
            .build();
        let processor = BatchSpanProcessor::new(exporter, config);

        // Rapidly send many more spans than the queue can hold
        let total_spans_to_send = 100;
        for i in 0..total_spans_to_send {
            let span = create_test_span(&format!("span_{}", i));
            processor.on_end(span);
        }

        // Force flush any remaining spans - this waits for export to complete
        let _ = processor.force_flush();

        let dropped = processor.dropped_spans_count.load(Ordering::Relaxed);
        let exported = exported_count.load(Ordering::Relaxed);

        // Verify that dropped + exported = total (every span is accounted for)
        assert_eq!(
            dropped + exported,
            total_spans_to_send,
            "dropped ({}) + exported ({}) should equal total sent ({})",
            dropped,
            exported,
            total_spans_to_send
        );

        // With 100 spans sent rapidly and a slow exporter, we should have some drops
        assert!(
            dropped > 0,
            "Expected some spans to be dropped due to full queue. Exported: {}",
            exported
        );
    }

    #[test]
    fn batchspanprocessor_sync_ignores_max_concurrent_exports() {
        #[derive(Debug)]
        struct TrackingExporter {
            active: Arc<AtomicUsize>,
            max_inflight: Arc<AtomicUsize>,
            export_calls: Arc<AtomicUsize>,
            delay: Duration,
        }

        impl SpanExporter for TrackingExporter {
            async fn export(&self, _batch: Vec<SpanData>) -> OTelSdkResult {
                self.export_calls.fetch_add(1, Ordering::SeqCst);
                let inflight = self.active.fetch_add(1, Ordering::SeqCst) + 1;
                self.max_inflight.fetch_max(inflight, Ordering::SeqCst);

                std::thread::sleep(self.delay);
                self.active.fetch_sub(1, Ordering::SeqCst);
                Ok(())
            }
        }

        let active = Arc::new(AtomicUsize::new(0));
        let max_inflight = Arc::new(AtomicUsize::new(0));
        let export_calls = Arc::new(AtomicUsize::new(0));
        let exporter = TrackingExporter {
            active: active.clone(),
            max_inflight: max_inflight.clone(),
            export_calls: export_calls.clone(),
            delay: Duration::from_millis(50),
        };

        let config = BatchConfig {
            max_export_batch_size: 1,
            max_queue_size: 16,
            scheduled_delay: Duration::from_secs(3600),
            max_export_timeout: Duration::from_secs(5),
            max_concurrent_exports: 4,
        };

        let processor = BatchSpanProcessor::new(exporter, config);

        processor.on_end(new_test_export_span_data());
        processor.on_end(new_test_export_span_data());
        processor.on_end(new_test_export_span_data());

        processor.force_flush().expect("force flush failed");
        processor.shutdown().expect("shutdown failed");

        assert_eq!(
            export_calls.load(Ordering::SeqCst),
            3,
            "expected three exports for three spans with max_export_batch_size=1"
        );
        assert_eq!(
            max_inflight.load(Ordering::SeqCst),
            1,
            "sync BatchSpanProcessor should export serially regardless of max_concurrent_exports"
        );
    }

    #[test]
    fn validate_span_attributes_exported_correctly() {
        let exporter = MockSpanExporter::new();
        let exporter_shared = exporter.exported_spans.clone();
        let config = BatchConfigBuilder::default().build();
        let processor = BatchSpanProcessor::new(exporter, config);

        // Create a span with attributes
        let mut span_data = create_test_span("attribute_validation");
        span_data.attributes = vec![
            KeyValue::new("key1", "value1"),
            KeyValue::new("key2", "value2"),
        ];
        processor.on_end(span_data.clone());

        // Force flush to export the span
        let _ = processor.force_flush();

        // Validate the exported attributes
        let exported_spans = exporter_shared.lock().unwrap();
        assert_eq!(exported_spans.len(), 1);
        let exported_span = &exported_spans[0];
        assert!(exported_span
            .attributes
            .contains(&KeyValue::new("key1", "value1")));
        assert!(exported_span
            .attributes
            .contains(&KeyValue::new("key2", "value2")));
    }

    #[test]
    fn batchspanprocessor_sets_and_exports_with_resource() {
        let exporter = MockSpanExporter::new();
        let exporter_shared = exporter.exported_spans.clone();
        let resource_shared = exporter.exported_resource.clone();
        let config = BatchConfigBuilder::default().build();
        let mut processor = BatchSpanProcessor::new(exporter, config);

        // Set a resource for the processor
        let resource = Resource::builder_empty()
            .with_attributes(vec![KeyValue::new("service.name", "test_service")])
            .build();
        processor.set_resource(&resource);

        // Create a span and send it to the processor
        let test_span = create_test_span("resource_test");
        processor.on_end(test_span.clone());

        // Force flush to ensure the span is exported
        let _ = processor.force_flush();

        // Validate spans are exported
        let exported_spans = exporter_shared.lock().unwrap();
        assert_eq!(exported_spans.len(), 1);

        // Validate the resource is correctly set in the exporter
        let exported_resource = resource_shared.lock().unwrap();
        assert!(exported_resource.is_some());
        assert_eq!(
            exported_resource
                .as_ref()
                .unwrap()
                .get(&Key::new("service.name")),
            Some(Value::from("test_service"))
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_batch_processor_current_thread_runtime() {
        let exporter = MockSpanExporter::new();
        let exporter_shared = exporter.exported_spans.clone();

        let config = BatchConfigBuilder::default()
            .with_max_queue_size(5)
            .with_max_export_batch_size(3)
            .build();

        let processor = BatchSpanProcessor::new(exporter, config);

        for _ in 0..4 {
            let span = new_test_export_span_data();
            processor.on_end(span);
        }

        processor.force_flush().unwrap();

        let exported_spans = exporter_shared.lock().unwrap();
        assert_eq!(exported_spans.len(), 4);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_batch_processor_multi_thread_count_1_runtime() {
        let exporter = MockSpanExporter::new();
        let exporter_shared = exporter.exported_spans.clone();

        let config = BatchConfigBuilder::default()
            .with_max_queue_size(5)
            .with_max_export_batch_size(3)
            .build();

        let processor = BatchSpanProcessor::new(exporter, config);

        for _ in 0..4 {
            let span = new_test_export_span_data();
            processor.on_end(span);
        }

        processor.force_flush().unwrap();

        let exported_spans = exporter_shared.lock().unwrap();
        assert_eq!(exported_spans.len(), 4);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_batch_processor_multi_thread() {
        let exporter = MockSpanExporter::new();
        let exporter_shared = exporter.exported_spans.clone();

        let config = BatchConfigBuilder::default()
            .with_max_queue_size(20)
            .with_max_export_batch_size(5)
            .build();

        // Create the processor with the thread-safe exporter
        let processor = Arc::new(BatchSpanProcessor::new(exporter, config));

        let mut handles = vec![];
        for _ in 0..10 {
            let processor_clone = Arc::clone(&processor);
            let handle = tokio::spawn(async move {
                let span = new_test_export_span_data();
                processor_clone.on_end(span);
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }

        processor.force_flush().unwrap();

        // Verify exported spans
        let exported_spans = exporter_shared.lock().unwrap();
        assert_eq!(exported_spans.len(), 10);
    }
}
