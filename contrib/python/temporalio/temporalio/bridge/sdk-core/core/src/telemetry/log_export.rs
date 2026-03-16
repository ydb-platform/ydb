use futures_channel::mpsc::{Receiver, Sender, channel};
use parking_lot::Mutex;
use ringbuf::{HeapRb, consumer::Consumer, producer::Producer, traits::Split};
use std::{collections::HashMap, fmt, sync::Arc, time::SystemTime};
use temporal_sdk_core_api::telemetry::{CoreLog, CoreLogConsumer};
use tracing_subscriber::Layer;

#[derive(Debug)]
struct CoreLogFieldStorage(HashMap<String, serde_json::Value>);

pub(super) struct CoreLogConsumerLayer {
    consumer: Arc<dyn CoreLogConsumer>,
}

impl CoreLogConsumerLayer {
    pub(super) fn new(consumer: Arc<dyn CoreLogConsumer>) -> Self {
        Self { consumer }
    }

    pub(super) fn new_buffered(ringbuf_capacity: usize) -> (Self, CoreLogBuffer) {
        let (consumer, buffer) = CoreLogBufferedConsumer::new(ringbuf_capacity);
        (
            Self {
                consumer: Arc::new(consumer),
            },
            buffer,
        )
    }
}

impl<S> Layer<S> for CoreLogConsumerLayer
where
    S: tracing::Subscriber,
    S: for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
{
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let span = ctx.span(id).unwrap();
        let mut fields = HashMap::new();
        let mut visitor = JsonVisitor(&mut fields);
        attrs.record(&mut visitor);
        let storage = CoreLogFieldStorage(fields);
        let mut extensions = span.extensions_mut();
        extensions.insert(storage);
    }

    fn on_record(
        &self,
        id: &tracing::span::Id,
        values: &tracing::span::Record<'_>,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let span = ctx.span(id).unwrap();

        let mut extensions_mut = span.extensions_mut();
        let custom_field_storage: &mut CoreLogFieldStorage =
            extensions_mut.get_mut::<CoreLogFieldStorage>().unwrap();
        let json_data = &mut custom_field_storage.0;

        let mut visitor = JsonVisitor(json_data);
        values.record(&mut visitor);
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let mut fields = HashMap::new();
        let mut visitor = JsonVisitor(&mut fields);
        event.record(&mut visitor);

        let mut spans = vec![];
        if let Some(scope) = ctx.event_scope(event) {
            for span in scope.from_root() {
                let extensions = span.extensions();
                let storage = extensions.get::<CoreLogFieldStorage>().unwrap();
                let field_data = &storage.0;
                for (k, v) in field_data {
                    fields.insert(k.to_string(), v.clone());
                }
                spans.push(span.name().to_string());
            }
        }

        // "message" is the magic default field keyname for the string passed to the event
        let message = fields.remove("message").unwrap_or_default();
        let log = CoreLog {
            target: event.metadata().target().to_string(),
            // This weird as_str dance prevents adding extra quotes
            message: message.as_str().unwrap_or_default().to_string(),
            timestamp: SystemTime::now(),
            level: *event.metadata().level(),
            fields,
            span_contexts: spans,
        };
        self.consumer.on_log(log);
    }
}

/// Core log consumer implementation backed by a ring buffer.
pub struct CoreLogBufferedConsumer {
    logs_in: Mutex<<HeapRb<CoreLog> as Split>::Prod>,
}

impl CoreLogBufferedConsumer {
    /// Create a log consumer and drainable buffer for the given capacity.
    pub fn new(ringbuf_capacity: usize) -> (Self, CoreLogBuffer) {
        let (logs_in, logs_out) = HeapRb::new(ringbuf_capacity).split();
        (
            Self {
                logs_in: Mutex::new(logs_in),
            },
            CoreLogBuffer { logs_out },
        )
    }
}

impl CoreLogConsumer for CoreLogBufferedConsumer {
    fn on_log(&self, log: CoreLog) {
        let _ = self.logs_in.lock().try_push(log);
    }
}

impl fmt::Debug for CoreLogBufferedConsumer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<buffered consumer>")
    }
}

/// Buffer of core logs that can be drained.
pub struct CoreLogBuffer {
    logs_out: <HeapRb<CoreLog> as Split>::Cons,
}

impl CoreLogBuffer {
    /// Drain the buffer of its logs.
    pub fn drain(&mut self) -> Vec<CoreLog> {
        self.logs_out.pop_iter().collect()
    }
}

/// Core log consumer implementation backed by a mpsc channel.
pub struct CoreLogStreamConsumer {
    tx: Sender<CoreLog>,
}

impl CoreLogStreamConsumer {
    /// Create a stream consumer and stream of logs.
    pub fn new(buffer: usize) -> (Self, Receiver<CoreLog>) {
        let (tx, rx) = channel(buffer);
        (Self { tx }, rx)
    }
}

impl CoreLogConsumer for CoreLogStreamConsumer {
    fn on_log(&self, log: CoreLog) {
        // We will drop messages if we can't send
        let _ = self.tx.clone().try_send(log);
    }
}

impl fmt::Debug for CoreLogStreamConsumer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<stream consumer>")
    }
}

struct JsonVisitor<'a>(&'a mut HashMap<String, serde_json::Value>);

impl tracing::field::Visit for JsonVisitor<'_> {
    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.0
            .insert(field.name().to_string(), serde_json::json!(value));
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.0
            .insert(field.name().to_string(), serde_json::json!(value));
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.0
            .insert(field.name().to_string(), serde_json::json!(value));
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.0
            .insert(field.name().to_string(), serde_json::json!(value));
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0
            .insert(field.name().to_string(), serde_json::json!(value));
    }

    fn record_error(
        &mut self,
        field: &tracing::field::Field,
        value: &(dyn std::error::Error + 'static),
    ) {
        self.0.insert(
            field.name().to_string(),
            serde_json::json!(value.to_string()),
        );
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0.insert(
            field.name().to_string(),
            serde_json::json!(format!("{value:?}")),
        );
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        telemetry::{CoreLogStreamConsumer, construct_filter_string},
        telemetry_init,
    };
    use futures_util::stream::StreamExt;
    use std::{
        fmt,
        sync::{Arc, Mutex},
    };
    use temporal_sdk_core_api::telemetry::{
        CoreLog, CoreLogConsumer, CoreTelemetry, Logger, TelemetryOptionsBuilder,
    };
    use tracing::Level;

    #[instrument(fields(bros = "brohemian"))]
    fn instrumented(thing: &str) {
        warn!("warn");
        info!(foo = "bar", "info");
        debug!("debug");
    }

    fn write_logs() {
        let top_span = span!(Level::INFO, "yayspan", huh = "wat");
        let _guard = top_span.enter();
        info!("Whata?");
        instrumented("hi");
        info!("Donezo");
    }

    fn assert_logs(logs: Vec<CoreLog>) {
        // Verify debug log was not forwarded
        assert!(!logs.iter().any(|l| l.message == "debug"));
        assert_eq!(logs.len(), 4);
        // Ensure fields are attached to events properly
        let info_msg = &logs[2];
        assert_eq!(info_msg.message, "info");
        assert_eq!(info_msg.fields.len(), 4);
        assert_eq!(info_msg.fields.get("huh"), Some(&"wat".into()));
        assert_eq!(info_msg.fields.get("foo"), Some(&"bar".into()));
        assert_eq!(info_msg.fields.get("bros"), Some(&"brohemian".into()));
        assert_eq!(info_msg.fields.get("thing"), Some(&"hi".into()));
    }

    #[tokio::test]
    async fn test_forwarding_output() {
        let opts = TelemetryOptionsBuilder::default()
            .logging(Logger::Forward {
                filter: construct_filter_string(Level::INFO, Level::WARN),
            })
            .build()
            .unwrap();
        let instance = telemetry_init(opts).unwrap();
        let _g = tracing::subscriber::set_default(instance.trace_subscriber().unwrap().clone());

        write_logs();
        assert_logs(instance.fetch_buffered_logs());
    }

    struct CaptureConsumer(Mutex<Vec<CoreLog>>);

    impl CoreLogConsumer for CaptureConsumer {
        fn on_log(&self, log: CoreLog) {
            self.0.lock().unwrap().push(log);
        }
    }

    impl fmt::Debug for CaptureConsumer {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("<capture consumer>")
        }
    }

    #[tokio::test]
    async fn test_push_output() {
        let consumer = Arc::new(CaptureConsumer(Mutex::new(Vec::new())));
        let opts = TelemetryOptionsBuilder::default()
            .logging(Logger::Push {
                filter: construct_filter_string(Level::INFO, Level::WARN),
                consumer: consumer.clone(),
            })
            .build()
            .unwrap();
        let instance = telemetry_init(opts).unwrap();
        let _g = tracing::subscriber::set_default(instance.trace_subscriber().unwrap().clone());

        write_logs();
        assert_logs(consumer.0.lock().unwrap().drain(..).collect());
    }

    #[tokio::test]
    async fn test_push_stream_output() {
        let (consumer, stream) = CoreLogStreamConsumer::new(100);
        let consumer = Arc::new(consumer);
        let opts = TelemetryOptionsBuilder::default()
            .logging(Logger::Push {
                filter: construct_filter_string(Level::INFO, Level::WARN),
                consumer: consumer.clone(),
            })
            .build()
            .unwrap();
        let instance = telemetry_init(opts).unwrap();
        let _g = tracing::subscriber::set_default(instance.trace_subscriber().unwrap().clone());

        write_logs();
        assert_logs(stream.ready_chunks(100).next().await.unwrap());
    }
}
