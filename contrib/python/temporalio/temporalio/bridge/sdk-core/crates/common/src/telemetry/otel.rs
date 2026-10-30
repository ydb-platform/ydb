use super::{
    HistogramBucketOverrides, MetricTemporality, OtelCollectorOptions, OtlpProtocol,
    TELEM_SERVICE_NAME,
    metrics::{
        ACTIVITY_EXEC_LATENCY_HISTOGRAM_NAME, ACTIVITY_SCHED_TO_START_LATENCY_HISTOGRAM_NAME,
        CoreMeter, Counter, DEFAULT_MS_BUCKETS, DEFAULT_S_BUCKETS, Gauge, GaugeF64, Histogram,
        HistogramBase, HistogramDuration, HistogramDurationBase, HistogramF64, HistogramF64Base,
        MetricAttributable, MetricAttributes, MetricParameters, NewAttributes, UpDownCounter,
        WORKFLOW_E2E_LATENCY_HISTOGRAM_NAME, WORKFLOW_TASK_EXECUTION_LATENCY_HISTOGRAM_NAME,
        WORKFLOW_TASK_REPLAY_LATENCY_HISTOGRAM_NAME,
        WORKFLOW_TASK_SCHED_TO_START_LATENCY_HISTOGRAM_NAME, default_buckets_for,
    },
};
use crate::dbg_panic;
use opentelemetry::{
    self, Key, KeyValue,
    metrics::{Meter, MeterProvider as MeterProviderT},
};
#[cfg(any(feature = "tls-ring", feature = "tls-aws-lc"))]
use opentelemetry_otlp::tonic_types::transport::ClientTlsConfig;
use opentelemetry_otlp::{WithExportConfig, WithHttpConfig, WithTonicConfig};
use opentelemetry_sdk::{
    Resource,
    error::OTelSdkResult,
    metrics,
    metrics::{
        Aggregation, Instrument, InstrumentKind, MeterProviderBuilder, PeriodicReader,
        SdkMeterProvider, Temporality, data::ResourceMetrics, exporter::PushMetricExporter,
    },
};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tonic::metadata::MetadataMap;
use tracing::{Dispatch, instrument::WithSubscriber};

const OTLP_METRIC_EXPORT_WARN_TARGET: &str = "temporalio_common::telemetry::otel::metric_export";

fn histo_view(
    metric_name: &'static str,
    use_seconds: bool,
) -> impl Fn(&Instrument) -> Option<metrics::Stream> + Send + Sync + 'static {
    let buckets = default_buckets_for(metric_name, use_seconds).to_vec();
    move |ins: &Instrument| {
        if ins.name().ends_with(metric_name) {
            Some(
                metrics::Stream::builder()
                    .with_aggregation(Aggregation::ExplicitBucketHistogram {
                        boundaries: buckets.clone(),
                        record_min_max: true,
                    })
                    .build()
                    .expect("Hardcoded metric stream always builds"),
            )
        } else {
            None
        }
    }
}

pub(super) fn augment_meter_provider_with_defaults(
    mut mpb: MeterProviderBuilder,
    global_tags: &HashMap<String, String>,
    use_seconds: bool,
    bucket_overrides: HistogramBucketOverrides,
) -> Result<MeterProviderBuilder, anyhow::Error> {
    for (name, buckets) in bucket_overrides.overrides {
        mpb = mpb.with_view(move |ins: &Instrument| {
            if ins.name().contains(&name) {
                Some(
                    metrics::Stream::builder()
                        .with_aggregation(Aggregation::ExplicitBucketHistogram {
                            boundaries: buckets.clone(),
                            record_min_max: true,
                        })
                        .build()
                        .expect("Hardcoded metric stream always builds"),
                )
            } else {
                None
            }
        });
    }
    let mut mpb = mpb
        .with_view(histo_view(WORKFLOW_E2E_LATENCY_HISTOGRAM_NAME, use_seconds))
        .with_view(histo_view(
            WORKFLOW_TASK_EXECUTION_LATENCY_HISTOGRAM_NAME,
            use_seconds,
        ))
        .with_view(histo_view(
            WORKFLOW_TASK_REPLAY_LATENCY_HISTOGRAM_NAME,
            use_seconds,
        ))
        .with_view(histo_view(
            WORKFLOW_TASK_SCHED_TO_START_LATENCY_HISTOGRAM_NAME,
            use_seconds,
        ))
        .with_view(histo_view(
            ACTIVITY_SCHED_TO_START_LATENCY_HISTOGRAM_NAME,
            use_seconds,
        ))
        .with_view(histo_view(
            ACTIVITY_EXEC_LATENCY_HISTOGRAM_NAME,
            use_seconds,
        ));
    // Fallback default
    mpb = mpb.with_view(move |ins: &Instrument| {
        if ins.kind() == InstrumentKind::Histogram {
            Some(
                metrics::Stream::builder()
                    .with_aggregation(Aggregation::ExplicitBucketHistogram {
                        boundaries: if use_seconds {
                            DEFAULT_S_BUCKETS.to_vec()
                        } else {
                            DEFAULT_MS_BUCKETS.to_vec()
                        },
                        record_min_max: true,
                    })
                    .build()
                    .expect("Hardcoded metric stream always builds"),
            )
        } else {
            None
        }
    });
    Ok(mpb.with_resource(default_resource(global_tags)))
}

/// Create an OTel meter that can be used as a [CoreMeter] to export metrics over OTLP.
pub fn build_otlp_metric_exporter(
    opts: OtelCollectorOptions,
) -> Result<CoreOtelMeter, anyhow::Error> {
    // The HTTP exporter's reqwest client resolves its rustls provider from the process default.
    super::ensure_default_crypto_provider();
    let exporter = match opts.protocol {
        OtlpProtocol::Grpc => {
            let exporter = opentelemetry_otlp::MetricExporter::builder()
                .with_tonic()
                .with_endpoint(opts.url.to_string());
            #[cfg(any(feature = "tls-ring", feature = "tls-aws-lc"))]
            let exporter = if opts.url.scheme() == "https" || opts.url.scheme() == "grpcs" {
                exporter.with_tls_config(ClientTlsConfig::new().with_native_roots())
            } else {
                exporter
            };
            exporter
                .with_metadata(MetadataMap::from_headers((&opts.headers).try_into()?))
                .with_temporality(metric_temporality_to_temporality(opts.metric_temporality))
                .build()?
        }
        OtlpProtocol::Http => opentelemetry_otlp::MetricExporter::builder()
            .with_http()
            .with_endpoint(opts.url.to_string())
            .with_headers(opts.headers)
            .with_temporality(metric_temporality_to_temporality(opts.metric_temporality))
            .build()?,
    };
    let reader = PeriodicReader::builder(TracingMetricExporter::new(exporter))
        .with_interval(opts.metric_periodicity)
        .build();
    let mp = augment_meter_provider_with_defaults(
        MeterProviderBuilder::default().with_reader(reader),
        &opts.global_tags,
        opts.use_seconds_for_durations,
        opts.histogram_bucket_overrides,
    )?
    .build();
    Ok::<_, anyhow::Error>(CoreOtelMeter {
        meter: mp.meter(TELEM_SERVICE_NAME),
        use_seconds_for_durations: opts.use_seconds_for_durations,
        _mp: mp,
    })
}

struct TracingMetricExporter<E> {
    inner: E,
    dispatch: Dispatch,
}

impl<E> TracingMetricExporter<E> {
    fn new(inner: E) -> Self {
        let dispatch = tracing::dispatcher::get_default(Clone::clone);
        Self { inner, dispatch }
    }
}

impl<E: PushMetricExporter> PushMetricExporter for TracingMetricExporter<E> {
    fn export(&self, metrics: &ResourceMetrics) -> impl Future<Output = OTelSdkResult> + Send {
        let dispatch = self.dispatch.clone();
        let export = tracing::dispatcher::with_default(&dispatch, || self.inner.export(metrics));
        async move {
            let result = export.await;
            if let Err(err) = &result {
                tracing::warn!(
                    target: OTLP_METRIC_EXPORT_WARN_TARGET,
                    error = %err,
                    "OTLP metric export failed; metrics may be dropped"
                );
            }
            result
        }
        .with_subscriber(dispatch)
    }

    fn force_flush(&self) -> OTelSdkResult {
        tracing::dispatcher::with_default(&self.dispatch, || self.inner.force_flush())
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        tracing::dispatcher::with_default(&self.dispatch, || {
            self.inner.shutdown_with_timeout(timeout)
        })
    }

    fn temporality(&self) -> Temporality {
        tracing::dispatcher::with_default(&self.dispatch, || self.inner.temporality())
    }
}

#[derive(Debug)]
pub struct CoreOtelMeter {
    pub meter: Meter,
    use_seconds_for_durations: bool,
    // we have to hold on to the provider otherwise otel automatically shuts it down on drop
    // for whatever crazy reason
    _mp: SdkMeterProvider,
}

impl CoreMeter for CoreOtelMeter {
    fn new_attributes(&self, attribs: NewAttributes) -> MetricAttributes {
        MetricAttributes::OTel {
            kvs: Arc::new(attribs.attributes.into_iter().map(KeyValue::from).collect()),
        }
    }

    fn extend_attributes(
        &self,
        existing: MetricAttributes,
        attribs: NewAttributes,
    ) -> MetricAttributes {
        if let MetricAttributes::OTel { mut kvs } = existing {
            Arc::make_mut(&mut kvs).extend(attribs.attributes.into_iter().map(Into::into));
            MetricAttributes::OTel { kvs }
        } else {
            dbg_panic!("Must use OTel attributes with an OTel metric implementation");
            existing
        }
    }

    fn counter(&self, params: MetricParameters) -> Counter {
        Counter::new(Arc::new(
            self.meter
                .u64_counter(params.name)
                .with_unit(params.unit)
                .with_description(params.description)
                .build(),
        ))
    }

    fn histogram(&self, params: MetricParameters) -> Histogram {
        Histogram::new(Arc::new(self.create_histogram(params)))
    }

    fn histogram_f64(&self, params: MetricParameters) -> HistogramF64 {
        HistogramF64::new(Arc::new(self.create_histogram_f64(params)))
    }

    fn histogram_duration(&self, mut params: MetricParameters) -> HistogramDuration {
        HistogramDuration::new(Arc::new(if self.use_seconds_for_durations {
            params.unit = "s".into();
            DurationHistogram::Seconds(self.create_histogram_f64(params))
        } else {
            params.unit = "ms".into();
            DurationHistogram::Milliseconds(self.create_histogram(params))
        }))
    }

    fn gauge(&self, params: MetricParameters) -> Gauge {
        Gauge::new(Arc::new(
            self.meter
                .u64_gauge(params.name)
                .with_unit(params.unit)
                .with_description(params.description)
                .build(),
        ))
    }

    fn gauge_f64(&self, params: MetricParameters) -> GaugeF64 {
        GaugeF64::new(Arc::new(
            self.meter
                .f64_gauge(params.name)
                .with_unit(params.unit)
                .with_description(params.description)
                .build(),
        ))
    }

    fn up_down_counter(&self, params: MetricParameters) -> UpDownCounter {
        UpDownCounter::new(Arc::new(
            self.meter
                .i64_up_down_counter(params.name)
                .with_unit(params.unit)
                .with_description(params.description)
                .build(),
        ))
    }
}

impl CoreOtelMeter {
    fn create_histogram(&self, params: MetricParameters) -> opentelemetry::metrics::Histogram<u64> {
        self.meter
            .u64_histogram(params.name)
            .with_unit(params.unit)
            .with_description(params.description)
            .build()
    }

    fn create_histogram_f64(
        &self,
        params: MetricParameters,
    ) -> opentelemetry::metrics::Histogram<f64> {
        self.meter
            .f64_histogram(params.name)
            .with_unit(params.unit)
            .with_description(params.description)
            .build()
    }
}

enum DurationHistogram {
    Milliseconds(opentelemetry::metrics::Histogram<u64>),
    Seconds(opentelemetry::metrics::Histogram<f64>),
}

enum DurationHistogramBase {
    Millis(Box<dyn HistogramBase>),
    Secs(Box<dyn HistogramF64Base>),
}

impl HistogramDurationBase for DurationHistogramBase {
    fn records(&self, value: Duration) {
        match self {
            DurationHistogramBase::Millis(h) => h.records(value.as_millis() as u64),
            DurationHistogramBase::Secs(h) => h.records(value.as_secs_f64()),
        }
    }
}
impl MetricAttributable<Box<dyn HistogramDurationBase>> for DurationHistogram {
    fn with_attributes(
        &self,
        attributes: &MetricAttributes,
    ) -> Result<Box<dyn HistogramDurationBase>, Box<dyn std::error::Error>> {
        Ok(match self {
            DurationHistogram::Milliseconds(h) => Box::new(DurationHistogramBase::Millis(
                h.with_attributes(attributes)?,
            )),
            DurationHistogram::Seconds(h) => {
                Box::new(DurationHistogramBase::Secs(h.with_attributes(attributes)?))
            }
        })
    }
}

fn default_resource_instance() -> &'static Resource {
    use std::sync::OnceLock;

    static INSTANCE: OnceLock<Resource> = OnceLock::new();
    INSTANCE.get_or_init(|| {
        let resource = Resource::builder().build();
        if resource.get(&Key::from("service.name")).is_some_and(|v| {
            let service_name = v.as_str();
            // OTel 0.32 may suffix the unknown-service fallback with the process name if available
            service_name == "unknown_service" || service_name.starts_with("unknown_service:")
        }) {
            // otel spec recommends to leave service.name as unknown_service but we want to
            // maintain backwards compatability with existing library behaviour
            return Resource::builder_empty()
                .with_attributes(
                    resource
                        .iter()
                        .map(|(k, v)| KeyValue::new(k.clone(), v.clone())),
                )
                .with_attribute(KeyValue::new("service.name", TELEM_SERVICE_NAME))
                .build();
        }
        resource
    })
}

fn default_resource(override_values: &HashMap<String, String>) -> Resource {
    Resource::builder_empty()
        .with_attributes(
            default_resource_instance()
                .iter()
                .map(|(k, v)| KeyValue::new(k.clone(), v.clone())),
        )
        .with_attributes(
            override_values
                .iter()
                .map(|(k, v)| KeyValue::new(k.clone(), v.clone())),
        )
        .build()
}

fn metric_temporality_to_temporality(t: MetricTemporality) -> Temporality {
    match t {
        MetricTemporality::Cumulative => Temporality::Cumulative,
        MetricTemporality::Delta => Temporality::Delta,
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use opentelemetry::{Key, Value};
    use opentelemetry_sdk::{
        error::{OTelSdkError, OTelSdkResult},
        metrics::{Temporality, data::ResourceMetrics, exporter::PushMetricExporter},
    };
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };
    use tracing_core::{
        Event, Level, Metadata, Subscriber,
        span::{Attributes, Id, Record},
    };

    const EXPORT_LOG_TARGET: &str = "temporalio_common_test_exporter";

    struct CapturingSubscriber {
        saw_export_log: Arc<AtomicBool>,
        saw_wrapper_warn: Arc<AtomicBool>,
    }

    impl Subscriber for CapturingSubscriber {
        fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, _span: &Attributes<'_>) -> Id {
            Id::from_u64(1)
        }

        fn record(&self, _span: &Id, _values: &Record<'_>) {}

        fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

        fn event(&self, event: &Event<'_>) {
            if event.metadata().target() == EXPORT_LOG_TARGET {
                self.saw_export_log.store(true, Ordering::SeqCst);
            }
            if *event.metadata().level() == Level::WARN
                && event.metadata().target() == OTLP_METRIC_EXPORT_WARN_TARGET
            {
                self.saw_wrapper_warn.store(true, Ordering::SeqCst);
            }
        }

        fn enter(&self, _span: &Id) {}

        fn exit(&self, _span: &Id) {}
    }

    struct FailingExporter;

    impl PushMetricExporter for FailingExporter {
        async fn export(&self, _metrics: &ResourceMetrics) -> OTelSdkResult {
            tracing::debug!(target: EXPORT_LOG_TARGET, "export polled");
            Err(OTelSdkError::InternalFailure("export failed".to_string()))
        }

        fn force_flush(&self) -> OTelSdkResult {
            Ok(())
        }

        fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
            Ok(())
        }

        fn temporality(&self) -> Temporality {
            Temporality::Cumulative
        }
    }

    #[test]
    pub(crate) fn default_resource_instance_service_name_default() {
        let resource = default_resource_instance();
        let service_name = resource.get(&Key::from("service.name"));
        assert_eq!(service_name, Some(Value::from(TELEM_SERVICE_NAME)));
    }

    #[tokio::test]
    pub(crate) async fn traced_exporter_enters_subscriber_while_polling_export() {
        let saw_export_log = Arc::new(AtomicBool::new(false));
        let saw_wrapper_warn = Arc::new(AtomicBool::new(false));
        let subscriber = Arc::new(CapturingSubscriber {
            saw_export_log: saw_export_log.clone(),
            saw_wrapper_warn,
        });
        let exporter = tracing::dispatcher::with_default(&Dispatch::new(subscriber), || {
            TracingMetricExporter::new(FailingExporter)
        });
        let metrics = ResourceMetrics::default();

        assert!(exporter.export(&metrics).await.is_err());
        assert!(saw_export_log.load(Ordering::SeqCst));
    }

    #[test]
    pub(crate) fn periodic_reader_export_failure_emits_core_warn() {
        let saw_export_log = Arc::new(AtomicBool::new(false));
        let saw_wrapper_warn = Arc::new(AtomicBool::new(false));
        let subscriber = Arc::new(CapturingSubscriber {
            saw_export_log,
            saw_wrapper_warn: saw_wrapper_warn.clone(),
        });
        let provider = tracing::dispatcher::with_default(&Dispatch::new(subscriber), || {
            let reader = PeriodicReader::builder(TracingMetricExporter::new(FailingExporter))
                .with_interval(Duration::from_secs(60))
                .build();
            MeterProviderBuilder::default().with_reader(reader).build()
        });
        let meter = provider.meter("temporalio_common_test");
        meter
            .u64_counter("temporalio_common_test_counter")
            .build()
            .add(1, &[]);

        assert!(provider.force_flush().is_err());
        assert!(saw_wrapper_warn.load(Ordering::SeqCst));
    }
}
