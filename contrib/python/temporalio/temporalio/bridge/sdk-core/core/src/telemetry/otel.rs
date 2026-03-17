use super::{
    TELEM_SERVICE_NAME, default_buckets_for,
    metrics::{
        ACTIVITY_EXEC_LATENCY_HISTOGRAM_NAME, ACTIVITY_SCHED_TO_START_LATENCY_HISTOGRAM_NAME,
        DEFAULT_MS_BUCKETS, WORKFLOW_E2E_LATENCY_HISTOGRAM_NAME,
        WORKFLOW_TASK_EXECUTION_LATENCY_HISTOGRAM_NAME,
        WORKFLOW_TASK_REPLAY_LATENCY_HISTOGRAM_NAME,
        WORKFLOW_TASK_SCHED_TO_START_LATENCY_HISTOGRAM_NAME,
    },
};
use crate::{abstractions::dbg_panic, telemetry::metrics::DEFAULT_S_BUCKETS};
use opentelemetry::{
    self, Key, KeyValue, Value,
    metrics::{Meter, MeterProvider as MeterProviderT},
};
use opentelemetry_otlp::{WithExportConfig, WithHttpConfig, WithTonicConfig};
use opentelemetry_sdk::{
    Resource, metrics,
    metrics::{
        Aggregation, Instrument, InstrumentKind, MeterProviderBuilder, PeriodicReader,
        SdkMeterProvider, Temporality,
    },
};
use std::{collections::HashMap, sync::Arc, time::Duration};
use temporal_sdk_core_api::telemetry::{
    HistogramBucketOverrides, MetricTemporality, OtelCollectorOptions, OtlpProtocol,
    metrics::{
        CoreMeter, Counter, Gauge, GaugeF64, Histogram, HistogramBase, HistogramDuration,
        HistogramDurationBase, HistogramF64, HistogramF64Base, MetricAttributable,
        MetricAttributes, MetricParameters, NewAttributes,
    },
};
use tonic::{metadata::MetadataMap, transport::ClientTlsConfig};

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
    let exporter = match opts.protocol {
        OtlpProtocol::Grpc => {
            let mut exporter = opentelemetry_otlp::MetricExporter::builder()
                .with_tonic()
                .with_endpoint(opts.url.to_string());
            if opts.url.scheme() == "https" || opts.url.scheme() == "grpcs" {
                exporter = exporter.with_tls_config(ClientTlsConfig::new().with_native_roots());
            }
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
    let reader = PeriodicReader::builder(exporter)
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
        if resource.get(&Key::from("service.name")) == Some(Value::from("unknown_service")) {
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
    use opentelemetry::Key;

    #[test]
    pub(crate) fn default_resource_instance_service_name_default() {
        let resource = default_resource_instance();
        let service_name = resource.get(&Key::from("service.name"));
        assert_eq!(service_name, Some(Value::from(TELEM_SERVICE_NAME)));
    }
}
