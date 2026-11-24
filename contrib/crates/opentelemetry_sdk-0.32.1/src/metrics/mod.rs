//! The crust of the OpenTelemetry metrics SDK.
//!
//! ## Configuration
//!
//! The metrics SDK configuration is stored with each [SdkMeterProvider].
//! Configuration for [Resource]s, views, and `ManualReader` or
//! [PeriodicReader] instances can be specified.
//!
//! ### Example
//!
//! ```
//! use opentelemetry::global;
//! use opentelemetry::KeyValue;
//! use opentelemetry_sdk::{metrics::SdkMeterProvider, Resource};
//!
//! // Generate SDK configuration, resource, views, etc
//! let resource = Resource::builder().build(); // default attributes about the current process
//!
//! // Create a meter provider with the desired config
//! let meter_provider = SdkMeterProvider::builder().with_resource(resource).build();
//! global::set_meter_provider(meter_provider.clone());
//!
//! // Use the meter provider to create meter instances
//! let meter = global::meter("my_app");
//!
//! // Create instruments scoped to the meter
//! let counter = meter
//!     .u64_counter("power_consumption")
//!     .with_unit("kWh")
//!     .build();
//!
//! // use instruments to record measurements
//! counter.add(10, &[KeyValue::new("rate", "standard")]);
//!
//! // shutdown the provider at the end of the application to ensure any metrics not yet
//! // exported are flushed.
//! meter_provider.shutdown().unwrap();
//! ```
//!
//! [Resource]: crate::Resource

#[allow(unreachable_pub)]
#[allow(unused)]
pub(crate) mod aggregation;
pub mod data;
mod error;
pub mod exporter;
pub(crate) mod instrument;
pub(crate) mod internal;
#[cfg(feature = "experimental_metrics_custom_reader")]
pub(crate) mod manual_reader;
pub(crate) mod meter;
mod meter_provider;
pub(crate) mod noop;
pub(crate) mod periodic_reader;
#[cfg(feature = "experimental_metrics_periodicreader_with_async_runtime")]
/// Module for periodic reader with async runtime.
pub mod periodic_reader_with_async_runtime;
pub(crate) mod pipeline;
#[cfg(feature = "experimental_metrics_custom_reader")]
pub mod reader;
#[cfg(not(feature = "experimental_metrics_custom_reader"))]
pub(crate) mod reader;
pub(crate) mod view;

/// In-Memory metric exporter for testing purpose.
#[cfg(any(feature = "testing", test))]
#[cfg_attr(docsrs, doc(cfg(any(feature = "testing", test))))]
pub mod in_memory_exporter;
#[cfg(any(feature = "testing", test))]
#[cfg_attr(docsrs, doc(cfg(any(feature = "testing", test))))]
pub use in_memory_exporter::{InMemoryMetricExporter, InMemoryMetricExporterBuilder};

pub use aggregation::*;
#[cfg(feature = "experimental_metrics_custom_reader")]
pub use manual_reader::*;
pub use meter_provider::*;
pub use periodic_reader::*;
#[cfg(feature = "experimental_metrics_custom_reader")]
pub use pipeline::Pipeline;

pub use instrument::{Instrument, InstrumentKind, Stream, StreamBuilder};

use std::hash::Hash;
use std::str::FromStr;

/// Defines the window that an aggregation was calculated over.
#[derive(Debug, Copy, Clone, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Temporality {
    /// A measurement interval that continues to expand forward in time from a
    /// starting point.
    ///
    /// New measurements are added to all previous measurements since a start time.
    #[default]
    Cumulative,

    /// A measurement interval that resets each cycle.
    ///
    /// Measurements from one cycle are recorded independently, measurements from
    /// other cycles do not affect them.
    Delta,

    /// Configures Synchronous Counter and Histogram instruments to use
    /// Delta aggregation temporality, which allows them to shed memory
    /// following a cardinality explosion, thus use less memory.
    LowMemory,
}

impl FromStr for Temporality {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "cumulative" => Ok(Temporality::Cumulative),
            "delta" => Ok(Temporality::Delta),
            "lowmemory" => Ok(Temporality::LowMemory),
            _ => Err(()),
        }
    }
}

#[cfg(all(test, feature = "testing"))]
mod tests {
    #[cfg(feature = "experimental_metrics_bound_instruments")]
    use self::data::ExponentialHistogramDataPoint;
    use self::data::{HistogramDataPoint, MetricData, ScopeMetrics, SumDataPoint};
    use super::internal::Number;
    use super::*;
    use crate::metrics::data::ResourceMetrics;
    use crate::metrics::internal::AggregatedMetricsAccess;
    use crate::metrics::InMemoryMetricExporter;
    use crate::metrics::InMemoryMetricExporterBuilder;
    use data::GaugeDataPoint;
    use opentelemetry::metrics::{Counter, Meter, UpDownCounter};
    use opentelemetry::InstrumentationScope;
    use opentelemetry::Value;
    use opentelemetry::{metrics::MeterProvider as _, KeyValue};
    use rand::{rngs, Rng, SeedableRng};
    use std::cmp::{max, min};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Duration;

    // Run all tests in this mod
    // cargo test metrics::tests --features=testing,spec_unstable_metrics_views
    // Note for all tests from this point onwards in this mod:
    // "multi_thread" tokio flavor must be used else flush won't
    // be able to make progress!

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[cfg(not(feature = "experimental_metrics_disable_name_validation"))]
    async fn invalid_instrument_config_noops() {
        // Run this test with stdout enabled to see output.
        // cargo test invalid_instrument_config_noops --features=testing,spec_unstable_metrics_views -- --nocapture
        let invalid_instrument_names = vec![
            "_startWithNoneAlphabet",
            "utf8char锈",
            "a".repeat(256).leak(),
            "invalid name",
        ];
        for name in invalid_instrument_names {
            let test_context = TestContext::new(Temporality::Cumulative);
            let counter = test_context.meter().u64_counter(name).build();
            counter.add(1, &[]);

            let up_down_counter = test_context.meter().i64_up_down_counter(name).build();
            up_down_counter.add(1, &[]);

            let gauge = test_context.meter().f64_gauge(name).build();
            gauge.record(1.9, &[]);

            let histogram = test_context.meter().f64_histogram(name).build();
            histogram.record(1.0, &[]);

            let _observable_counter = test_context
                .meter()
                .u64_observable_counter(name)
                .with_callback(move |observer| {
                    observer.observe(1, &[]);
                })
                .build();

            let _observable_gauge = test_context
                .meter()
                .f64_observable_gauge(name)
                .with_callback(move |observer| {
                    observer.observe(1.0, &[]);
                })
                .build();

            let _observable_up_down_counter = test_context
                .meter()
                .i64_observable_up_down_counter(name)
                .with_callback(move |observer| {
                    observer.observe(1, &[]);
                })
                .build();

            test_context.flush_metrics();

            // As instrument name is invalid, no metrics should be exported
            test_context.check_no_metrics();
        }

        let invalid_bucket_boundaries = vec![
            vec![1.0, 1.0],                          // duplicate boundaries
            vec![1.0, 2.0, 3.0, 2.0],                // duplicate non consequent boundaries
            vec![1.0, 2.0, 3.0, 4.0, 2.5],           // unsorted boundaries
            vec![1.0, 2.0, 3.0, f64::INFINITY, 4.0], // boundaries with positive infinity
            vec![1.0, 2.0, 3.0, f64::NAN],           // boundaries with NaNs
            vec![f64::NEG_INFINITY, 2.0, 3.0],       // boundaries with negative infinity
        ];
        for bucket_boundaries in invalid_bucket_boundaries {
            let test_context = TestContext::new(Temporality::Cumulative);
            let histogram = test_context
                .meter()
                .f64_histogram("test")
                .with_boundaries(bucket_boundaries)
                .build();
            histogram.record(1.9, &[]);
            test_context.flush_metrics();

            // As bucket boundaries provided via advisory params are invalid,
            // no metrics should be exported
            test_context.check_no_metrics();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[cfg(feature = "experimental_metrics_disable_name_validation")]
    async fn valid_instrument_config_with_feature_experimental_metrics_disable_name_validation() {
        // Run this test with stdout enabled to see output.
        // cargo test valid_instrument_config_with_feature_experimental_metrics_disable_name_validation --all-features -- --nocapture
        let invalid_instrument_names = vec![
            "_startWithNoneAlphabet",
            "utf8char锈",
            "",
            "a".repeat(256).leak(),
            "\\allow\\slash /sec",
            "\\allow\\$$slash /sec",
            "Total $ Count",
            "\\test\\UsagePercent(Total) > 80%",
            "invalid name",
        ];
        for name in invalid_instrument_names {
            let test_context = TestContext::new(Temporality::Cumulative);
            let counter = test_context.meter().u64_counter(name).build();
            counter.add(1, &[]);

            let up_down_counter = test_context.meter().i64_up_down_counter(name).build();
            up_down_counter.add(1, &[]);

            let gauge = test_context.meter().f64_gauge(name).build();
            gauge.record(1.9, &[]);

            let histogram = test_context.meter().f64_histogram(name).build();
            histogram.record(1.0, &[]);

            let _observable_counter = test_context
                .meter()
                .u64_observable_counter(name)
                .with_callback(move |observer| {
                    observer.observe(1, &[]);
                })
                .build();

            let _observable_gauge = test_context
                .meter()
                .f64_observable_gauge(name)
                .with_callback(move |observer| {
                    observer.observe(1.0, &[]);
                })
                .build();

            let _observable_up_down_counter = test_context
                .meter()
                .i64_observable_up_down_counter(name)
                .with_callback(move |observer| {
                    observer.observe(1, &[]);
                })
                .build();

            test_context.flush_metrics();

            // As instrument name are valid because of the feature flag, metrics should be exported
            let resource_metrics = test_context
                .exporter
                .get_finished_metrics()
                .expect("metrics expected to be exported");

            assert!(!resource_metrics.is_empty(), "metrics should be exported");
        }

        // Ensuring that the Histograms with invalid bucket boundaries are not exported
        // when using the feature flag
        let invalid_bucket_boundaries = vec![
            vec![1.0, 1.0],                          // duplicate boundaries
            vec![1.0, 2.0, 3.0, 2.0],                // duplicate non consequent boundaries
            vec![1.0, 2.0, 3.0, 4.0, 2.5],           // unsorted boundaries
            vec![1.0, 2.0, 3.0, f64::INFINITY, 4.0], // boundaries with positive infinity
            vec![1.0, 2.0, 3.0, f64::NAN],           // boundaries with NaNs
            vec![f64::NEG_INFINITY, 2.0, 3.0],       // boundaries with negative infinity
        ];
        for bucket_boundaries in invalid_bucket_boundaries {
            let test_context = TestContext::new(Temporality::Cumulative);
            let histogram = test_context
                .meter()
                .f64_histogram("test")
                .with_boundaries(bucket_boundaries)
                .build();
            histogram.record(1.9, &[]);
            test_context.flush_metrics();

            // As bucket boundaries provided via advisory params are invalid,
            // no metrics should be exported
            test_context.check_no_metrics();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn counter_aggregation_delta() {
        // Run this test with stdout enabled to see output.
        // cargo test counter_aggregation_delta --features=testing -- --nocapture
        counter_aggregation_helper(Temporality::Delta);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn counter_aggregation_cumulative() {
        // Run this test with stdout enabled to see output.
        // cargo test counter_aggregation_cumulative --features=testing -- --nocapture
        counter_aggregation_helper(Temporality::Cumulative);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn counter_aggregation_no_attributes_cumulative() {
        let mut test_context = TestContext::new(Temporality::Cumulative);
        let counter = test_context.u64_counter("test", "my_counter", None);

        counter.add(50, &[]);
        test_context.flush_metrics();

        let MetricData::Sum(sum) = test_context.get_aggregation::<u64>("my_counter", None) else {
            unreachable!()
        };

        assert_eq!(sum.data_points.len(), 1, "Expected only one data point");
        assert!(sum.is_monotonic, "Should produce monotonic.");
        assert_eq!(
            sum.temporality,
            Temporality::Cumulative,
            "Should produce cumulative"
        );

        let data_point = &sum.data_points[0];
        assert!(data_point.attributes.is_empty(), "Non-empty attribute set");
        assert_eq!(data_point.value, 50, "Unexpected data point value");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn counter_aggregation_no_attributes_delta() {
        let mut test_context = TestContext::new(Temporality::Delta);
        let counter = test_context.u64_counter("test", "my_counter", None);

        counter.add(50, &[]);
        test_context.flush_metrics();

        let MetricData::Sum(sum) = test_context.get_aggregation::<u64>("my_counter", None) else {
            unreachable!()
        };

        assert_eq!(sum.data_points.len(), 1, "Expected only one data point");
        assert!(sum.is_monotonic, "Should produce monotonic.");
        assert_eq!(sum.temporality, Temporality::Delta, "Should produce delta");

        let data_point = &sum.data_points[0];
        assert!(data_point.attributes.is_empty(), "Non-empty attribute set");
        assert_eq!(data_point.value, 50, "Unexpected data point value");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn counter_aggregation_overflow_delta() {
        counter_aggregation_overflow_helper(Temporality::Delta);
        counter_aggregation_overflow_helper_custom_limit(Temporality::Delta);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn counter_aggregation_overflow_cumulative() {
        counter_aggregation_overflow_helper(Temporality::Cumulative);
        counter_aggregation_overflow_helper_custom_limit(Temporality::Cumulative);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn counter_aggregation_attribute_order_sorted_first_delta() {
        // Run this test with stdout enabled to see output.
        // cargo test counter_aggregation_attribute_order_sorted_first_delta --features=testing -- --nocapture
        counter_aggregation_attribute_order_helper(Temporality::Delta, true);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn counter_aggregation_attribute_order_sorted_first_cumulative() {
        // Run this test with stdout enabled to see output.
        // cargo test counter_aggregation_attribute_order_sorted_first_cumulative --features=testing -- --nocapture
        counter_aggregation_attribute_order_helper(Temporality::Cumulative, true);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn counter_aggregation_attribute_order_unsorted_first_delta() {
        // Run this test with stdout enabled to see output.
        // cargo test counter_aggregation_attribute_order_unsorted_first_delta --features=testing -- --nocapture

        counter_aggregation_attribute_order_helper(Temporality::Delta, false);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn counter_aggregation_attribute_order_unsorted_first_cumulative() {
        // Run this test with stdout enabled to see output.
        // cargo test counter_aggregation_attribute_order_unsorted_first_cumulative --features=testing -- --nocapture

        counter_aggregation_attribute_order_helper(Temporality::Cumulative, false);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn histogram_aggregation_cumulative() {
        // Run this test with stdout enabled to see output.
        // cargo test histogram_aggregation_cumulative --features=testing -- --nocapture
        histogram_aggregation_helper(Temporality::Cumulative);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn histogram_aggregation_delta() {
        // Run this test with stdout enabled to see output.
        // cargo test histogram_aggregation_delta --features=testing -- --nocapture
        histogram_aggregation_helper(Temporality::Delta);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn histogram_aggregation_with_custom_bounds() {
        // Run this test with stdout enabled to see output.
        // cargo test histogram_aggregation_with_custom_bounds --features=testing -- --nocapture
        histogram_aggregation_with_custom_bounds_helper(Temporality::Delta);
        histogram_aggregation_with_custom_bounds_helper(Temporality::Cumulative);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn histogram_aggregation_with_empty_bounds() {
        // Run this test with stdout enabled to see output.
        // cargo test histogram_aggregation_with_empty_bounds --features=testing -- --nocapture
        histogram_aggregation_with_empty_bounds_helper(Temporality::Delta);
        histogram_aggregation_with_empty_bounds_helper(Temporality::Cumulative);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn histogram_aggregation_with_custom_bounds_and_view() {
        // Run this test with stdout enabled to see output.
        // cargo test histogram_aggregation_with_custom_bounds_and_view --features=testing -- --nocapture
        histogram_aggregation_with_custom_bounds_and_view_helper(Temporality::Delta);
        histogram_aggregation_with_custom_bounds_and_view_helper(Temporality::Cumulative);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn exponential_histogram_aggregation_with_view() {
        // Run this test with stdout enabled to see output.
        // cargo test exponential_histogram_aggregation_with_view --features=testing -- --nocapture
        exponential_histogram_aggregation_with_view_helper(Temporality::Delta);
        exponential_histogram_aggregation_with_view_helper(Temporality::Cumulative);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn updown_counter_aggregation_cumulative() {
        // Run this test with stdout enabled to see output.
        // cargo test updown_counter_aggregation_cumulative --features=testing -- --nocapture
        updown_counter_aggregation_helper(Temporality::Cumulative);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn updown_counter_aggregation_delta() {
        // Run this test with stdout enabled to see output.
        // cargo test updown_counter_aggregation_delta --features=testing -- --nocapture
        updown_counter_aggregation_helper(Temporality::Delta);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn gauge_aggregation() {
        // Run this test with stdout enabled to see output.
        // cargo test gauge_aggregation --features=testing -- --nocapture

        // Gauge should use last value aggregation regardless of the aggregation temporality used.
        gauge_aggregation_helper(Temporality::Delta);
        gauge_aggregation_helper(Temporality::Cumulative);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn observable_gauge_aggregation() {
        // Run this test with stdout enabled to see output.
        // cargo test observable_gauge_aggregation --features=testing -- --nocapture

        // Gauge should use last value aggregation regardless of the aggregation temporality used.
        observable_gauge_aggregation_helper(Temporality::Delta, false);
        observable_gauge_aggregation_helper(Temporality::Delta, true);
        observable_gauge_aggregation_helper(Temporality::Cumulative, false);
        observable_gauge_aggregation_helper(Temporality::Cumulative, true);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn observable_counter_aggregation_cumulative_non_zero_increment() {
        // Run this test with stdout enabled to see output.
        // cargo test observable_counter_aggregation_cumulative_non_zero_increment --features=testing -- --nocapture
        observable_counter_aggregation_helper(Temporality::Cumulative, 100, 10, 4, false);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn observable_counter_aggregation_cumulative_non_zero_increment_no_attrs() {
        // Run this test with stdout enabled to see output.
        // cargo test observable_counter_aggregation_cumulative_non_zero_increment_no_attrs --features=testing -- --nocapture
        observable_counter_aggregation_helper(Temporality::Cumulative, 100, 10, 4, true);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn observable_counter_aggregation_delta_non_zero_increment() {
        // Run this test with stdout enabled to see output.
        // cargo test observable_counter_aggregation_delta_non_zero_increment --features=testing -- --nocapture
        observable_counter_aggregation_helper(Temporality::Delta, 100, 10, 4, false);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn observable_counter_aggregation_delta_non_zero_increment_no_attrs() {
        // Run this test with stdout enabled to see output.
        // cargo test observable_counter_aggregation_delta_non_zero_increment_no_attrs --features=testing -- --nocapture
        observable_counter_aggregation_helper(Temporality::Delta, 100, 10, 4, true);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn observable_counter_aggregation_cumulative_zero_increment() {
        // Run this test with stdout enabled to see output.
        // cargo test observable_counter_aggregation_cumulative_zero_increment --features=testing -- --nocapture
        observable_counter_aggregation_helper(Temporality::Cumulative, 100, 0, 4, false);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn observable_counter_aggregation_cumulative_zero_increment_no_attrs() {
        // Run this test with stdout enabled to see output.
        // cargo test observable_counter_aggregation_cumulative_zero_increment_no_attrs --features=testing -- --nocapture
        observable_counter_aggregation_helper(Temporality::Cumulative, 100, 0, 4, true);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn observable_counter_aggregation_delta_zero_increment() {
        // Run this test with stdout enabled to see output.
        // cargo test observable_counter_aggregation_delta_zero_increment --features=testing -- --nocapture
        observable_counter_aggregation_helper(Temporality::Delta, 100, 0, 4, false);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn observable_counter_aggregation_delta_zero_increment_no_attrs() {
        // Run this test with stdout enabled to see output.
        // cargo test observable_counter_aggregation_delta_zero_increment_no_attrs --features=testing -- --nocapture
        observable_counter_aggregation_helper(Temporality::Delta, 100, 0, 4, true);
    }

    fn observable_counter_aggregation_helper(
        temporality: Temporality,
        start: u64,
        increment: u64,
        length: u64,
        is_empty_attributes: bool,
    ) {
        // Arrange
        let mut test_context = TestContext::new(temporality);
        let attributes = if is_empty_attributes {
            vec![]
        } else {
            vec![KeyValue::new("key1", "value1")]
        };
        // The Observable counter reports values[0], values[1],....values[n] on each flush.
        let values: Vec<u64> = (0..length).map(|i| start + i * increment).collect();
        println!("Testing with observable values: {values:?}");
        let values = Arc::new(values);
        let values_clone = values.clone();
        let i = Arc::new(Mutex::new(0));
        let _observable_counter = test_context
            .meter()
            .u64_observable_counter("my_observable_counter")
            .with_unit("my_unit")
            .with_callback(move |observer| {
                let mut index = i.lock().unwrap();
                if *index < values.len() {
                    observer.observe(values[*index], &attributes);
                    *index += 1;
                }
            })
            .build();

        for (iter, v) in values_clone.iter().enumerate() {
            test_context.flush_metrics();
            let MetricData::Sum(sum) =
                test_context.get_aggregation::<u64>("my_observable_counter", None)
            else {
                unreachable!()
            };
            assert_eq!(sum.data_points.len(), 1);
            assert!(sum.is_monotonic, "Counter should produce monotonic.");
            if let Temporality::Cumulative = temporality {
                assert_eq!(
                    sum.temporality,
                    Temporality::Cumulative,
                    "Should produce cumulative"
                );
            } else {
                assert_eq!(sum.temporality, Temporality::Delta, "Should produce delta");
            }

            // find and validate datapoint
            let data_point = if is_empty_attributes {
                &sum.data_points[0]
            } else {
                find_sum_datapoint_with_key_value(&sum.data_points, "key1", "value1")
                    .expect("datapoint with key1=value1 expected")
            };

            if let Temporality::Cumulative = temporality {
                // Cumulative counter should have the value as is.
                assert_eq!(data_point.value, *v);
            } else {
                // Delta counter should have the increment value.
                // Except for the first value which should be the start value.
                if iter == 0 {
                    assert_eq!(data_point.value, start);
                } else {
                    assert_eq!(data_point.value, increment);
                }
            }

            test_context.reset_metrics();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn observable_counter_delta_attribute_set_reappears_after_gap() {
        // Run this test with stdout enabled to see output.
        // cargo test observable_counter_delta_attribute_set_reappears_after_gap --features=testing -- --nocapture

        // This test verifies the behavior when an attribute set is not reported
        // for one collection cycle and then reappears.
        // See: https://github.com/open-telemetry/opentelemetry-specification/issues/4861
        //
        // Scenario (Observable Counter with Delta temporality):
        // | Collection | Callback Reports  | Expected Delta Export  |
        // |------------|-------------------|------------------------|
        // | 1          | A=100, B=50       | A=100, B=50            |
        // | 2          | A=150 (B missing) | A=50 (B not exported)  |
        // | 3          | A=200, B=80       | A=50, B=80             |
        //
        // Current implementation: When B reappears, its delta is calculated from zero
        // (fresh start), not from the last known value. This is Option 1 from the spec issue.

        let mut test_context = TestContext::new(Temporality::Delta);

        // Shared state for callback: (collection_cycle, value_a, value_b_option)
        // value_b_option is None when B should not be reported
        let callback_state = Arc::new(Mutex::new((0u32, 0u64, Option::<u64>::None)));
        let callback_state_clone = callback_state.clone();

        let _observable_counter = test_context
            .meter()
            .u64_observable_counter("my_observable_counter")
            .with_callback(move |observer| {
                let state = callback_state_clone.lock().unwrap();
                let (_cycle, value_a, value_b_option) = *state;

                observer.observe(value_a, &[KeyValue::new("key", "A")]);
                if let Some(value_b) = value_b_option {
                    observer.observe(value_b, &[KeyValue::new("key", "B")]);
                }
            })
            .build();

        // Collection 1: A=100, B=50
        {
            *callback_state.lock().unwrap() = (1, 100, Some(50));
            test_context.flush_metrics();

            let MetricData::Sum(sum) =
                test_context.get_aggregation::<u64>("my_observable_counter", None)
            else {
                unreachable!()
            };

            assert_eq!(sum.data_points.len(), 2);
            assert_eq!(sum.temporality, Temporality::Delta);

            let dp_a = find_sum_datapoint_with_key_value(&sum.data_points, "key", "A")
                .expect("datapoint for A expected");
            let dp_b = find_sum_datapoint_with_key_value(&sum.data_points, "key", "B")
                .expect("datapoint for B expected");

            // First collection: delta = value - 0
            assert_eq!(
                dp_a.value, 100,
                "A's delta should be 100 (first collection)"
            );
            assert_eq!(dp_b.value, 50, "B's delta should be 50 (first collection)");

            test_context.reset_metrics();
        }

        // Collection 2: A=150, B missing
        {
            *callback_state.lock().unwrap() = (2, 150, None);
            test_context.flush_metrics();

            let MetricData::Sum(sum) =
                test_context.get_aggregation::<u64>("my_observable_counter", None)
            else {
                unreachable!()
            };

            // Only A should be exported, B is not observed so not exported (per spec)
            assert_eq!(
                sum.data_points.len(),
                1,
                "Only A should be exported when B is not observed"
            );

            let dp_a = find_sum_datapoint_with_key_value(&sum.data_points, "key", "A")
                .expect("datapoint for A expected");
            assert_eq!(dp_a.value, 50, "A's delta should be 50 (150 - 100)");

            test_context.reset_metrics();
        }

        // Collection 3: A=200, B=80 (B reappears)
        {
            *callback_state.lock().unwrap() = (3, 200, Some(80));
            test_context.flush_metrics();

            let MetricData::Sum(sum) =
                test_context.get_aggregation::<u64>("my_observable_counter", None)
            else {
                unreachable!()
            };

            assert_eq!(sum.data_points.len(), 2);

            let dp_a = find_sum_datapoint_with_key_value(&sum.data_points, "key", "A")
                .expect("datapoint for A expected");
            let dp_b = find_sum_datapoint_with_key_value(&sum.data_points, "key", "B")
                .expect("datapoint for B expected");

            assert_eq!(dp_a.value, 50, "A's delta should be 50 (200 - 150)");

            // B reappears after a gap. Current implementation uses "delta from zero" (Option 1).
            // This means B's delta = 80 - 0 = 80, not 80 - 50 = 30.
            // See: https://github.com/open-telemetry/opentelemetry-specification/issues/4861
            // TODO: Watch for spec clarification on this behavior.
            assert_eq!(
                dp_b.value, 80,
                "B's delta should be 80 (fresh start after gap, not 30 from last known value)"
            );

            test_context.reset_metrics();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn empty_meter_name_retained() {
        async fn meter_name_retained_helper(
            meter: Meter,
            provider: SdkMeterProvider,
            exporter: InMemoryMetricExporter,
        ) {
            // Act
            let counter = meter.u64_counter("my_counter").build();

            counter.add(10, &[]);
            provider.force_flush().unwrap();

            // Assert
            let resource_metrics = exporter
                .get_finished_metrics()
                .expect("metrics are expected to be exported.");
            assert!(
                resource_metrics[0].scope_metrics[0].metrics.len() == 1,
                "There should be a single metric"
            );
            let meter_name = resource_metrics[0].scope_metrics[0].scope.name();
            assert_eq!(meter_name, "");
        }

        let exporter = InMemoryMetricExporter::default();
        let meter_provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .build();

        // Test Meter creation in 2 ways, both with empty string as meter name
        let meter1 = meter_provider.meter("");
        meter_name_retained_helper(meter1, meter_provider.clone(), exporter.clone()).await;

        let meter_scope = InstrumentationScope::builder("").build();
        let meter2 = meter_provider.meter_with_scope(meter_scope);
        meter_name_retained_helper(meter2, meter_provider, exporter).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn counter_duplicate_instrument_merge() {
        // Arrange
        let exporter = InMemoryMetricExporter::default();
        let meter_provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .build();

        // Act
        let meter = meter_provider.meter("test");
        let counter = meter
            .u64_counter("my_counter")
            .with_unit("my_unit")
            .with_description("my_description")
            .build();

        let counter_duplicated = meter
            .u64_counter("my_counter")
            .with_unit("my_unit")
            .with_description("my_description")
            .build();

        let attribute = vec![KeyValue::new("key1", "value1")];
        counter.add(10, &attribute);
        counter_duplicated.add(5, &attribute);

        meter_provider.force_flush().unwrap();

        // Assert
        let resource_metrics = exporter
            .get_finished_metrics()
            .expect("metrics are expected to be exported.");
        assert!(
            resource_metrics[0].scope_metrics[0].metrics.len() == 1,
            "There should be single metric merging duplicate instruments"
        );
        let metric = &resource_metrics[0].scope_metrics[0].metrics[0];
        assert_eq!(metric.name, "my_counter");
        assert_eq!(metric.unit, "my_unit");
        let MetricData::Sum(sum) = u64::extract_metrics_data_ref(&metric.data)
            .expect("Sum aggregation expected for Counter instruments by default")
        else {
            unreachable!()
        };

        // Expecting 1 time-series.
        assert_eq!(sum.data_points.len(), 1);

        let datapoint = &sum.data_points[0];
        assert_eq!(datapoint.value, 15);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn counter_duplicate_instrument_different_meter_no_merge() {
        // Arrange
        let exporter = InMemoryMetricExporter::default();
        let meter_provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .build();

        // Act
        let meter1 = meter_provider.meter("test.meter1");
        let meter2 = meter_provider.meter("test.meter2");
        let counter1 = meter1
            .u64_counter("my_counter")
            .with_unit("my_unit")
            .with_description("my_description")
            .build();

        let counter2 = meter2
            .u64_counter("my_counter")
            .with_unit("my_unit")
            .with_description("my_description")
            .build();

        let attribute = vec![KeyValue::new("key1", "value1")];
        counter1.add(10, &attribute);
        counter2.add(5, &attribute);

        meter_provider.force_flush().unwrap();

        // Assert
        let resource_metrics = exporter
            .get_finished_metrics()
            .expect("metrics are expected to be exported.");
        assert!(
            resource_metrics[0].scope_metrics.len() == 2,
            "There should be 2 separate scope"
        );
        assert!(
            resource_metrics[0].scope_metrics[0].metrics.len() == 1,
            "There should be single metric for the scope"
        );
        assert!(
            resource_metrics[0].scope_metrics[1].metrics.len() == 1,
            "There should be single metric for the scope"
        );

        let scope1 = find_scope_metric(&resource_metrics[0].scope_metrics, "test.meter1");
        let scope2 = find_scope_metric(&resource_metrics[0].scope_metrics, "test.meter2");

        if let Some(scope1) = scope1 {
            let metric1 = &scope1.metrics[0];
            assert_eq!(metric1.name, "my_counter");
            assert_eq!(metric1.unit, "my_unit");
            assert_eq!(metric1.description, "my_description");
            let MetricData::Sum(sum1) = u64::extract_metrics_data_ref(&metric1.data)
                .expect("Sum aggregation expected for Counter instruments by default")
            else {
                unreachable!()
            };

            // Expecting 1 time-series.
            assert_eq!(sum1.data_points.len(), 1);

            let datapoint1 = &sum1.data_points[0];
            assert_eq!(datapoint1.value, 10);
        } else {
            panic!("No MetricScope found for 'test.meter1'");
        }

        if let Some(scope2) = scope2 {
            let metric2 = &scope2.metrics[0];
            assert_eq!(metric2.name, "my_counter");
            assert_eq!(metric2.unit, "my_unit");
            assert_eq!(metric2.description, "my_description");

            let MetricData::Sum(sum2) = u64::extract_metrics_data_ref(&metric2.data)
                .expect("Sum aggregation expected for Counter instruments by default")
            else {
                unreachable!()
            };

            // Expecting 1 time-series.
            assert_eq!(sum2.data_points.len(), 1);

            let datapoint2 = &sum2.data_points[0];
            assert_eq!(datapoint2.value, 5);
        } else {
            panic!("No MetricScope found for 'test.meter2'");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn instrumentation_scope_identity_test() {
        // Arrange
        let exporter = InMemoryMetricExporter::default();
        let meter_provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .build();

        // Act
        // Meters are identical.
        // Hence there should be a single metric stream output for this test.
        let make_scope = |attributes| {
            InstrumentationScope::builder("test.meter")
                .with_version("v0.1.0")
                .with_schema_url("http://example.com")
                .with_attributes(attributes)
                .build()
        };

        let meter1 =
            meter_provider.meter_with_scope(make_scope(vec![KeyValue::new("key", "value1")]));
        let meter2 =
            meter_provider.meter_with_scope(make_scope(vec![KeyValue::new("key", "value1")]));

        let counter1 = meter1
            .u64_counter("my_counter")
            .with_unit("my_unit")
            .with_description("my_description")
            .build();

        let counter2 = meter2
            .u64_counter("my_counter")
            .with_unit("my_unit")
            .with_description("my_description")
            .build();

        let attribute = vec![KeyValue::new("key1", "value1")];
        counter1.add(10, &attribute);
        counter2.add(5, &attribute);

        meter_provider.force_flush().unwrap();

        // Assert
        let resource_metrics = exporter
            .get_finished_metrics()
            .expect("metrics are expected to be exported.");
        println!("resource_metrics: {resource_metrics:?}");
        assert!(
            resource_metrics[0].scope_metrics.len() == 1,
            "There should be a single scope as the meters are identical"
        );
        assert!(
            resource_metrics[0].scope_metrics[0].metrics.len() == 1,
            "There should be single metric for the scope as instruments are identical"
        );

        let scope = &resource_metrics[0].scope_metrics[0].scope;
        assert_eq!(scope.name(), "test.meter");
        assert_eq!(scope.version(), Some("v0.1.0"));
        assert_eq!(scope.schema_url(), Some("http://example.com"));

        // This is validating current behavior, but it is not guaranteed to be the case in the future,
        // as this is a user error and SDK reserves right to change this behavior.
        assert!(scope.attributes().eq(&[KeyValue::new("key", "value1")]));

        let metric = &resource_metrics[0].scope_metrics[0].metrics[0];
        assert_eq!(metric.name, "my_counter");
        assert_eq!(metric.unit, "my_unit");
        assert_eq!(metric.description, "my_description");

        let MetricData::Sum(sum) = u64::extract_metrics_data_ref(&metric.data)
            .expect("Sum aggregation expected for Counter instruments by default")
        else {
            unreachable!()
        };

        // Expecting 1 time-series.
        assert_eq!(sum.data_points.len(), 1);

        let datapoint = &sum.data_points[0];
        assert_eq!(datapoint.value, 15);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn histogram_aggregation_with_invalid_aggregation_should_proceed_as_if_view_not_exist() {
        // Run this test with stdout enabled to see output.
        // cargo test histogram_aggregation_with_invalid_aggregation_should_proceed_as_if_view_not_exist --features=testing -- --nocapture

        // Arrange
        let exporter = InMemoryMetricExporter::default();
        let view = |i: &Instrument| {
            if i.name == "test_histogram" {
                Stream::builder()
                    .with_aggregation(aggregation::Aggregation::ExplicitBucketHistogram {
                        boundaries: vec![0.9, 1.9, 1.2, 1.3, 1.4, 1.5], // invalid boundaries
                        record_min_max: false,
                    })
                    .with_name("test_histogram_renamed")
                    .with_unit("test_unit_renamed")
                    .build()
                    .ok()
            } else {
                None
            }
        };
        let meter_provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .with_view(view)
            .build();

        // Act
        let meter = meter_provider.meter("test");
        let histogram = meter
            .f64_histogram("test_histogram")
            .with_unit("test_unit")
            .build();

        histogram.record(1.5, &[KeyValue::new("key1", "value1")]);
        meter_provider.force_flush().unwrap();

        // Assert
        let resource_metrics = exporter
            .get_finished_metrics()
            .expect("metrics are expected to be exported.");
        assert!(!resource_metrics.is_empty());
        let metric = &resource_metrics[0].scope_metrics[0].metrics[0];
        assert_eq!(
            metric.name, "test_histogram",
            "View rename should be ignored and original name retained."
        );
        assert_eq!(
            metric.unit, "test_unit",
            "View rename of unit should be ignored and original unit retained."
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn counter_with_lastvalue_aggregation_uses_default() {
        // LastValue aggregation is only valid for Gauge instruments.
        // When applied to a Counter via a view, the view is ignored and
        // the default aggregation (Sum) is used per the spec:
        // "proceed as if the View did not match"

        // Arrange
        let exporter = InMemoryMetricExporter::default();
        let view = |i: &Instrument| {
            if i.name == "my_counter" {
                Stream::builder()
                    .with_aggregation(aggregation::Aggregation::LastValue)
                    .with_name("my_counter_renamed")
                    .build()
                    .ok()
            } else {
                None
            }
        };
        let meter_provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .with_view(view)
            .build();

        // Act
        let meter = meter_provider.meter("test");
        let counter = meter.u64_counter("my_counter").build();
        counter.add(10, &[KeyValue::new("key1", "value1")]);
        meter_provider.force_flush().unwrap();

        // Assert - view is ignored, default aggregation is used
        let resource_metrics = exporter
            .get_finished_metrics()
            .expect("metrics are expected to be exported.");
        assert!(!resource_metrics.is_empty());
        let metric = &resource_metrics[0].scope_metrics[0].metrics[0];
        // Original name is used (view rename is ignored)
        assert_eq!(
            metric.name, "my_counter",
            "View rename should be ignored due to incompatible aggregation."
        );
        // Default Sum aggregation is used
        assert!(
            matches!(
                &metric.data,
                data::AggregatedMetrics::U64(data::MetricData::Sum(_))
            ),
            "Counter should use default Sum aggregation when LastValue is incompatible."
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn gauge_with_sum_aggregation_uses_default() {
        // Sum aggregation is not valid for Gauge instruments.
        // When applied to a Gauge via a view, the view is ignored and
        // the default aggregation (LastValue/Gauge) is used per the spec:
        // "proceed as if the View did not match"

        // Arrange
        let exporter = InMemoryMetricExporter::default();
        let view = |i: &Instrument| {
            if i.name == "my_gauge" {
                Stream::builder()
                    .with_aggregation(aggregation::Aggregation::Sum)
                    .with_name("my_gauge_renamed")
                    .build()
                    .ok()
            } else {
                None
            }
        };
        let meter_provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .with_view(view)
            .build();

        // Act
        let meter = meter_provider.meter("test");
        let gauge = meter.f64_gauge("my_gauge").build();
        gauge.record(42.0, &[KeyValue::new("key1", "value1")]);
        meter_provider.force_flush().unwrap();

        // Assert - view is ignored, default aggregation is used
        let resource_metrics = exporter
            .get_finished_metrics()
            .expect("metrics are expected to be exported.");
        assert!(!resource_metrics.is_empty());
        let metric = &resource_metrics[0].scope_metrics[0].metrics[0];
        // Original name is used (view rename is ignored)
        assert_eq!(
            metric.name, "my_gauge",
            "View rename should be ignored due to incompatible aggregation."
        );
        // Default Gauge (LastValue) aggregation is used
        assert!(
            matches!(
                &metric.data,
                data::AggregatedMetrics::F64(data::MetricData::Gauge(_))
            ),
            "Gauge should use default LastValue aggregation when Sum is incompatible."
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn updowncounter_with_lastvalue_aggregation_uses_default() {
        // LastValue aggregation is only valid for Gauge instruments.
        // When applied to an UpDownCounter via a view, the view is ignored and
        // the default aggregation (Sum) is used per the spec:
        // "proceed as if the View did not match"

        // Arrange
        let exporter = InMemoryMetricExporter::default();
        let view = |i: &Instrument| {
            if i.name == "my_updown_counter" {
                Stream::builder()
                    .with_aggregation(aggregation::Aggregation::LastValue)
                    .with_name("my_updown_counter_renamed")
                    .build()
                    .ok()
            } else {
                None
            }
        };
        let meter_provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .with_view(view)
            .build();

        // Act
        let meter = meter_provider.meter("test");
        let counter = meter.i64_up_down_counter("my_updown_counter").build();
        counter.add(-5, &[KeyValue::new("key1", "value1")]);
        meter_provider.force_flush().unwrap();

        // Assert - view is ignored, default aggregation is used
        let resource_metrics = exporter
            .get_finished_metrics()
            .expect("metrics are expected to be exported.");
        assert!(!resource_metrics.is_empty());
        let metric = &resource_metrics[0].scope_metrics[0].metrics[0];
        // Original name is used (view rename is ignored)
        assert_eq!(
            metric.name, "my_updown_counter",
            "View rename should be ignored due to incompatible aggregation."
        );
        // Default Sum aggregation is used
        assert!(
            matches!(
                &metric.data,
                data::AggregatedMetrics::I64(data::MetricData::Sum(_))
            ),
            "UpDownCounter should use default Sum aggregation when LastValue is incompatible."
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn histogram_with_lastvalue_aggregation_uses_default() {
        // LastValue aggregation is only valid for Gauge instruments.
        // When applied to a Histogram via a view, the view is ignored and
        // the default aggregation (ExplicitBucketHistogram) is used per the spec:
        // "proceed as if the View did not match"

        // Arrange
        let exporter = InMemoryMetricExporter::default();
        let view = |i: &Instrument| {
            if i.name == "my_histogram" {
                Stream::builder()
                    .with_aggregation(aggregation::Aggregation::LastValue)
                    .with_name("my_histogram_renamed")
                    .build()
                    .ok()
            } else {
                None
            }
        };
        let meter_provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .with_view(view)
            .build();

        // Act
        let meter = meter_provider.meter("test");
        let histogram = meter.f64_histogram("my_histogram").build();
        histogram.record(42.0, &[KeyValue::new("key1", "value1")]);
        meter_provider.force_flush().unwrap();

        // Assert - view is ignored, default aggregation is used
        let resource_metrics = exporter
            .get_finished_metrics()
            .expect("metrics are expected to be exported.");
        assert!(!resource_metrics.is_empty());
        let metric = &resource_metrics[0].scope_metrics[0].metrics[0];
        // Original name is used (view rename is ignored)
        assert_eq!(
            metric.name, "my_histogram",
            "View rename should be ignored due to incompatible aggregation."
        );
        // Default Histogram aggregation is used
        assert!(
            matches!(
                &metric.data,
                data::AggregatedMetrics::F64(data::MetricData::Histogram(_))
            ),
            "Histogram should use default ExplicitBucketHistogram aggregation when LastValue is incompatible."
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn observable_gauge_with_sum_aggregation_uses_default() {
        // Sum aggregation is not valid for Observable Gauge instruments.
        // When applied to an Observable Gauge via a view, the view is ignored and
        // the default aggregation (LastValue/Gauge) is used per the spec:
        // "proceed as if the View did not match"

        // Arrange
        let exporter = InMemoryMetricExporter::default();
        let view = |i: &Instrument| {
            if i.name == "my_observable_gauge" {
                Stream::builder()
                    .with_aggregation(aggregation::Aggregation::Sum)
                    .with_name("my_observable_gauge_renamed")
                    .build()
                    .ok()
            } else {
                None
            }
        };
        let meter_provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .with_view(view)
            .build();

        // Act
        let meter = meter_provider.meter("test");
        let _observable_gauge = meter
            .f64_observable_gauge("my_observable_gauge")
            .with_callback(|observer| {
                observer.observe(42.0, &[KeyValue::new("key1", "value1")]);
            })
            .build();
        meter_provider.force_flush().unwrap();

        // Assert - view is ignored, default aggregation is used
        let resource_metrics = exporter
            .get_finished_metrics()
            .expect("metrics are expected to be exported.");
        assert!(!resource_metrics.is_empty());
        let metric = &resource_metrics[0].scope_metrics[0].metrics[0];
        // Original name is used (view rename is ignored)
        assert_eq!(
            metric.name, "my_observable_gauge",
            "View rename should be ignored due to incompatible aggregation."
        );
        // Default Gauge (LastValue) aggregation is used
        assert!(
            matches!(
                &metric.data,
                data::AggregatedMetrics::F64(data::MetricData::Gauge(_))
            ),
            "Observable Gauge should use default LastValue aggregation when Sum is incompatible."
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn observable_counter_with_lastvalue_aggregation_uses_default() {
        // LastValue aggregation is only valid for Gauge instruments.
        // When applied to an Observable Counter via a view, the view is ignored and
        // the default aggregation (Sum) is used per the spec:
        // "proceed as if the View did not match"

        // Arrange
        let exporter = InMemoryMetricExporter::default();
        let view = |i: &Instrument| {
            if i.name == "my_observable_counter" {
                Stream::builder()
                    .with_aggregation(aggregation::Aggregation::LastValue)
                    .with_name("my_observable_counter_renamed")
                    .build()
                    .ok()
            } else {
                None
            }
        };
        let meter_provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .with_view(view)
            .build();

        // Act
        let meter = meter_provider.meter("test");
        let _observable_counter = meter
            .u64_observable_counter("my_observable_counter")
            .with_callback(|observer| {
                observer.observe(100, &[KeyValue::new("key1", "value1")]);
            })
            .build();
        meter_provider.force_flush().unwrap();

        // Assert - view is ignored, default aggregation is used
        let resource_metrics = exporter
            .get_finished_metrics()
            .expect("metrics are expected to be exported.");
        assert!(!resource_metrics.is_empty());
        let metric = &resource_metrics[0].scope_metrics[0].metrics[0];
        // Original name is used (view rename is ignored)
        assert_eq!(
            metric.name, "my_observable_counter",
            "View rename should be ignored due to incompatible aggregation."
        );
        // Default Sum aggregation is used
        assert!(
            matches!(
                &metric.data,
                data::AggregatedMetrics::U64(data::MetricData::Sum(_))
            ),
            "Observable Counter should use default Sum aggregation when LastValue is incompatible."
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn observable_updowncounter_with_lastvalue_aggregation_uses_default() {
        // LastValue aggregation is only valid for Gauge instruments.
        // When applied to an Observable UpDownCounter via a view, the view is ignored and
        // the default aggregation (Sum) is used per the spec:
        // "proceed as if the View did not match"

        // Arrange
        let exporter = InMemoryMetricExporter::default();
        let view = |i: &Instrument| {
            if i.name == "my_observable_updowncounter" {
                Stream::builder()
                    .with_aggregation(aggregation::Aggregation::LastValue)
                    .with_name("my_observable_updowncounter_renamed")
                    .build()
                    .ok()
            } else {
                None
            }
        };
        let meter_provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .with_view(view)
            .build();

        // Act
        let meter = meter_provider.meter("test");
        let _observable_updowncounter = meter
            .i64_observable_up_down_counter("my_observable_updowncounter")
            .with_callback(|observer| {
                observer.observe(-50, &[KeyValue::new("key1", "value1")]);
            })
            .build();
        meter_provider.force_flush().unwrap();

        // Assert - view is ignored, default aggregation is used
        let resource_metrics = exporter
            .get_finished_metrics()
            .expect("metrics are expected to be exported.");
        assert!(!resource_metrics.is_empty());
        let metric = &resource_metrics[0].scope_metrics[0].metrics[0];
        // Original name is used (view rename is ignored)
        assert_eq!(
            metric.name, "my_observable_updowncounter",
            "View rename should be ignored due to incompatible aggregation."
        );
        // Default Sum aggregation is used
        assert!(
            matches!(
                &metric.data,
                data::AggregatedMetrics::I64(data::MetricData::Sum(_))
            ),
            "Observable UpDownCounter should use default Sum aggregation when LastValue is incompatible."
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn histogram_with_sum_aggregation_is_valid() {
        // Sum aggregation is valid for Histogram instruments.
        // When applied via a view, the aggregation should change from
        // ExplicitBucketHistogram to Sum.

        // Arrange
        let exporter = InMemoryMetricExporter::default();
        let view = |i: &Instrument| {
            if i.name == "my_histogram" {
                Stream::builder()
                    .with_aggregation(aggregation::Aggregation::Sum)
                    .with_name("my_histogram_renamed")
                    .build()
                    .ok()
            } else {
                None
            }
        };
        let meter_provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .with_view(view)
            .build();

        // Act
        let meter = meter_provider.meter("test");
        let histogram = meter.f64_histogram("my_histogram").build();
        histogram.record(10.0, &[KeyValue::new("key1", "value1")]);
        histogram.record(20.0, &[KeyValue::new("key1", "value1")]);
        histogram.record(30.0, &[KeyValue::new("key1", "value1")]);
        meter_provider.force_flush().unwrap();

        // Assert - view is applied, Sum aggregation is used
        let resource_metrics = exporter
            .get_finished_metrics()
            .expect("metrics are expected to be exported.");
        assert!(!resource_metrics.is_empty());
        let metric = &resource_metrics[0].scope_metrics[0].metrics[0];
        // View rename is applied
        assert_eq!(
            metric.name, "my_histogram_renamed",
            "View rename should be applied for compatible aggregation."
        );
        // Sum aggregation is used instead of default Histogram
        let MetricData::Sum(sum) = f64::extract_metrics_data_ref(&metric.data)
            .expect("Sum aggregation expected when view specifies Sum")
        else {
            panic!("Expected Sum aggregation for Histogram with Sum view");
        };
        assert_eq!(sum.data_points.len(), 1);
        assert_eq!(sum.data_points[0].value, 60.0); // 10 + 20 + 30
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn gauge_with_histogram_aggregation_is_valid() {
        // Histogram aggregation is valid for Gauge instruments.
        // When applied via a view, the aggregation should change from
        // LastValue to ExplicitBucketHistogram.

        // Arrange
        let exporter = InMemoryMetricExporter::default();
        let view = |i: &Instrument| {
            if i.name == "my_gauge" {
                Stream::builder()
                    .with_aggregation(aggregation::Aggregation::ExplicitBucketHistogram {
                        boundaries: vec![5.0, 10.0, 25.0, 50.0],
                        record_min_max: true,
                    })
                    .with_name("my_gauge_renamed")
                    .build()
                    .ok()
            } else {
                None
            }
        };
        let meter_provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .with_view(view)
            .build();

        // Act
        let meter = meter_provider.meter("test");
        let gauge = meter.f64_gauge("my_gauge").build();
        gauge.record(3.0, &[KeyValue::new("key1", "value1")]);
        gauge.record(7.0, &[KeyValue::new("key1", "value1")]);
        gauge.record(15.0, &[KeyValue::new("key1", "value1")]);
        gauge.record(30.0, &[KeyValue::new("key1", "value1")]);
        meter_provider.force_flush().unwrap();

        // Assert - view is applied, Histogram aggregation is used
        let resource_metrics = exporter
            .get_finished_metrics()
            .expect("metrics are expected to be exported.");
        assert!(!resource_metrics.is_empty());
        let metric = &resource_metrics[0].scope_metrics[0].metrics[0];
        // View rename is applied
        assert_eq!(
            metric.name, "my_gauge_renamed",
            "View rename should be applied for compatible aggregation."
        );
        // Histogram aggregation is used instead of default LastValue
        let MetricData::Histogram(histogram) = f64::extract_metrics_data_ref(&metric.data)
            .expect("Histogram aggregation expected when view specifies Histogram")
        else {
            panic!("Expected Histogram aggregation for Gauge with Histogram view");
        };
        assert_eq!(histogram.data_points.len(), 1);
        let dp = &histogram.data_points[0];
        assert_eq!(dp.count, 4);
        assert_eq!(dp.sum, 55.0); // 3 + 7 + 15 + 30
        assert_eq!(dp.min, Some(3.0));
        assert_eq!(dp.max, Some(30.0));
        // Bucket boundaries: [5.0, 10.0, 25.0, 50.0]
        // Values: 3.0 (bucket 0), 7.0 (bucket 1), 15.0 (bucket 2), 30.0 (bucket 3)
        assert_eq!(dp.bucket_counts, vec![1, 1, 1, 1, 0]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn counter_with_histogram_aggregation_is_valid() {
        // Histogram aggregation is valid for Counter instruments.
        // When applied via a view, the aggregation should change from
        // Sum to ExplicitBucketHistogram.

        // Arrange
        let exporter = InMemoryMetricExporter::default();
        let view = |i: &Instrument| {
            if i.name == "my_counter" {
                Stream::builder()
                    .with_aggregation(aggregation::Aggregation::ExplicitBucketHistogram {
                        boundaries: vec![5.0, 10.0, 25.0, 50.0],
                        record_min_max: true,
                    })
                    .with_name("my_counter_renamed")
                    .build()
                    .ok()
            } else {
                None
            }
        };
        let meter_provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .with_view(view)
            .build();

        // Act
        let meter = meter_provider.meter("test");
        let counter = meter.u64_counter("my_counter").build();
        counter.add(3, &[KeyValue::new("key1", "value1")]);
        counter.add(7, &[KeyValue::new("key1", "value1")]);
        counter.add(15, &[KeyValue::new("key1", "value1")]);
        counter.add(30, &[KeyValue::new("key1", "value1")]);
        meter_provider.force_flush().unwrap();

        // Assert - view is applied, Histogram aggregation is used
        let resource_metrics = exporter
            .get_finished_metrics()
            .expect("metrics are expected to be exported.");
        assert!(!resource_metrics.is_empty());
        let metric = &resource_metrics[0].scope_metrics[0].metrics[0];
        // View rename is applied
        assert_eq!(
            metric.name, "my_counter_renamed",
            "View rename should be applied for compatible aggregation."
        );
        // Histogram aggregation is used instead of default Sum
        let MetricData::Histogram(histogram) = u64::extract_metrics_data_ref(&metric.data)
            .expect("Histogram aggregation expected when view specifies Histogram")
        else {
            panic!("Expected Histogram aggregation for Counter with Histogram view");
        };
        assert_eq!(histogram.data_points.len(), 1);
        let dp = &histogram.data_points[0];
        assert_eq!(dp.count, 4);
        assert_eq!(dp.sum, 55); // 3 + 7 + 15 + 30
        assert_eq!(dp.min, Some(3));
        assert_eq!(dp.max, Some(30));
        // Bucket boundaries: [5.0, 10.0, 25.0, 50.0]
        // Values: 3 (bucket 0), 7 (bucket 1), 15 (bucket 2), 30 (bucket 3)
        assert_eq!(dp.bucket_counts, vec![1, 1, 1, 1, 0]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn updowncounter_with_histogram_aggregation_is_valid() {
        // Histogram aggregation is valid for UpDownCounter instruments.
        // When applied via a view, the aggregation should change from
        // Sum to ExplicitBucketHistogram.

        // Arrange
        let exporter = InMemoryMetricExporter::default();
        let view = |i: &Instrument| {
            if i.name == "my_updowncounter" {
                Stream::builder()
                    .with_aggregation(aggregation::Aggregation::ExplicitBucketHistogram {
                        boundaries: vec![0.0, 10.0, 20.0, 50.0],
                        record_min_max: true,
                    })
                    .with_name("my_updowncounter_renamed")
                    .build()
                    .ok()
            } else {
                None
            }
        };
        let meter_provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .with_view(view)
            .build();

        // Act
        let meter = meter_provider.meter("test");
        let updowncounter = meter.i64_up_down_counter("my_updowncounter").build();
        updowncounter.add(-5, &[KeyValue::new("key1", "value1")]);
        updowncounter.add(15, &[KeyValue::new("key1", "value1")]);
        updowncounter.add(25, &[KeyValue::new("key1", "value1")]);
        meter_provider.force_flush().unwrap();

        // Assert - view is applied, Histogram aggregation is used
        let resource_metrics = exporter
            .get_finished_metrics()
            .expect("metrics are expected to be exported.");
        assert!(!resource_metrics.is_empty());
        let metric = &resource_metrics[0].scope_metrics[0].metrics[0];
        // View rename is applied
        assert_eq!(
            metric.name, "my_updowncounter_renamed",
            "View rename should be applied for compatible aggregation."
        );
        // Histogram aggregation is used instead of default Sum
        let MetricData::Histogram(histogram) = i64::extract_metrics_data_ref(&metric.data)
            .expect("Histogram aggregation expected when view specifies Histogram")
        else {
            panic!("Expected Histogram aggregation for UpDownCounter with Histogram view");
        };
        assert_eq!(histogram.data_points.len(), 1);
        let dp = &histogram.data_points[0];
        assert_eq!(dp.count, 3);
        // Note: Sum is not recorded for UpDownCounter histogram per the spec
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn counter_with_exponential_histogram_aggregation_is_valid() {
        // ExponentialHistogram aggregation is valid for Counter instruments.
        // When applied via a view, the aggregation should change from
        // Sum to Base2ExponentialHistogram.

        // Arrange
        let exporter = InMemoryMetricExporter::default();
        let view = |i: &Instrument| {
            if i.name == "my_counter" {
                Stream::builder()
                    .with_aggregation(aggregation::Aggregation::Base2ExponentialHistogram {
                        max_size: 160,
                        max_scale: 20,
                        record_min_max: true,
                    })
                    .with_name("my_counter_renamed")
                    .build()
                    .ok()
            } else {
                None
            }
        };
        let meter_provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .with_view(view)
            .build();

        // Act
        let meter = meter_provider.meter("test");
        let counter = meter.u64_counter("my_counter").build();
        counter.add(5, &[KeyValue::new("key1", "value1")]);
        counter.add(10, &[KeyValue::new("key1", "value1")]);
        counter.add(20, &[KeyValue::new("key1", "value1")]);
        meter_provider.force_flush().unwrap();

        // Assert - view is applied, ExponentialHistogram aggregation is used
        let resource_metrics = exporter
            .get_finished_metrics()
            .expect("metrics are expected to be exported.");
        assert!(!resource_metrics.is_empty());
        let metric = &resource_metrics[0].scope_metrics[0].metrics[0];
        // View rename is applied
        assert_eq!(
            metric.name, "my_counter_renamed",
            "View rename should be applied for compatible aggregation."
        );
        // ExponentialHistogram aggregation is used instead of default Sum
        let MetricData::ExponentialHistogram(exp_hist) =
            u64::extract_metrics_data_ref(&metric.data)
                .expect("ExponentialHistogram aggregation expected when view specifies it")
        else {
            panic!("Expected ExponentialHistogram aggregation for Counter with ExponentialHistogram view");
        };
        assert_eq!(exp_hist.data_points.len(), 1);
        let dp = &exp_hist.data_points[0];
        assert_eq!(dp.count(), 3);
        assert_eq!(dp.sum(), 35); // 5 + 10 + 20
        assert_eq!(dp.min(), Some(5));
        assert_eq!(dp.max(), Some(20));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn gauge_with_exponential_histogram_aggregation_is_valid() {
        // ExponentialHistogram aggregation is valid for Gauge instruments.
        // When applied via a view, the aggregation should change from
        // LastValue to Base2ExponentialHistogram.

        // Arrange
        let exporter = InMemoryMetricExporter::default();
        let view = |i: &Instrument| {
            if i.name == "my_gauge" {
                Stream::builder()
                    .with_aggregation(aggregation::Aggregation::Base2ExponentialHistogram {
                        max_size: 160,
                        max_scale: 20,
                        record_min_max: true,
                    })
                    .with_name("my_gauge_renamed")
                    .build()
                    .ok()
            } else {
                None
            }
        };
        let meter_provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .with_view(view)
            .build();

        // Act
        let meter = meter_provider.meter("test");
        let gauge = meter.f64_gauge("my_gauge").build();
        gauge.record(2.5, &[KeyValue::new("key1", "value1")]);
        gauge.record(7.5, &[KeyValue::new("key1", "value1")]);
        gauge.record(15.0, &[KeyValue::new("key1", "value1")]);
        meter_provider.force_flush().unwrap();

        // Assert - view is applied, ExponentialHistogram aggregation is used
        let resource_metrics = exporter
            .get_finished_metrics()
            .expect("metrics are expected to be exported.");
        assert!(!resource_metrics.is_empty());
        let metric = &resource_metrics[0].scope_metrics[0].metrics[0];
        // View rename is applied
        assert_eq!(
            metric.name, "my_gauge_renamed",
            "View rename should be applied for compatible aggregation."
        );
        // ExponentialHistogram aggregation is used instead of default LastValue
        let MetricData::ExponentialHistogram(exp_hist) =
            f64::extract_metrics_data_ref(&metric.data)
                .expect("ExponentialHistogram aggregation expected when view specifies it")
        else {
            panic!("Expected ExponentialHistogram aggregation for Gauge with ExponentialHistogram view");
        };
        assert_eq!(exp_hist.data_points.len(), 1);
        let dp = &exp_hist.data_points[0];
        assert_eq!(dp.count(), 3);
        assert_eq!(dp.sum(), 25.0); // 2.5 + 7.5 + 15.0
        assert_eq!(dp.min(), Some(2.5));
        assert_eq!(dp.max(), Some(15.0));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn counter_with_drop_aggregation_is_dropped() {
        // Run this test with stdout enabled to see output.
        // cargo test counter_with_drop_aggregation_is_dropped --features=testing -- --nocapture

        // When a view matches an instrument and specifies Aggregation::Drop,
        // the instrument should be dropped and no metrics should be exported.

        // Arrange
        let exporter = InMemoryMetricExporter::default();
        let view = |i: &Instrument| {
            if i.name == "my_counter_to_drop" {
                Stream::builder()
                    .with_aggregation(aggregation::Aggregation::Drop)
                    .build()
                    .ok()
            } else {
                None
            }
        };
        let meter_provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .with_view(view)
            .build();

        // Act
        let meter = meter_provider.meter("test");
        let counter = meter.u64_counter("my_counter_to_drop").build();
        counter.add(10, &[KeyValue::new("key1", "value1")]);
        meter_provider.force_flush().unwrap();

        // Assert - no metrics should be exported because the view drops the instrument
        let resource_metrics = exporter
            .get_finished_metrics()
            .expect("metrics result expected");
        assert!(
            resource_metrics.is_empty()
                || resource_metrics[0].scope_metrics.is_empty()
                || resource_metrics[0].scope_metrics[0].metrics.is_empty(),
            "No metrics should be exported when view uses Aggregation::Drop. Got: {:?}",
            resource_metrics
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn histogram_with_drop_aggregation_is_dropped() {
        // Run this test with stdout enabled to see output.
        // cargo test histogram_with_drop_aggregation_is_dropped --features=testing -- --nocapture

        // When a view matches a histogram and specifies Aggregation::Drop,
        // the instrument should be dropped and no metrics should be exported.

        // Arrange
        let exporter = InMemoryMetricExporter::default();
        let view = |i: &Instrument| {
            if i.name == "my_histogram_to_drop" {
                Stream::builder()
                    .with_aggregation(aggregation::Aggregation::Drop)
                    .build()
                    .ok()
            } else {
                None
            }
        };
        let meter_provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .with_view(view)
            .build();

        // Act
        let meter = meter_provider.meter("test");
        let histogram = meter.f64_histogram("my_histogram_to_drop").build();
        histogram.record(42.0, &[KeyValue::new("key1", "value1")]);
        meter_provider.force_flush().unwrap();

        // Assert - no metrics should be exported because the view drops the instrument
        let resource_metrics = exporter
            .get_finished_metrics()
            .expect("metrics result expected");
        assert!(
            resource_metrics.is_empty()
                || resource_metrics[0].scope_metrics.is_empty()
                || resource_metrics[0].scope_metrics[0].metrics.is_empty(),
            "No metrics should be exported when view uses Aggregation::Drop. Got: {:?}",
            resource_metrics
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn gauge_with_drop_aggregation_is_dropped() {
        // Run this test with stdout enabled to see output.
        // cargo test gauge_with_drop_aggregation_is_dropped --features=testing -- --nocapture

        // When a view matches a gauge and specifies Aggregation::Drop,
        // the instrument should be dropped and no metrics should be exported.

        // Arrange
        let exporter = InMemoryMetricExporter::default();
        let view = |i: &Instrument| {
            if i.name == "my_gauge_to_drop" {
                Stream::builder()
                    .with_aggregation(aggregation::Aggregation::Drop)
                    .build()
                    .ok()
            } else {
                None
            }
        };
        let meter_provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .with_view(view)
            .build();

        // Act
        let meter = meter_provider.meter("test");
        let gauge = meter.f64_gauge("my_gauge_to_drop").build();
        gauge.record(42.0, &[KeyValue::new("key1", "value1")]);
        meter_provider.force_flush().unwrap();

        // Assert - no metrics should be exported because the view drops the instrument
        let resource_metrics = exporter
            .get_finished_metrics()
            .expect("metrics result expected");
        assert!(
            resource_metrics.is_empty()
                || resource_metrics[0].scope_metrics.is_empty()
                || resource_metrics[0].scope_metrics[0].metrics.is_empty(),
            "No metrics should be exported when view uses Aggregation::Drop. Got: {:?}",
            resource_metrics
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn counter_with_drop_aggregation_and_rename_should_still_drop() {
        // Run this test with stdout enabled to see output.
        // cargo test counter_with_drop_aggregation_and_rename_should_still_drop --features=testing -- --nocapture

        // When a view matches and specifies Aggregation::Drop, the instrument should be
        // dropped even if the view also specifies other customizations (like name).
        // The name customization is meaningless for a dropped instrument, but the Drop
        // intent should still be honored.

        // Arrange
        let exporter = InMemoryMetricExporter::default();
        let view = |i: &Instrument| {
            if i.name == "my_counter" {
                Stream::builder()
                    .with_name("dropped_counter") // Meaningless but shouldn't break Drop
                    .with_aggregation(aggregation::Aggregation::Drop)
                    .build()
                    .ok()
            } else {
                None
            }
        };
        let meter_provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .with_view(view)
            .build();

        // Act
        let meter = meter_provider.meter("test");
        let counter = meter.u64_counter("my_counter").build();
        counter.add(10, &[KeyValue::new("key1", "value1")]);
        meter_provider.force_flush().unwrap();

        // Assert - no metrics should be exported because the view drops the instrument
        let resource_metrics = exporter
            .get_finished_metrics()
            .expect("metrics result expected");
        assert!(
            resource_metrics.is_empty()
                || resource_metrics[0].scope_metrics.is_empty()
                || resource_metrics[0].scope_metrics[0].metrics.is_empty(),
            "No metrics should be exported when view uses Aggregation::Drop, even with rename. Got: {:?}",
            resource_metrics
        );
    }

    #[cfg(feature = "spec_unstable_metrics_views")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[ignore = "Spatial aggregation is not yet implemented."]
    async fn spatial_aggregation_when_view_drops_attributes_observable_counter() {
        // cargo test metrics::tests::spatial_aggregation_when_view_drops_attributes_observable_counter --features=testing

        // Arrange
        let exporter = InMemoryMetricExporter::default();
        // View drops all attributes.
        let view = |i: &Instrument| {
            if i.name == "my_observable_counter" {
                Stream::builder()
                    .with_allowed_attribute_keys(vec![])
                    .build()
                    .ok()
            } else {
                None
            }
        };
        let meter_provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .with_view(view)
            .build();

        // Act
        let meter = meter_provider.meter("test");
        let _observable_counter = meter
            .u64_observable_counter("my_observable_counter")
            .with_callback(|observer| {
                observer.observe(
                    100,
                    &[
                        KeyValue::new("statusCode", "200"),
                        KeyValue::new("verb", "get"),
                    ],
                );

                observer.observe(
                    100,
                    &[
                        KeyValue::new("statusCode", "200"),
                        KeyValue::new("verb", "post"),
                    ],
                );

                observer.observe(
                    100,
                    &[
                        KeyValue::new("statusCode", "500"),
                        KeyValue::new("verb", "get"),
                    ],
                );
            })
            .build();

        meter_provider.force_flush().unwrap();

        // Assert
        let resource_metrics = exporter
            .get_finished_metrics()
            .expect("metrics are expected to be exported.");
        assert!(!resource_metrics.is_empty());
        let metric = &resource_metrics[0].scope_metrics[0].metrics[0];
        assert_eq!(metric.name, "my_observable_counter",);

        let MetricData::Sum(sum) = u64::extract_metrics_data_ref(&metric.data)
            .expect("Sum aggregation expected for ObservableCounter instruments by default")
        else {
            unreachable!()
        };

        // Expecting 1 time-series only, as the view drops all attributes resulting
        // in a single time-series.
        // This is failing today, due to lack of support for spatial aggregation.
        assert_eq!(sum.data_points.len(), 1);

        // find and validate the single datapoint
        let data_point = &sum.data_points[0];
        assert_eq!(data_point.value, 300);
    }

    #[cfg(feature = "spec_unstable_metrics_views")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn spatial_aggregation_when_view_drops_attributes_counter() {
        // cargo test spatial_aggregation_when_view_drops_attributes_counter --features=testing

        // Arrange
        let exporter = InMemoryMetricExporter::default();
        // View drops all attributes.
        let view = |i: &Instrument| {
            if i.name == "my_counter" {
                Some(
                    Stream::builder()
                        .with_allowed_attribute_keys(vec![])
                        .build()
                        .unwrap(),
                )
            } else {
                None
            }
        };
        let meter_provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .with_view(view)
            .build();

        // Act
        let meter = meter_provider.meter("test");
        let counter = meter.u64_counter("my_counter").build();

        // Normally, this would generate 3 time-series, but since the view
        // drops all attributes, we expect only 1 time-series.
        counter.add(
            10,
            [
                KeyValue::new("statusCode", "200"),
                KeyValue::new("verb", "Get"),
            ]
            .as_ref(),
        );

        counter.add(
            10,
            [
                KeyValue::new("statusCode", "500"),
                KeyValue::new("verb", "Get"),
            ]
            .as_ref(),
        );

        counter.add(
            10,
            [
                KeyValue::new("statusCode", "200"),
                KeyValue::new("verb", "Post"),
            ]
            .as_ref(),
        );

        meter_provider.force_flush().unwrap();

        // Assert
        let resource_metrics = exporter
            .get_finished_metrics()
            .expect("metrics are expected to be exported.");
        assert!(!resource_metrics.is_empty());
        let metric = &resource_metrics[0].scope_metrics[0].metrics[0];
        assert_eq!(metric.name, "my_counter",);

        let MetricData::Sum(sum) = u64::extract_metrics_data_ref(&metric.data)
            .expect("Sum aggregation expected for Counter instruments by default")
        else {
            unreachable!()
        };

        // Expecting 1 time-series only, as the view drops all attributes resulting
        // in a single time-series.
        // This is failing today, due to lack of support for spatial aggregation.
        assert_eq!(sum.data_points.len(), 1);
        // find and validate the single datapoint
        let data_point = &sum.data_points[0];
        assert_eq!(data_point.value, 30);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn no_attr_cumulative_up_down_counter() {
        let mut test_context = TestContext::new(Temporality::Cumulative);
        let counter = test_context.i64_up_down_counter("test", "my_counter", Some("my_unit"));

        counter.add(50, &[]);
        test_context.flush_metrics();

        let MetricData::Sum(sum) =
            test_context.get_aggregation::<i64>("my_counter", Some("my_unit"))
        else {
            unreachable!()
        };

        assert_eq!(sum.data_points.len(), 1, "Expected only one data point");
        assert!(!sum.is_monotonic, "Should not produce monotonic.");
        assert_eq!(
            sum.temporality,
            Temporality::Cumulative,
            "Should produce cumulative"
        );

        let data_point = &sum.data_points[0];
        assert!(data_point.attributes.is_empty(), "Non-empty attribute set");
        assert_eq!(data_point.value, 50, "Unexpected data point value");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn no_attr_up_down_counter_always_cumulative() {
        let mut test_context = TestContext::new(Temporality::Delta);
        let counter = test_context.i64_up_down_counter("test", "my_counter", Some("my_unit"));

        counter.add(50, &[]);
        test_context.flush_metrics();

        let MetricData::Sum(sum) =
            test_context.get_aggregation::<i64>("my_counter", Some("my_unit"))
        else {
            unreachable!()
        };

        assert_eq!(sum.data_points.len(), 1, "Expected only one data point");
        assert!(!sum.is_monotonic, "Should not produce monotonic.");
        assert_eq!(
            sum.temporality,
            Temporality::Cumulative,
            "Should produce Cumulative due to UpDownCounter temporality_preference"
        );

        let data_point = &sum.data_points[0];
        assert!(data_point.attributes.is_empty(), "Non-empty attribute set");
        assert_eq!(data_point.value, 50, "Unexpected data point value");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn no_attr_cumulative_counter_value_added_after_export() {
        let mut test_context = TestContext::new(Temporality::Cumulative);
        let counter = test_context.u64_counter("test", "my_counter", None);

        counter.add(50, &[]);
        test_context.flush_metrics();
        let _ = test_context.get_aggregation::<u64>("my_counter", None);
        test_context.reset_metrics();

        counter.add(5, &[]);
        test_context.flush_metrics();
        let MetricData::Sum(sum) = test_context.get_aggregation::<u64>("my_counter", None) else {
            unreachable!()
        };

        assert_eq!(sum.data_points.len(), 1, "Expected only one data point");
        assert!(sum.is_monotonic, "Should produce monotonic.");
        assert_eq!(
            sum.temporality,
            Temporality::Cumulative,
            "Should produce cumulative"
        );

        let data_point = &sum.data_points[0];
        assert!(data_point.attributes.is_empty(), "Non-empty attribute set");
        assert_eq!(data_point.value, 55, "Unexpected data point value");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn no_attr_delta_counter_value_reset_after_export() {
        let mut test_context = TestContext::new(Temporality::Delta);
        let counter = test_context.u64_counter("test", "my_counter", None);

        counter.add(50, &[]);
        test_context.flush_metrics();
        let _ = test_context.get_aggregation::<u64>("my_counter", None);
        test_context.reset_metrics();

        counter.add(5, &[]);
        test_context.flush_metrics();
        let MetricData::Sum(sum) = test_context.get_aggregation::<u64>("my_counter", None) else {
            unreachable!()
        };

        assert_eq!(sum.data_points.len(), 1, "Expected only one data point");
        assert!(sum.is_monotonic, "Should produce monotonic.");
        assert_eq!(
            sum.temporality,
            Temporality::Delta,
            "Should produce cumulative"
        );

        let data_point = &sum.data_points[0];
        assert!(data_point.attributes.is_empty(), "Non-empty attribute set");
        assert_eq!(data_point.value, 5, "Unexpected data point value");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn second_delta_export_does_not_give_no_attr_value_if_add_not_called() {
        let mut test_context = TestContext::new(Temporality::Delta);
        let counter = test_context.u64_counter("test", "my_counter", None);

        counter.add(50, &[]);
        test_context.flush_metrics();
        let _ = test_context.get_aggregation::<u64>("my_counter", None);
        test_context.reset_metrics();

        counter.add(50, &[KeyValue::new("a", "b")]);
        test_context.flush_metrics();
        let MetricData::Sum(sum) = test_context.get_aggregation::<u64>("my_counter", None) else {
            unreachable!()
        };

        let no_attr_data_point = sum.data_points.iter().find(|x| x.attributes.is_empty());

        assert!(
            no_attr_data_point.is_none(),
            "Expected no data points with no attributes"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn delta_memory_efficiency_test() {
        // Run this test with stdout enabled to see output.
        // cargo test delta_memory_efficiency_test --features=testing -- --nocapture

        // Arrange
        let mut test_context = TestContext::new(Temporality::Delta);
        let counter = test_context.u64_counter("test", "my_counter", None);

        // Act
        counter.add(1, &[KeyValue::new("key1", "value1")]);
        counter.add(1, &[KeyValue::new("key1", "value1")]);
        counter.add(1, &[KeyValue::new("key1", "value1")]);
        counter.add(1, &[KeyValue::new("key1", "value1")]);
        counter.add(1, &[KeyValue::new("key1", "value1")]);

        counter.add(1, &[KeyValue::new("key1", "value2")]);
        counter.add(1, &[KeyValue::new("key1", "value2")]);
        counter.add(1, &[KeyValue::new("key1", "value2")]);
        test_context.flush_metrics();

        let MetricData::Sum(sum) = test_context.get_aggregation::<u64>("my_counter", None) else {
            unreachable!()
        };

        // Expecting 2 time-series.
        assert_eq!(sum.data_points.len(), 2);

        // find and validate key1=value1 datapoint
        let data_point1 = find_sum_datapoint_with_key_value(&sum.data_points, "key1", "value1")
            .expect("datapoint with key1=value1 expected");
        assert_eq!(data_point1.value, 5);

        // find and validate key1=value2 datapoint
        let data_point1 = find_sum_datapoint_with_key_value(&sum.data_points, "key1", "value2")
            .expect("datapoint with key1=value2 expected");
        assert_eq!(data_point1.value, 3);

        test_context.exporter.reset();
        // flush again, and validate that nothing is flushed
        // as delta temporality.
        test_context.flush_metrics();

        let resource_metrics = test_context
            .exporter
            .get_finished_metrics()
            .expect("metrics are expected to be exported.");
        println!("resource_metrics: {resource_metrics:?}");
        assert!(resource_metrics.is_empty(), "No metrics should be exported as no new measurements were recorded since last collect.");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn counter_multithreaded() {
        // Run this test with stdout enabled to see output.
        // cargo test counter_multithreaded --features=testing -- --nocapture

        counter_multithreaded_aggregation_helper(Temporality::Delta);
        counter_multithreaded_aggregation_helper(Temporality::Cumulative);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn counter_f64_multithreaded() {
        // Run this test with stdout enabled to see output.
        // cargo test counter_f64_multithreaded --features=testing -- --nocapture

        counter_f64_multithreaded_aggregation_helper(Temporality::Delta);
        counter_f64_multithreaded_aggregation_helper(Temporality::Cumulative);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn histogram_multithreaded() {
        // Run this test with stdout enabled to see output.
        // cargo test histogram_multithreaded --features=testing -- --nocapture

        histogram_multithreaded_aggregation_helper(Temporality::Delta);
        histogram_multithreaded_aggregation_helper(Temporality::Cumulative);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn histogram_f64_multithreaded() {
        // Run this test with stdout enabled to see output.
        // cargo test histogram_f64_multithreaded --features=testing -- --nocapture

        histogram_f64_multithreaded_aggregation_helper(Temporality::Delta);
        histogram_f64_multithreaded_aggregation_helper(Temporality::Cumulative);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn synchronous_instruments_cumulative_with_gap_in_measurements() {
        // Run this test with stdout enabled to see output.
        // cargo test synchronous_instruments_cumulative_with_gap_in_measurements --features=testing -- --nocapture

        synchronous_instruments_cumulative_with_gap_in_measurements_helper("counter");
        synchronous_instruments_cumulative_with_gap_in_measurements_helper("updown_counter");
        synchronous_instruments_cumulative_with_gap_in_measurements_helper("histogram");
        synchronous_instruments_cumulative_with_gap_in_measurements_helper("gauge");
    }

    fn synchronous_instruments_cumulative_with_gap_in_measurements_helper(
        instrument_name: &'static str,
    ) {
        let mut test_context = TestContext::new(Temporality::Cumulative);
        let attributes = &[KeyValue::new("key1", "value1")];

        // Create instrument and emit measurements
        match instrument_name {
            "counter" => {
                let counter = test_context.meter().u64_counter("test_counter").build();
                counter.add(5, &[]);
                counter.add(10, attributes);
            }
            "updown_counter" => {
                let updown_counter = test_context
                    .meter()
                    .i64_up_down_counter("test_updowncounter")
                    .build();
                updown_counter.add(15, &[]);
                updown_counter.add(20, attributes);
            }
            "histogram" => {
                let histogram = test_context.meter().u64_histogram("test_histogram").build();
                histogram.record(25, &[]);
                histogram.record(30, attributes);
            }
            "gauge" => {
                let gauge = test_context.meter().u64_gauge("test_gauge").build();
                gauge.record(35, &[]);
                gauge.record(40, attributes);
            }
            _ => panic!("Incorrect instrument kind provided"),
        };

        test_context.flush_metrics();

        // Test the first export
        assert_correct_export(&mut test_context, instrument_name);

        // Reset and export again without making any measurements
        test_context.reset_metrics();

        test_context.flush_metrics();

        // Test that latest export has the same data as the previous one
        assert_correct_export(&mut test_context, instrument_name);

        fn assert_correct_export(test_context: &mut TestContext, instrument_name: &'static str) {
            match instrument_name {
                "counter" => {
                    let MetricData::Sum(sum) =
                        test_context.get_aggregation::<u64>("test_counter", None)
                    else {
                        unreachable!()
                    };
                    assert_eq!(sum.data_points.len(), 2);
                    let zero_attribute_datapoint =
                        find_sum_datapoint_with_no_attributes(&sum.data_points)
                            .expect("datapoint with no attributes expected");
                    assert_eq!(zero_attribute_datapoint.value, 5);
                    let data_point1 =
                        find_sum_datapoint_with_key_value(&sum.data_points, "key1", "value1")
                            .expect("datapoint with key1=value1 expected");
                    assert_eq!(data_point1.value, 10);
                }
                "updown_counter" => {
                    let MetricData::Sum(sum) =
                        test_context.get_aggregation::<i64>("test_updowncounter", None)
                    else {
                        unreachable!()
                    };
                    assert_eq!(sum.data_points.len(), 2);
                    let zero_attribute_datapoint =
                        find_sum_datapoint_with_no_attributes(&sum.data_points)
                            .expect("datapoint with no attributes expected");
                    assert_eq!(zero_attribute_datapoint.value, 15);
                    let data_point1 =
                        find_sum_datapoint_with_key_value(&sum.data_points, "key1", "value1")
                            .expect("datapoint with key1=value1 expected");
                    assert_eq!(data_point1.value, 20);
                }
                "histogram" => {
                    let MetricData::Histogram(histogram_data) =
                        test_context.get_aggregation::<u64>("test_histogram", None)
                    else {
                        unreachable!()
                    };
                    assert_eq!(histogram_data.data_points.len(), 2);
                    let zero_attribute_datapoint =
                        find_histogram_datapoint_with_no_attributes(&histogram_data.data_points)
                            .expect("datapoint with no attributes expected");
                    assert_eq!(zero_attribute_datapoint.count, 1);
                    assert_eq!(zero_attribute_datapoint.sum, 25);
                    assert_eq!(zero_attribute_datapoint.min, Some(25));
                    assert_eq!(zero_attribute_datapoint.max, Some(25));
                    let data_point1 = find_histogram_datapoint_with_key_value(
                        &histogram_data.data_points,
                        "key1",
                        "value1",
                    )
                    .expect("datapoint with key1=value1 expected");
                    assert_eq!(data_point1.count, 1);
                    assert_eq!(data_point1.sum, 30);
                    assert_eq!(data_point1.min, Some(30));
                    assert_eq!(data_point1.max, Some(30));
                }
                "gauge" => {
                    let MetricData::Gauge(gauge_data) =
                        test_context.get_aggregation::<u64>("test_gauge", None)
                    else {
                        unreachable!()
                    };
                    assert_eq!(gauge_data.data_points.len(), 2);
                    let zero_attribute_datapoint =
                        find_gauge_datapoint_with_no_attributes(&gauge_data.data_points)
                            .expect("datapoint with no attributes expected");
                    assert_eq!(zero_attribute_datapoint.value, 35);
                    let data_point1 = find_gauge_datapoint_with_key_value(
                        &gauge_data.data_points,
                        "key1",
                        "value1",
                    )
                    .expect("datapoint with key1=value1 expected");
                    assert_eq!(data_point1.value, 40);
                }
                _ => panic!("Incorrect instrument kind provided"),
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn asynchronous_instruments_cumulative_data_points_only_from_last_measurement() {
        // Run this test with stdout enabled to see output.
        // cargo test asynchronous_instruments_cumulative_data_points_only_from_last_measurement --features=testing -- --nocapture

        asynchronous_instruments_cumulative_data_points_only_from_last_measurement_helper("gauge");
        asynchronous_instruments_cumulative_data_points_only_from_last_measurement_helper(
            "counter",
        );
        asynchronous_instruments_cumulative_data_points_only_from_last_measurement_helper(
            "updown_counter",
        );
    }

    #[test]
    fn view_test_rename() {
        test_view_customization(
            |i| {
                if i.name == "my_counter" {
                    Some(
                        Stream::builder()
                            .with_name("my_counter_renamed")
                            .build()
                            .unwrap(),
                    )
                } else {
                    None
                }
            },
            "my_counter_renamed",
            "my_unit",
            "my_description",
        )
    }

    #[test]
    fn view_test_change_unit() {
        test_view_customization(
            |i| {
                if i.name == "my_counter" {
                    Some(Stream::builder().with_unit("my_unit_new").build().unwrap())
                } else {
                    None
                }
            },
            "my_counter",
            "my_unit_new",
            "my_description",
        )
    }

    #[test]
    fn view_test_change_description() {
        test_view_customization(
            |i| {
                if i.name == "my_counter" {
                    Some(
                        Stream::builder()
                            .with_description("my_description_new")
                            .build()
                            .unwrap(),
                    )
                } else {
                    None
                }
            },
            "my_counter",
            "my_unit",
            "my_description_new",
        )
    }

    #[test]
    fn view_test_change_name_unit() {
        test_view_customization(
            |i| {
                if i.name == "my_counter" {
                    Some(
                        Stream::builder()
                            .with_name("my_counter_renamed")
                            .with_unit("my_unit_new")
                            .build()
                            .unwrap(),
                    )
                } else {
                    None
                }
            },
            "my_counter_renamed",
            "my_unit_new",
            "my_description",
        )
    }

    #[test]
    fn view_test_change_name_unit_desc() {
        test_view_customization(
            |i| {
                if i.name == "my_counter" {
                    Some(
                        Stream::builder()
                            .with_name("my_counter_renamed")
                            .with_unit("my_unit_new")
                            .with_description("my_description_new")
                            .build()
                            .unwrap(),
                    )
                } else {
                    None
                }
            },
            "my_counter_renamed",
            "my_unit_new",
            "my_description_new",
        )
    }

    #[test]
    fn view_test_match_unit() {
        test_view_customization(
            |i| {
                if i.unit == "my_unit" {
                    Some(Stream::builder().with_unit("my_unit_new").build().unwrap())
                } else {
                    None
                }
            },
            "my_counter",
            "my_unit_new",
            "my_description",
        )
    }

    #[test]
    fn view_test_match_none() {
        test_view_customization(
            |i| {
                if i.name == "not_expected_to_match" {
                    Some(Stream::builder().build().unwrap())
                } else {
                    None
                }
            },
            "my_counter",
            "my_unit",
            "my_description",
        )
    }

    #[test]
    fn view_test_match_multiple() {
        test_view_customization(
            |i| {
                if i.name == "my_counter" && i.unit == "my_unit" {
                    Some(
                        Stream::builder()
                            .with_name("my_counter_renamed")
                            .build()
                            .unwrap(),
                    )
                } else {
                    None
                }
            },
            "my_counter_renamed",
            "my_unit",
            "my_description",
        )
    }

    /// Helper function to test view customizations
    fn test_view_customization<F>(
        view_function: F,
        expected_name: &str,
        expected_unit: &str,
        expected_description: &str,
    ) where
        F: Fn(&Instrument) -> Option<Stream> + Send + Sync + 'static,
    {
        // Run this test with stdout enabled to see output.
        // cargo test view_test_* --all-features -- --nocapture

        // Arrange
        let exporter = InMemoryMetricExporter::default();
        let meter_provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .with_view(view_function)
            .build();

        // Act
        let meter = meter_provider.meter("test");
        let counter = meter
            .f64_counter("my_counter")
            .with_unit("my_unit")
            .with_description("my_description")
            .build();

        counter.add(1.5, &[KeyValue::new("key1", "value1")]);
        meter_provider.force_flush().unwrap();

        // Assert
        let resource_metrics = exporter
            .get_finished_metrics()
            .expect("metrics are expected to be exported.");
        assert_eq!(resource_metrics.len(), 1);
        assert_eq!(resource_metrics[0].scope_metrics.len(), 1);
        let metric = &resource_metrics[0].scope_metrics[0].metrics[0];
        assert_eq!(
            metric.name, expected_name,
            "Expected name: {expected_name}."
        );
        assert_eq!(
            metric.unit, expected_unit,
            "Expected unit: {expected_unit}."
        );
        assert_eq!(
            metric.description, expected_description,
            "Expected description: {expected_description}."
        );
    }

    // Following are just a basic set of advanced View tests - Views bring a lot
    // of permutations and combinations, and we need
    // to expand coverage for more scenarios in future.
    // It is best to first split this file into multiple files
    // based on scenarios (eg: regular aggregation, cardinality, views, view_advanced, etc)
    // and then add more tests for each of the scenarios.
    #[test]
    fn test_view_single_instrument_multiple_stream() {
        // Run this test with stdout enabled to see output.
        // cargo test test_view_multiple_stream --all-features

        // Each of the views match the instrument name "my_counter" and create a
        // new stream with a different name. In other words, View can be used to
        // create multiple streams for the same instrument.

        let view1 = |i: &Instrument| {
            if i.name() == "my_counter" {
                Some(Stream::builder().with_name("my_counter_1").build().unwrap())
            } else {
                None
            }
        };

        let view2 = |i: &Instrument| {
            if i.name() == "my_counter" {
                Some(Stream::builder().with_name("my_counter_2").build().unwrap())
            } else {
                None
            }
        };

        // Arrange
        let exporter = InMemoryMetricExporter::default();
        let meter_provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .with_view(view1)
            .with_view(view2)
            .build();

        // Act
        let meter = meter_provider.meter("test");
        let counter = meter.f64_counter("my_counter").build();

        counter.add(1.5, &[KeyValue::new("key1", "value1")]);
        meter_provider.force_flush().unwrap();

        // Assert
        let resource_metrics = exporter
            .get_finished_metrics()
            .expect("metrics are expected to be exported.");
        assert_eq!(resource_metrics.len(), 1);
        assert_eq!(resource_metrics[0].scope_metrics.len(), 1);
        let metrics = &resource_metrics[0].scope_metrics[0].metrics;
        assert_eq!(metrics.len(), 2);
        assert_eq!(metrics[0].name, "my_counter_1");
        assert_eq!(metrics[1].name, "my_counter_2");
    }

    #[test]
    fn test_view_multiple_instrument_single_stream() {
        // Run this test with stdout enabled to see output.
        // cargo test test_view_multiple_instrument_single_stream --all-features

        // The view matches the instrument name "my_counter1" and "my_counter1"
        // and create a single new stream for both. In other words, View can be used to
        // "merge" multiple instruments into a single stream.
        let view = |i: &Instrument| {
            if i.name() == "my_counter1" || i.name() == "my_counter2" {
                Some(Stream::builder().with_name("my_counter").build().unwrap())
            } else {
                None
            }
        };

        // Arrange
        let exporter = InMemoryMetricExporter::default();
        let meter_provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .with_view(view)
            .build();

        // Act
        let meter = meter_provider.meter("test");
        let counter1 = meter.f64_counter("my_counter1").build();
        let counter2 = meter.f64_counter("my_counter2").build();

        counter1.add(1.5, &[KeyValue::new("key1", "value1")]);
        counter2.add(1.5, &[KeyValue::new("key1", "value1")]);
        meter_provider.force_flush().unwrap();

        // Assert
        let resource_metrics = exporter
            .get_finished_metrics()
            .expect("metrics are expected to be exported.");
        assert_eq!(resource_metrics.len(), 1);
        assert_eq!(resource_metrics[0].scope_metrics.len(), 1);
        let metrics = &resource_metrics[0].scope_metrics[0].metrics;
        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics[0].name, "my_counter");
        // TODO: Assert that the data points are aggregated correctly.
    }

    fn asynchronous_instruments_cumulative_data_points_only_from_last_measurement_helper(
        instrument_name: &'static str,
    ) {
        let mut test_context = TestContext::new(Temporality::Cumulative);
        let attributes = Arc::new([KeyValue::new("key1", "value1")]);

        // Create instrument and emit measurements once
        match instrument_name {
            "counter" => {
                let has_run = AtomicBool::new(false);
                let _observable_counter = test_context
                    .meter()
                    .u64_observable_counter("test_counter")
                    .with_callback(move |observer| {
                        if !has_run.load(Ordering::SeqCst) {
                            observer.observe(5, &[]);
                            observer.observe(10, &*attributes.clone());
                            has_run.store(true, Ordering::SeqCst);
                        }
                    })
                    .build();
            }
            "updown_counter" => {
                let has_run = AtomicBool::new(false);
                let _observable_up_down_counter = test_context
                    .meter()
                    .i64_observable_up_down_counter("test_updowncounter")
                    .with_callback(move |observer| {
                        if !has_run.load(Ordering::SeqCst) {
                            observer.observe(15, &[]);
                            observer.observe(20, &*attributes.clone());
                            has_run.store(true, Ordering::SeqCst);
                        }
                    })
                    .build();
            }
            "gauge" => {
                let has_run = AtomicBool::new(false);
                let _observable_gauge = test_context
                    .meter()
                    .u64_observable_gauge("test_gauge")
                    .with_callback(move |observer| {
                        if !has_run.load(Ordering::SeqCst) {
                            observer.observe(25, &[]);
                            observer.observe(30, &*attributes.clone());
                            has_run.store(true, Ordering::SeqCst);
                        }
                    })
                    .build();
            }
            _ => panic!("Incorrect instrument kind provided"),
        };

        test_context.flush_metrics();

        // Test the first export
        assert_correct_export(&mut test_context, instrument_name);

        // Reset and export again without making any measurements
        test_context.reset_metrics();

        test_context.flush_metrics();

        test_context.check_no_metrics();

        fn assert_correct_export(test_context: &mut TestContext, instrument_name: &'static str) {
            match instrument_name {
                "counter" => {
                    let MetricData::Sum(sum) =
                        test_context.get_aggregation::<u64>("test_counter", None)
                    else {
                        unreachable!()
                    };
                    assert_eq!(sum.data_points.len(), 2);
                    assert!(sum.is_monotonic);
                    let zero_attribute_datapoint =
                        find_sum_datapoint_with_no_attributes(&sum.data_points)
                            .expect("datapoint with no attributes expected");
                    assert_eq!(zero_attribute_datapoint.value, 5);
                    let data_point1 =
                        find_sum_datapoint_with_key_value(&sum.data_points, "key1", "value1")
                            .expect("datapoint with key1=value1 expected");
                    assert_eq!(data_point1.value, 10);
                }
                "updown_counter" => {
                    let MetricData::Sum(sum) =
                        test_context.get_aggregation::<i64>("test_updowncounter", None)
                    else {
                        unreachable!()
                    };
                    assert_eq!(sum.data_points.len(), 2);
                    assert!(!sum.is_monotonic);
                    let zero_attribute_datapoint =
                        find_sum_datapoint_with_no_attributes(&sum.data_points)
                            .expect("datapoint with no attributes expected");
                    assert_eq!(zero_attribute_datapoint.value, 15);
                    let data_point1 =
                        find_sum_datapoint_with_key_value(&sum.data_points, "key1", "value1")
                            .expect("datapoint with key1=value1 expected");
                    assert_eq!(data_point1.value, 20);
                }
                "gauge" => {
                    let MetricData::Gauge(gauge_data) =
                        test_context.get_aggregation::<u64>("test_gauge", None)
                    else {
                        unreachable!()
                    };
                    assert_eq!(gauge_data.data_points.len(), 2);
                    let zero_attribute_datapoint =
                        find_gauge_datapoint_with_no_attributes(&gauge_data.data_points)
                            .expect("datapoint with no attributes expected");
                    assert_eq!(zero_attribute_datapoint.value, 25);
                    let data_point1 = find_gauge_datapoint_with_key_value(
                        &gauge_data.data_points,
                        "key1",
                        "value1",
                    )
                    .expect("datapoint with key1=value1 expected");
                    assert_eq!(data_point1.value, 30);
                }
                _ => panic!("Incorrect instrument kind provided"),
            }
        }
    }

    fn counter_multithreaded_aggregation_helper(temporality: Temporality) {
        // Arrange
        let mut test_context = TestContext::new(temporality);
        let counter = Arc::new(test_context.u64_counter("test", "my_counter", None));

        for i in 0..10 {
            thread::scope(|s| {
                s.spawn(|| {
                    counter.add(1, &[]);

                    counter.add(1, &[KeyValue::new("key1", "value1")]);
                    counter.add(1, &[KeyValue::new("key1", "value1")]);
                    counter.add(1, &[KeyValue::new("key1", "value1")]);

                    // Test concurrent collection by forcing half of the update threads to `force_flush` metrics and sleep for some time.
                    if i % 2 == 0 {
                        test_context.flush_metrics();
                        thread::sleep(Duration::from_millis(i)); // Make each thread sleep for some time duration for better testing
                    }

                    counter.add(1, &[KeyValue::new("key1", "value1")]);
                    counter.add(1, &[KeyValue::new("key1", "value1")]);
                });
            });
        }

        test_context.flush_metrics();

        // Assert
        // We invoke `test_context.flush_metrics()` six times.
        let sums = test_context
            .get_from_multiple_aggregations::<u64>("my_counter", None, 6)
            .into_iter()
            .map(|data| {
                if let MetricData::Sum(sum) = data {
                    sum
                } else {
                    unreachable!()
                }
            })
            .collect::<Vec<_>>();

        let mut sum_zero_attributes = 0;
        let mut sum_key1_value1 = 0;
        sums.iter().for_each(|sum| {
            assert_eq!(sum.data_points.len(), 2); // Expecting 1 time-series.
            assert!(sum.is_monotonic, "Counter should produce monotonic.");
            assert_eq!(sum.temporality, temporality);

            if temporality == Temporality::Delta {
                sum_zero_attributes += sum.data_points[0].value;
                sum_key1_value1 += sum.data_points[1].value;
            } else {
                sum_zero_attributes = sum.data_points[0].value;
                sum_key1_value1 = sum.data_points[1].value;
            };
        });

        assert_eq!(sum_zero_attributes, 10);
        assert_eq!(sum_key1_value1, 50); // Each of the 10 update threads record measurements summing up to 5.
    }

    fn counter_f64_multithreaded_aggregation_helper(temporality: Temporality) {
        // Arrange
        let mut test_context = TestContext::new(temporality);
        let counter = Arc::new(test_context.meter().f64_counter("test_counter").build());

        for i in 0..10 {
            thread::scope(|s| {
                s.spawn(|| {
                    counter.add(1.23, &[]);

                    counter.add(1.23, &[KeyValue::new("key1", "value1")]);
                    counter.add(1.23, &[KeyValue::new("key1", "value1")]);
                    counter.add(1.23, &[KeyValue::new("key1", "value1")]);

                    // Test concurrent collection by forcing half of the update threads to `force_flush` metrics and sleep for some time.
                    if i % 2 == 0 {
                        test_context.flush_metrics();
                        thread::sleep(Duration::from_millis(i)); // Make each thread sleep for some time duration for better testing
                    }

                    counter.add(1.23, &[KeyValue::new("key1", "value1")]);
                    counter.add(1.23, &[KeyValue::new("key1", "value1")]);
                });
            });
        }

        test_context.flush_metrics();

        // Assert
        // We invoke `test_context.flush_metrics()` six times.
        let sums = test_context
            .get_from_multiple_aggregations::<f64>("test_counter", None, 6)
            .into_iter()
            .map(|data| {
                if let MetricData::Sum(sum) = data {
                    sum
                } else {
                    unreachable!()
                }
            })
            .collect::<Vec<_>>();

        let mut sum_zero_attributes = 0.0;
        let mut sum_key1_value1 = 0.0;
        sums.iter().for_each(|sum| {
            assert_eq!(sum.data_points.len(), 2); // Expecting 1 time-series.
            assert!(sum.is_monotonic, "Counter should produce monotonic.");
            assert_eq!(sum.temporality, temporality);

            if temporality == Temporality::Delta {
                sum_zero_attributes += sum.data_points[0].value;
                sum_key1_value1 += sum.data_points[1].value;
            } else {
                sum_zero_attributes = sum.data_points[0].value;
                sum_key1_value1 = sum.data_points[1].value;
            };
        });

        assert!(f64::abs(12.3 - sum_zero_attributes) < 0.0001);
        assert!(f64::abs(61.5 - sum_key1_value1) < 0.0001); // Each of the 10 update threads record measurements 5 times = 10 * 5 * 1.23 = 61.5
    }

    fn histogram_multithreaded_aggregation_helper(temporality: Temporality) {
        // Arrange
        let mut test_context = TestContext::new(temporality);
        let histogram = Arc::new(test_context.meter().u64_histogram("test_histogram").build());

        for i in 0..10 {
            thread::scope(|s| {
                s.spawn(|| {
                    histogram.record(1, &[]);
                    histogram.record(4, &[]);

                    histogram.record(5, &[KeyValue::new("key1", "value1")]);
                    histogram.record(7, &[KeyValue::new("key1", "value1")]);
                    histogram.record(18, &[KeyValue::new("key1", "value1")]);

                    // Test concurrent collection by forcing half of the update threads to `force_flush` metrics and sleep for some time.
                    if i % 2 == 0 {
                        test_context.flush_metrics();
                        thread::sleep(Duration::from_millis(i)); // Make each thread sleep for some time duration for better testing
                    }

                    histogram.record(35, &[KeyValue::new("key1", "value1")]);
                    histogram.record(35, &[KeyValue::new("key1", "value1")]);
                });
            });
        }

        test_context.flush_metrics();

        // Assert
        // We invoke `test_context.flush_metrics()` six times.
        let histograms = test_context
            .get_from_multiple_aggregations::<u64>("test_histogram", None, 6)
            .into_iter()
            .map(|data| {
                if let MetricData::Histogram(hist) = data {
                    hist
                } else {
                    unreachable!()
                }
            })
            .collect::<Vec<_>>();

        let (
            mut sum_zero_attributes,
            mut count_zero_attributes,
            mut min_zero_attributes,
            mut max_zero_attributes,
        ) = (0, 0, u64::MAX, u64::MIN);
        let (mut sum_key1_value1, mut count_key1_value1, mut min_key1_value1, mut max_key1_value1) =
            (0, 0, u64::MAX, u64::MIN);

        let mut bucket_counts_zero_attributes = vec![0; 16]; // There are 16 buckets for the default configuration
        let mut bucket_counts_key1_value1 = vec![0; 16];

        histograms.iter().for_each(|histogram| {
            assert_eq!(histogram.data_points.len(), 2); // Expecting 1 time-series.
            assert_eq!(histogram.temporality, temporality);

            let data_point_zero_attributes =
                find_histogram_datapoint_with_no_attributes(&histogram.data_points).unwrap();
            let data_point_key1_value1 =
                find_histogram_datapoint_with_key_value(&histogram.data_points, "key1", "value1")
                    .unwrap();

            if temporality == Temporality::Delta {
                sum_zero_attributes += data_point_zero_attributes.sum;
                sum_key1_value1 += data_point_key1_value1.sum;

                count_zero_attributes += data_point_zero_attributes.count;
                count_key1_value1 += data_point_key1_value1.count;

                min_zero_attributes =
                    min(min_zero_attributes, data_point_zero_attributes.min.unwrap());
                min_key1_value1 = min(min_key1_value1, data_point_key1_value1.min.unwrap());

                max_zero_attributes =
                    max(max_zero_attributes, data_point_zero_attributes.max.unwrap());
                max_key1_value1 = max(max_key1_value1, data_point_key1_value1.max.unwrap());

                assert_eq!(data_point_zero_attributes.bucket_counts.len(), 16);
                assert_eq!(data_point_key1_value1.bucket_counts.len(), 16);

                for (i, _) in data_point_zero_attributes.bucket_counts.iter().enumerate() {
                    bucket_counts_zero_attributes[i] += data_point_zero_attributes.bucket_counts[i];
                }

                for (i, _) in data_point_key1_value1.bucket_counts.iter().enumerate() {
                    bucket_counts_key1_value1[i] += data_point_key1_value1.bucket_counts[i];
                }
            } else {
                sum_zero_attributes = data_point_zero_attributes.sum;
                sum_key1_value1 = data_point_key1_value1.sum;

                count_zero_attributes = data_point_zero_attributes.count;
                count_key1_value1 = data_point_key1_value1.count;

                min_zero_attributes = data_point_zero_attributes.min.unwrap();
                min_key1_value1 = data_point_key1_value1.min.unwrap();

                max_zero_attributes = data_point_zero_attributes.max.unwrap();
                max_key1_value1 = data_point_key1_value1.max.unwrap();

                assert_eq!(data_point_zero_attributes.bucket_counts.len(), 16);
                assert_eq!(data_point_key1_value1.bucket_counts.len(), 16);

                bucket_counts_zero_attributes.clone_from(&data_point_zero_attributes.bucket_counts);
                bucket_counts_key1_value1.clone_from(&data_point_key1_value1.bucket_counts);
            };
        });

        // Default buckets:
        // (-∞, 0], (0, 5.0], (5.0, 10.0], (10.0, 25.0], (25.0, 50.0], (50.0, 75.0], (75.0, 100.0], (100.0, 250.0], (250.0, 500.0],
        // (500.0, 750.0], (750.0, 1000.0], (1000.0, 2500.0], (2500.0, 5000.0], (5000.0, 7500.0], (7500.0, 10000.0], (10000.0, +∞).

        assert_eq!(count_zero_attributes, 20); // Each of the 10 update threads record two measurements.
        assert_eq!(sum_zero_attributes, 50); // Each of the 10 update threads record measurements summing up to 5.
        assert_eq!(min_zero_attributes, 1);
        assert_eq!(max_zero_attributes, 4);

        for (i, count) in bucket_counts_zero_attributes.iter().enumerate() {
            match i {
                1 => assert_eq!(*count, 20), // For each of the 10 update threads, both the recorded values 1 and 4 fall under the bucket (0, 5].
                _ => assert_eq!(*count, 0),
            }
        }

        assert_eq!(count_key1_value1, 50); // Each of the 10 update threads record 5 measurements.
        assert_eq!(sum_key1_value1, 1000); // Each of the 10 update threads record measurements summing up to 100 (5 + 7 + 18 + 35 + 35).
        assert_eq!(min_key1_value1, 5);
        assert_eq!(max_key1_value1, 35);

        for (i, count) in bucket_counts_key1_value1.iter().enumerate() {
            match i {
                1 => assert_eq!(*count, 10), // For each of the 10 update threads, the recorded value 5 falls under the bucket (0, 5].
                2 => assert_eq!(*count, 10), // For each of the 10 update threads, the recorded value 7 falls under the bucket (5, 10].
                3 => assert_eq!(*count, 10), // For each of the 10 update threads, the recorded value 18 falls under the bucket (10, 25].
                4 => assert_eq!(*count, 20), // For each of the 10 update threads, the recorded value 35 (recorded twice) falls under the bucket (25, 50].
                _ => assert_eq!(*count, 0),
            }
        }
    }

    fn histogram_f64_multithreaded_aggregation_helper(temporality: Temporality) {
        // Arrange
        let mut test_context = TestContext::new(temporality);
        let histogram = Arc::new(test_context.meter().f64_histogram("test_histogram").build());

        for i in 0..10 {
            thread::scope(|s| {
                s.spawn(|| {
                    histogram.record(1.5, &[]);
                    histogram.record(4.6, &[]);

                    histogram.record(5.0, &[KeyValue::new("key1", "value1")]);
                    histogram.record(7.3, &[KeyValue::new("key1", "value1")]);
                    histogram.record(18.1, &[KeyValue::new("key1", "value1")]);

                    // Test concurrent collection by forcing half of the update threads to `force_flush` metrics and sleep for some time.
                    if i % 2 == 0 {
                        test_context.flush_metrics();
                        thread::sleep(Duration::from_millis(i)); // Make each thread sleep for some time duration for better testing
                    }

                    histogram.record(35.1, &[KeyValue::new("key1", "value1")]);
                    histogram.record(35.1, &[KeyValue::new("key1", "value1")]);
                });
            });
        }

        test_context.flush_metrics();

        // Assert
        // We invoke `test_context.flush_metrics()` six times.
        let histograms = test_context
            .get_from_multiple_aggregations::<f64>("test_histogram", None, 6)
            .into_iter()
            .map(|data| {
                if let MetricData::Histogram(hist) = data {
                    hist
                } else {
                    unreachable!()
                }
            })
            .collect::<Vec<_>>();

        let (
            mut sum_zero_attributes,
            mut count_zero_attributes,
            mut min_zero_attributes,
            mut max_zero_attributes,
        ) = (0.0, 0, f64::MAX, f64::MIN);
        let (mut sum_key1_value1, mut count_key1_value1, mut min_key1_value1, mut max_key1_value1) =
            (0.0, 0, f64::MAX, f64::MIN);

        let mut bucket_counts_zero_attributes = vec![0; 16]; // There are 16 buckets for the default configuration
        let mut bucket_counts_key1_value1 = vec![0; 16];

        histograms.iter().for_each(|histogram| {
            assert_eq!(histogram.data_points.len(), 2); // Expecting 1 time-series.
            assert_eq!(histogram.temporality, temporality);

            let data_point_zero_attributes =
                find_histogram_datapoint_with_no_attributes(&histogram.data_points).unwrap();
            let data_point_key1_value1 =
                find_histogram_datapoint_with_key_value(&histogram.data_points, "key1", "value1")
                    .unwrap();

            if temporality == Temporality::Delta {
                sum_zero_attributes += data_point_zero_attributes.sum;
                sum_key1_value1 += data_point_key1_value1.sum;

                count_zero_attributes += data_point_zero_attributes.count;
                count_key1_value1 += data_point_key1_value1.count;

                min_zero_attributes =
                    min_zero_attributes.min(data_point_zero_attributes.min.unwrap());
                min_key1_value1 = min_key1_value1.min(data_point_key1_value1.min.unwrap());

                max_zero_attributes =
                    max_zero_attributes.max(data_point_zero_attributes.max.unwrap());
                max_key1_value1 = max_key1_value1.max(data_point_key1_value1.max.unwrap());

                assert_eq!(data_point_zero_attributes.bucket_counts.len(), 16);
                assert_eq!(data_point_key1_value1.bucket_counts.len(), 16);

                for (i, _) in data_point_zero_attributes.bucket_counts.iter().enumerate() {
                    bucket_counts_zero_attributes[i] += data_point_zero_attributes.bucket_counts[i];
                }

                for (i, _) in data_point_key1_value1.bucket_counts.iter().enumerate() {
                    bucket_counts_key1_value1[i] += data_point_key1_value1.bucket_counts[i];
                }
            } else {
                sum_zero_attributes = data_point_zero_attributes.sum;
                sum_key1_value1 = data_point_key1_value1.sum;

                count_zero_attributes = data_point_zero_attributes.count;
                count_key1_value1 = data_point_key1_value1.count;

                min_zero_attributes = data_point_zero_attributes.min.unwrap();
                min_key1_value1 = data_point_key1_value1.min.unwrap();

                max_zero_attributes = data_point_zero_attributes.max.unwrap();
                max_key1_value1 = data_point_key1_value1.max.unwrap();

                assert_eq!(data_point_zero_attributes.bucket_counts.len(), 16);
                assert_eq!(data_point_key1_value1.bucket_counts.len(), 16);

                bucket_counts_zero_attributes.clone_from(&data_point_zero_attributes.bucket_counts);
                bucket_counts_key1_value1.clone_from(&data_point_key1_value1.bucket_counts);
            };
        });

        // Default buckets:
        // (-∞, 0], (0, 5.0], (5.0, 10.0], (10.0, 25.0], (25.0, 50.0], (50.0, 75.0], (75.0, 100.0], (100.0, 250.0], (250.0, 500.0],
        // (500.0, 750.0], (750.0, 1000.0], (1000.0, 2500.0], (2500.0, 5000.0], (5000.0, 7500.0], (7500.0, 10000.0], (10000.0, +∞).

        assert_eq!(count_zero_attributes, 20); // Each of the 10 update threads record two measurements.
        assert!(f64::abs(61.0 - sum_zero_attributes) < 0.0001); // Each of the 10 update threads record measurements summing up to 6.1 (1.5 + 4.6)
        assert_eq!(min_zero_attributes, 1.5);
        assert_eq!(max_zero_attributes, 4.6);

        for (i, count) in bucket_counts_zero_attributes.iter().enumerate() {
            match i {
                1 => assert_eq!(*count, 20), // For each of the 10 update threads, both the recorded values 1.5 and 4.6 fall under the bucket (0, 5.0].
                _ => assert_eq!(*count, 0),
            }
        }

        assert_eq!(count_key1_value1, 50); // Each of the 10 update threads record 5 measurements.
        assert!(f64::abs(1006.0 - sum_key1_value1) < 0.0001); // Each of the 10 update threads record measurements summing up to 100.4 (5.0 + 7.3 + 18.1 + 35.1 + 35.1).
        assert_eq!(min_key1_value1, 5.0);
        assert_eq!(max_key1_value1, 35.1);

        for (i, count) in bucket_counts_key1_value1.iter().enumerate() {
            match i {
                1 => assert_eq!(*count, 10), // For each of the 10 update threads, the recorded value 5.0 falls under the bucket (0, 5.0].
                2 => assert_eq!(*count, 10), // For each of the 10 update threads, the recorded value 7.3 falls under the bucket (5.0, 10.0].
                3 => assert_eq!(*count, 10), // For each of the 10 update threads, the recorded value 18.1 falls under the bucket (10.0, 25.0].
                4 => assert_eq!(*count, 20), // For each of the 10 update threads, the recorded value 35.1 (recorded twice) falls under the bucket (25.0, 50.0].
                _ => assert_eq!(*count, 0),
            }
        }
    }

    fn histogram_aggregation_helper(temporality: Temporality) {
        // Arrange
        let mut test_context = TestContext::new(temporality);
        let histogram = test_context.meter().u64_histogram("my_histogram").build();

        // Act
        let mut rand = rngs::SmallRng::from_os_rng();
        let values_kv1 = (0..50)
            .map(|_| rand.random_range(0..100))
            .collect::<Vec<u64>>();
        for value in values_kv1.iter() {
            histogram.record(*value, &[KeyValue::new("key1", "value1")]);
        }

        let values_kv2 = (0..30)
            .map(|_| rand.random_range(0..100))
            .collect::<Vec<u64>>();
        for value in values_kv2.iter() {
            histogram.record(*value, &[KeyValue::new("key1", "value2")]);
        }

        test_context.flush_metrics();

        // Assert
        let MetricData::Histogram(histogram_data) =
            test_context.get_aggregation::<u64>("my_histogram", None)
        else {
            unreachable!()
        };
        // Expecting 2 time-series.
        assert_eq!(histogram_data.data_points.len(), 2);
        if let Temporality::Cumulative = temporality {
            assert_eq!(
                histogram_data.temporality,
                Temporality::Cumulative,
                "Should produce cumulative"
            );
        } else {
            assert_eq!(
                histogram_data.temporality,
                Temporality::Delta,
                "Should produce delta"
            );
        }

        // find and validate key1=value2 datapoint
        let data_point1 =
            find_histogram_datapoint_with_key_value(&histogram_data.data_points, "key1", "value1")
                .expect("datapoint with key1=value1 expected");
        assert_eq!(data_point1.count, values_kv1.len() as u64);
        assert_eq!(data_point1.sum, values_kv1.iter().sum::<u64>());
        assert_eq!(data_point1.min.unwrap(), *values_kv1.iter().min().unwrap());
        assert_eq!(data_point1.max.unwrap(), *values_kv1.iter().max().unwrap());

        let data_point2 =
            find_histogram_datapoint_with_key_value(&histogram_data.data_points, "key1", "value2")
                .expect("datapoint with key1=value2 expected");
        assert_eq!(data_point2.count, values_kv2.len() as u64);
        assert_eq!(data_point2.sum, values_kv2.iter().sum::<u64>());
        assert_eq!(data_point2.min.unwrap(), *values_kv2.iter().min().unwrap());
        assert_eq!(data_point2.max.unwrap(), *values_kv2.iter().max().unwrap());

        // Reset and report more measurements
        test_context.reset_metrics();
        for value in values_kv1.iter() {
            histogram.record(*value, &[KeyValue::new("key1", "value1")]);
        }

        for value in values_kv2.iter() {
            histogram.record(*value, &[KeyValue::new("key1", "value2")]);
        }

        test_context.flush_metrics();

        let MetricData::Histogram(histogram_data) =
            test_context.get_aggregation::<u64>("my_histogram", None)
        else {
            unreachable!()
        };
        assert_eq!(histogram_data.data_points.len(), 2);
        let data_point1 =
            find_histogram_datapoint_with_key_value(&histogram_data.data_points, "key1", "value1")
                .expect("datapoint with key1=value1 expected");
        if temporality == Temporality::Cumulative {
            assert_eq!(data_point1.count, 2 * (values_kv1.len() as u64));
            assert_eq!(data_point1.sum, 2 * (values_kv1.iter().sum::<u64>()));
            assert_eq!(data_point1.min.unwrap(), *values_kv1.iter().min().unwrap());
            assert_eq!(data_point1.max.unwrap(), *values_kv1.iter().max().unwrap());
        } else {
            assert_eq!(data_point1.count, values_kv1.len() as u64);
            assert_eq!(data_point1.sum, values_kv1.iter().sum::<u64>());
            assert_eq!(data_point1.min.unwrap(), *values_kv1.iter().min().unwrap());
            assert_eq!(data_point1.max.unwrap(), *values_kv1.iter().max().unwrap());
        }

        let data_point1 =
            find_histogram_datapoint_with_key_value(&histogram_data.data_points, "key1", "value2")
                .expect("datapoint with key1=value1 expected");
        if temporality == Temporality::Cumulative {
            assert_eq!(data_point1.count, 2 * (values_kv2.len() as u64));
            assert_eq!(data_point1.sum, 2 * (values_kv2.iter().sum::<u64>()));
            assert_eq!(data_point1.min.unwrap(), *values_kv2.iter().min().unwrap());
            assert_eq!(data_point1.max.unwrap(), *values_kv2.iter().max().unwrap());
        } else {
            assert_eq!(data_point1.count, values_kv2.len() as u64);
            assert_eq!(data_point1.sum, values_kv2.iter().sum::<u64>());
            assert_eq!(data_point1.min.unwrap(), *values_kv2.iter().min().unwrap());
            assert_eq!(data_point1.max.unwrap(), *values_kv2.iter().max().unwrap());
        }
    }

    fn histogram_aggregation_with_custom_bounds_helper(temporality: Temporality) {
        let mut test_context = TestContext::new(temporality);
        let histogram = test_context
            .meter()
            .u64_histogram("test_histogram")
            .with_boundaries(vec![1.0, 2.5, 5.5])
            .build();
        histogram.record(1, &[KeyValue::new("key1", "value1")]);
        histogram.record(2, &[KeyValue::new("key1", "value1")]);
        histogram.record(3, &[KeyValue::new("key1", "value1")]);
        histogram.record(4, &[KeyValue::new("key1", "value1")]);
        histogram.record(5, &[KeyValue::new("key1", "value1")]);

        test_context.flush_metrics();

        // Assert
        let MetricData::Histogram(histogram_data) =
            test_context.get_aggregation::<u64>("test_histogram", None)
        else {
            unreachable!()
        };
        // Expecting 2 time-series.
        assert_eq!(histogram_data.data_points.len(), 1);
        if let Temporality::Cumulative = temporality {
            assert_eq!(
                histogram_data.temporality,
                Temporality::Cumulative,
                "Should produce cumulative"
            );
        } else {
            assert_eq!(
                histogram_data.temporality,
                Temporality::Delta,
                "Should produce delta"
            );
        }

        // find and validate key1=value1 datapoint
        let data_point =
            find_histogram_datapoint_with_key_value(&histogram_data.data_points, "key1", "value1")
                .expect("datapoint with key1=value1 expected");

        assert_eq!(data_point.count, 5);
        assert_eq!(data_point.sum, 15);

        // Check the bucket counts
        // -∞ to 1.0: 1
        // 1.0 to 2.5: 1
        // 2.5 to 5.5: 3
        // 5.5 to +∞: 0

        assert_eq!(vec![1.0, 2.5, 5.5], data_point.bounds);
        assert_eq!(vec![1, 1, 3, 0], data_point.bucket_counts);
    }

    fn histogram_aggregation_with_empty_bounds_helper(temporality: Temporality) {
        let mut test_context = TestContext::new(temporality);
        let histogram = test_context
            .meter()
            .u64_histogram("test_histogram")
            .with_boundaries(vec![])
            .build();
        histogram.record(1, &[KeyValue::new("key1", "value1")]);
        histogram.record(2, &[KeyValue::new("key1", "value1")]);
        histogram.record(3, &[KeyValue::new("key1", "value1")]);
        histogram.record(4, &[KeyValue::new("key1", "value1")]);
        histogram.record(5, &[KeyValue::new("key1", "value1")]);

        test_context.flush_metrics();

        // Assert
        let MetricData::Histogram(histogram_data) =
            test_context.get_aggregation::<u64>("test_histogram", None)
        else {
            unreachable!()
        };
        // Expecting 1 time-series.
        assert_eq!(histogram_data.data_points.len(), 1);
        if let Temporality::Cumulative = temporality {
            assert_eq!(
                histogram_data.temporality,
                Temporality::Cumulative,
                "Should produce cumulative"
            );
        } else {
            assert_eq!(
                histogram_data.temporality,
                Temporality::Delta,
                "Should produce delta"
            );
        }

        // find and validate key1=value1 datapoint
        let data_point =
            find_histogram_datapoint_with_key_value(&histogram_data.data_points, "key1", "value1")
                .expect("datapoint with key1=value1 expected");

        assert_eq!(data_point.count, 5);
        assert_eq!(data_point.sum, 15);
        assert!(data_point.bounds.is_empty());
        assert!(data_point.bucket_counts.is_empty());
    }

    fn histogram_aggregation_with_custom_bounds_and_view_helper(temporality: Temporality) {
        for specify_boundaries_in_view in [false, true] {
            let view = move |_: &Instrument| {
                let mut builder = Stream::builder();
                if specify_boundaries_in_view {
                    builder = builder.with_aggregation(Aggregation::ExplicitBucketHistogram {
                        boundaries: vec![1.5, 4.2, 6.7],
                        record_min_max: true,
                    });
                }
                Some(builder.build().unwrap())
            };
            let mut test_context = TestContext::new_with_view(temporality, view);
            let histogram = test_context
                .meter()
                .u64_histogram("test_histogram")
                .with_boundaries(vec![1.0, 2.5, 5.5])
                .build();
            histogram.record(1, &[KeyValue::new("key1", "value1")]);
            histogram.record(2, &[KeyValue::new("key1", "value1")]);
            histogram.record(3, &[KeyValue::new("key1", "value1")]);
            histogram.record(4, &[KeyValue::new("key1", "value1")]);
            histogram.record(5, &[KeyValue::new("key1", "value1")]);

            test_context.flush_metrics();

            let MetricData::Histogram(histogram_data) =
                test_context.get_aggregation::<u64>("test_histogram", None)
            else {
                unreachable!()
            };
            assert_eq!(histogram_data.data_points.len(), 1);
            if let Temporality::Cumulative = temporality {
                assert_eq!(
                    histogram_data.temporality,
                    Temporality::Cumulative,
                    "Should produce cumulative"
                );
            } else {
                assert_eq!(
                    histogram_data.temporality,
                    Temporality::Delta,
                    "Should produce delta"
                );
            }

            // find and validate key1=value1 datapoint
            let data_point = find_histogram_datapoint_with_key_value(
                &histogram_data.data_points,
                "key1",
                "value1",
            )
            .expect("datapoint with key1=value1 expected");

            assert_eq!(data_point.count, 5);
            assert_eq!(data_point.sum, 15);

            // Check the bucket counts
            if specify_boundaries_in_view {
                // If boundaries are specified in the view, they should take precedence
                assert_eq!(vec![1.5, 4.2, 6.7], data_point.bounds);
                assert_eq!(vec![1, 3, 1, 0], data_point.bucket_counts);
            } else {
                // If boundaries are not specified in the view, the ones from the instrument
                // should be used
                assert_eq!(vec![1.0, 2.5, 5.5], data_point.bounds);
                assert_eq!(vec![1, 1, 3, 0], data_point.bucket_counts);
            }
        }
    }

    fn exponential_histogram_aggregation_with_view_helper(temporality: Temporality) {
        // Arrange: Create a view that converts a regular histogram to Base2ExponentialHistogram
        let view = |i: &Instrument| {
            if i.name == "test_histogram" {
                Some(
                    Stream::builder()
                        .with_aggregation(Aggregation::Base2ExponentialHistogram {
                            max_size: 160,
                            max_scale: 20,
                            record_min_max: true,
                        })
                        .build()
                        .unwrap(),
                )
            } else {
                None
            }
        };
        let mut test_context = TestContext::new_with_view(temporality, view);
        let histogram = test_context.meter().f64_histogram("test_histogram").build();

        // Act: Record some values
        histogram.record(1.0, &[KeyValue::new("key1", "value1")]);
        histogram.record(2.0, &[KeyValue::new("key1", "value1")]);
        histogram.record(3.0, &[KeyValue::new("key1", "value1")]);
        histogram.record(4.0, &[KeyValue::new("key1", "value1")]);
        histogram.record(5.0, &[KeyValue::new("key1", "value1")]);

        test_context.flush_metrics();

        // Assert: Verify we get an ExponentialHistogram instead of regular Histogram
        let exponential_histogram_data =
            test_context.get_aggregation::<f64>("test_histogram", None);
        let MetricData::ExponentialHistogram(exp_hist) = exponential_histogram_data else {
            panic!(
                "Expected ExponentialHistogram aggregation, got {:?}",
                exponential_histogram_data
            );
        };

        assert_eq!(exp_hist.data_points.len(), 1);
        if let Temporality::Cumulative = temporality {
            assert_eq!(
                exp_hist.temporality,
                Temporality::Cumulative,
                "Should produce cumulative"
            );
        } else {
            assert_eq!(
                exp_hist.temporality,
                Temporality::Delta,
                "Should produce delta"
            );
        }

        // Validate the data point
        let data_point = &exp_hist.data_points[0];
        assert_eq!(data_point.count(), 5);
        assert_eq!(data_point.sum(), 15.0);
        assert_eq!(data_point.min(), Some(1.0));
        assert_eq!(data_point.max(), Some(5.0));

        // Validate exponential histogram specific fields
        // Scale should be within valid range (-10 to 20)
        let scale = data_point.scale();
        assert!(
            (-10..=20).contains(&scale),
            "Scale {} should be within valid range [-10, 20]",
            scale
        );

        // zero_count should be 0 since we only recorded positive values > 0
        assert_eq!(
            data_point.zero_count(),
            0,
            "zero_count should be 0 for positive values"
        );

        // Positive bucket should have counts (we recorded positive values)
        let positive_bucket = data_point.positive_bucket();
        let positive_counts: Vec<u64> = positive_bucket.counts().collect();
        let total_positive_count: u64 = positive_counts.iter().sum();
        assert_eq!(
            total_positive_count, 5,
            "Total count in positive buckets should equal number of recorded values"
        );

        // Negative bucket should be empty (we only recorded positive values)
        let negative_bucket = data_point.negative_bucket();
        let negative_counts: Vec<u64> = negative_bucket.counts().collect();
        let total_negative_count: u64 = negative_counts.iter().sum();
        assert_eq!(
            total_negative_count, 0,
            "Negative bucket should be empty for positive-only values"
        );

        // Verify the attribute is present
        let attrs: Vec<_> = data_point.attributes().collect();
        assert_eq!(attrs.len(), 1);
        assert_eq!(attrs[0].key.as_str(), "key1");

        // Reset and report more measurements to verify Delta vs Cumulative behavior
        test_context.reset_metrics();
        histogram.record(10.0, &[KeyValue::new("key1", "value1")]);
        histogram.record(20.0, &[KeyValue::new("key1", "value1")]);
        histogram.record(30.0, &[KeyValue::new("key1", "value1")]);

        test_context.flush_metrics();

        // Assert second collect
        let exponential_histogram_data =
            test_context.get_aggregation::<f64>("test_histogram", None);
        let MetricData::ExponentialHistogram(exp_hist) = exponential_histogram_data else {
            panic!(
                "Expected ExponentialHistogram aggregation, got {:?}",
                exponential_histogram_data
            );
        };

        assert_eq!(exp_hist.data_points.len(), 1);
        let data_point = &exp_hist.data_points[0];

        if temporality == Temporality::Cumulative {
            // Cumulative: values accumulate (5 original + 3 new = 8 count, 15 + 60 = 75 sum)
            assert_eq!(data_point.count(), 8);
            assert_eq!(data_point.sum(), 75.0);
            assert_eq!(data_point.min(), Some(1.0)); // min from first batch
            assert_eq!(data_point.max(), Some(30.0)); // max from second batch
        } else {
            // Delta: only new values (3 count, 60 sum)
            assert_eq!(data_point.count(), 3);
            assert_eq!(data_point.sum(), 60.0);
            assert_eq!(data_point.min(), Some(10.0));
            assert_eq!(data_point.max(), Some(30.0));
        }

        // Verify positive bucket counts match the count
        let positive_bucket = data_point.positive_bucket();
        let positive_counts: Vec<u64> = positive_bucket.counts().collect();
        let total_positive_count: u64 = positive_counts.iter().sum();
        assert_eq!(
            total_positive_count,
            data_point.count() as u64,
            "Total count in positive buckets should equal count"
        );
    }

    fn gauge_aggregation_helper(temporality: Temporality) {
        // Arrange
        let mut test_context = TestContext::new(temporality);
        let gauge = test_context.meter().i64_gauge("my_gauge").build();

        // Act
        gauge.record(1, &[KeyValue::new("key1", "value1")]);
        gauge.record(2, &[KeyValue::new("key1", "value1")]);
        gauge.record(1, &[KeyValue::new("key1", "value1")]);
        gauge.record(3, &[KeyValue::new("key1", "value1")]);
        gauge.record(4, &[KeyValue::new("key1", "value1")]);

        gauge.record(11, &[KeyValue::new("key1", "value2")]);
        gauge.record(13, &[KeyValue::new("key1", "value2")]);
        gauge.record(6, &[KeyValue::new("key1", "value2")]);

        test_context.flush_metrics();

        // Assert
        let MetricData::Gauge(gauge_data_point) =
            test_context.get_aggregation::<i64>("my_gauge", None)
        else {
            unreachable!()
        };
        // Expecting 2 time-series.
        assert_eq!(gauge_data_point.data_points.len(), 2);

        // find and validate key1=value2 datapoint
        let data_point1 =
            find_gauge_datapoint_with_key_value(&gauge_data_point.data_points, "key1", "value1")
                .expect("datapoint with key1=value1 expected");
        assert_eq!(data_point1.value, 4);

        let data_point1 =
            find_gauge_datapoint_with_key_value(&gauge_data_point.data_points, "key1", "value2")
                .expect("datapoint with key1=value2 expected");
        assert_eq!(data_point1.value, 6);

        // Reset and report more measurements
        test_context.reset_metrics();
        gauge.record(1, &[KeyValue::new("key1", "value1")]);
        gauge.record(2, &[KeyValue::new("key1", "value1")]);
        gauge.record(11, &[KeyValue::new("key1", "value1")]);
        gauge.record(3, &[KeyValue::new("key1", "value1")]);
        gauge.record(41, &[KeyValue::new("key1", "value1")]);

        gauge.record(34, &[KeyValue::new("key1", "value2")]);
        gauge.record(12, &[KeyValue::new("key1", "value2")]);
        gauge.record(54, &[KeyValue::new("key1", "value2")]);

        test_context.flush_metrics();

        let MetricData::Gauge(gauge) = test_context.get_aggregation::<i64>("my_gauge", None) else {
            unreachable!()
        };
        assert_eq!(gauge.data_points.len(), 2);
        let data_point1 = find_gauge_datapoint_with_key_value(&gauge.data_points, "key1", "value1")
            .expect("datapoint with key1=value1 expected");
        assert_eq!(data_point1.value, 41);

        let data_point1 = find_gauge_datapoint_with_key_value(&gauge.data_points, "key1", "value2")
            .expect("datapoint with key1=value2 expected");
        assert_eq!(data_point1.value, 54);
    }

    fn observable_gauge_aggregation_helper(temporality: Temporality, use_empty_attributes: bool) {
        // Arrange
        let mut test_context = TestContext::new(temporality);
        let _observable_gauge = test_context
            .meter()
            .i64_observable_gauge("test_observable_gauge")
            .with_callback(move |observer| {
                if use_empty_attributes {
                    observer.observe(1, &[]);
                }
                observer.observe(4, &[KeyValue::new("key1", "value1")]);
                observer.observe(5, &[KeyValue::new("key2", "value2")]);
            })
            .build();

        test_context.flush_metrics();

        // Assert
        let MetricData::Gauge(gauge) =
            test_context.get_aggregation::<i64>("test_observable_gauge", None)
        else {
            unreachable!()
        };
        // Expecting 2 time-series.
        let expected_time_series_count = if use_empty_attributes { 3 } else { 2 };
        assert_eq!(gauge.data_points.len(), expected_time_series_count);

        if use_empty_attributes {
            // find and validate zero attribute datapoint
            let zero_attribute_datapoint =
                find_gauge_datapoint_with_no_attributes(&gauge.data_points)
                    .expect("datapoint with no attributes expected");
            assert_eq!(zero_attribute_datapoint.value, 1);
        }

        // find and validate key1=value1 datapoint
        let data_point1 = find_gauge_datapoint_with_key_value(&gauge.data_points, "key1", "value1")
            .expect("datapoint with key1=value1 expected");
        assert_eq!(data_point1.value, 4);

        // find and validate key2=value2 datapoint
        let data_point2 = find_gauge_datapoint_with_key_value(&gauge.data_points, "key2", "value2")
            .expect("datapoint with key2=value2 expected");
        assert_eq!(data_point2.value, 5);

        // Reset and report more measurements
        test_context.reset_metrics();

        test_context.flush_metrics();

        let MetricData::Gauge(gauge) =
            test_context.get_aggregation::<i64>("test_observable_gauge", None)
        else {
            unreachable!()
        };
        assert_eq!(gauge.data_points.len(), expected_time_series_count);

        if use_empty_attributes {
            let zero_attribute_datapoint =
                find_gauge_datapoint_with_no_attributes(&gauge.data_points)
                    .expect("datapoint with no attributes expected");
            assert_eq!(zero_attribute_datapoint.value, 1);
        }

        let data_point1 = find_gauge_datapoint_with_key_value(&gauge.data_points, "key1", "value1")
            .expect("datapoint with key1=value1 expected");
        assert_eq!(data_point1.value, 4);

        let data_point2 = find_gauge_datapoint_with_key_value(&gauge.data_points, "key2", "value2")
            .expect("datapoint with key2=value2 expected");
        assert_eq!(data_point2.value, 5);
    }

    fn counter_aggregation_helper(temporality: Temporality) {
        // Arrange
        let mut test_context = TestContext::new(temporality);
        let counter = test_context.u64_counter("test", "my_counter", None);

        // Act
        counter.add(1, &[KeyValue::new("key1", "value1")]);
        counter.add(1, &[KeyValue::new("key1", "value1")]);
        counter.add(1, &[KeyValue::new("key1", "value1")]);
        counter.add(1, &[KeyValue::new("key1", "value1")]);
        counter.add(1, &[KeyValue::new("key1", "value1")]);

        counter.add(1, &[KeyValue::new("key1", "value2")]);
        counter.add(1, &[KeyValue::new("key1", "value2")]);
        counter.add(1, &[KeyValue::new("key1", "value2")]);

        test_context.flush_metrics();

        // Assert
        let MetricData::Sum(sum) = test_context.get_aggregation::<u64>("my_counter", None) else {
            unreachable!()
        };
        // Expecting 2 time-series.
        assert_eq!(sum.data_points.len(), 2);
        assert!(sum.is_monotonic, "Counter should produce monotonic.");
        if let Temporality::Cumulative = temporality {
            assert_eq!(
                sum.temporality,
                Temporality::Cumulative,
                "Should produce cumulative"
            );
        } else {
            assert_eq!(sum.temporality, Temporality::Delta, "Should produce delta");
        }

        // find and validate key1=value2 datapoint
        let data_point1 = find_sum_datapoint_with_key_value(&sum.data_points, "key1", "value1")
            .expect("datapoint with key1=value1 expected");
        assert_eq!(data_point1.value, 5);

        let data_point1 = find_sum_datapoint_with_key_value(&sum.data_points, "key1", "value2")
            .expect("datapoint with key1=value2 expected");
        assert_eq!(data_point1.value, 3);

        // Reset and report more measurements
        test_context.reset_metrics();
        counter.add(1, &[KeyValue::new("key1", "value1")]);
        counter.add(1, &[KeyValue::new("key1", "value1")]);
        counter.add(1, &[KeyValue::new("key1", "value1")]);
        counter.add(1, &[KeyValue::new("key1", "value1")]);
        counter.add(1, &[KeyValue::new("key1", "value1")]);

        counter.add(1, &[KeyValue::new("key1", "value2")]);
        counter.add(1, &[KeyValue::new("key1", "value2")]);
        counter.add(1, &[KeyValue::new("key1", "value2")]);

        test_context.flush_metrics();

        let MetricData::Sum(sum) = test_context.get_aggregation::<u64>("my_counter", None) else {
            unreachable!()
        };
        assert_eq!(sum.data_points.len(), 2);
        let data_point1 = find_sum_datapoint_with_key_value(&sum.data_points, "key1", "value1")
            .expect("datapoint with key1=value1 expected");
        if temporality == Temporality::Cumulative {
            assert_eq!(data_point1.value, 10);
        } else {
            assert_eq!(data_point1.value, 5);
        }

        let data_point1 = find_sum_datapoint_with_key_value(&sum.data_points, "key1", "value2")
            .expect("datapoint with key1=value2 expected");
        if temporality == Temporality::Cumulative {
            assert_eq!(data_point1.value, 6);
        } else {
            assert_eq!(data_point1.value, 3);
        }
    }

    fn counter_aggregation_overflow_helper(temporality: Temporality) {
        // Arrange
        let mut test_context = TestContext::new(temporality);
        let counter = test_context.u64_counter("test", "my_counter", None);

        // Act
        // Record measurements with A:0, A:1,.......A:1999, which just fits in the 2000 limit
        for v in 0..2000 {
            counter.add(100, &[KeyValue::new("A", v.to_string())]);
        }

        // Empty attributes is specially treated and does not count towards the limit.
        counter.add(3, &[]);
        counter.add(3, &[]);

        // All of the below will now go into overflow.
        counter.add(100, &[KeyValue::new("A", "foo")]);
        counter.add(100, &[KeyValue::new("A", "another")]);
        counter.add(100, &[KeyValue::new("A", "yet_another")]);
        test_context.flush_metrics();

        let MetricData::Sum(sum) = test_context.get_aggregation::<u64>("my_counter", None) else {
            unreachable!()
        };

        // Expecting 2002 metric points. (2000 + 1 overflow + Empty attributes)
        assert_eq!(sum.data_points.len(), 2002);

        let data_point =
            find_overflow_sum_datapoint(&sum.data_points).expect("overflow point expected");
        assert_eq!(data_point.value, 300);

        // let empty_attrs_data_point = &sum.data_points[0];
        let empty_attrs_data_point = find_sum_datapoint_with_no_attributes(&sum.data_points)
            .expect("Empty attributes point expected");
        assert!(
            empty_attrs_data_point.attributes.is_empty(),
            "Non-empty attribute set"
        );
        assert_eq!(
            empty_attrs_data_point.value, 6,
            "Empty attributes value should be 3+3=6"
        );

        // Phase 2 - for delta temporality, collect_and_reset uses in-place eviction:
        // the first collect marks entries as not-updated, and the second collect evicts
        // those still-stale entries. We need an extra flush to trigger that eviction
        // before adding new measurements that should fit under the cardinality limit.
        test_context.reset_metrics();
        if temporality == Temporality::Delta {
            test_context.flush_metrics();
            test_context.reset_metrics();
        }
        // The following should be aggregated normally for Delta,
        // and should go into overflow for Cumulative.
        counter.add(100, &[KeyValue::new("A", "foo")]);
        counter.add(100, &[KeyValue::new("A", "another")]);
        counter.add(100, &[KeyValue::new("A", "yet_another")]);
        test_context.flush_metrics();

        let MetricData::Sum(sum) = test_context.get_aggregation::<u64>("my_counter", None) else {
            unreachable!()
        };

        if temporality == Temporality::Delta {
            assert_eq!(sum.data_points.len(), 3);

            let data_point = find_sum_datapoint_with_key_value(&sum.data_points, "A", "foo")
                .expect("point expected");
            assert_eq!(data_point.value, 100);

            let data_point = find_sum_datapoint_with_key_value(&sum.data_points, "A", "another")
                .expect("point expected");
            assert_eq!(data_point.value, 100);

            let data_point =
                find_sum_datapoint_with_key_value(&sum.data_points, "A", "yet_another")
                    .expect("point expected");
            assert_eq!(data_point.value, 100);
        } else {
            // For cumulative, overflow should still be there, and new points should not be added.
            assert_eq!(sum.data_points.len(), 2002);
            let data_point =
                find_overflow_sum_datapoint(&sum.data_points).expect("overflow point expected");
            assert_eq!(data_point.value, 600);

            let data_point = find_sum_datapoint_with_key_value(&sum.data_points, "A", "foo");
            assert!(data_point.is_none(), "point should not be present");

            let data_point = find_sum_datapoint_with_key_value(&sum.data_points, "A", "another");
            assert!(data_point.is_none(), "point should not be present");

            let data_point =
                find_sum_datapoint_with_key_value(&sum.data_points, "A", "yet_another");
            assert!(data_point.is_none(), "point should not be present");
        }
    }

    fn counter_aggregation_overflow_helper_custom_limit(temporality: Temporality) {
        // Arrange
        let cardinality_limit = 2300;
        let view_change_cardinality = move |i: &Instrument| {
            if i.name == "my_counter" {
                Some(
                    Stream::builder()
                        .with_name("my_counter")
                        .with_cardinality_limit(cardinality_limit)
                        .build()
                        .unwrap(),
                )
            } else {
                None
            }
        };
        let mut test_context = TestContext::new_with_view(temporality, view_change_cardinality);
        let counter = test_context.u64_counter("test", "my_counter", None);

        // Act
        // Record measurements with A:0, A:1,.......A:cardinality_limit, which just fits in the cardinality_limit
        for v in 0..cardinality_limit {
            counter.add(100, &[KeyValue::new("A", v.to_string())]);
        }

        // Empty attributes is specially treated and does not count towards the limit.
        counter.add(3, &[]);
        counter.add(3, &[]);

        // All of the below will now go into overflow.
        counter.add(100, &[KeyValue::new("A", "foo")]);
        counter.add(100, &[KeyValue::new("A", "another")]);
        counter.add(100, &[KeyValue::new("A", "yet_another")]);
        test_context.flush_metrics();

        let MetricData::Sum(sum) = test_context.get_aggregation::<u64>("my_counter", None) else {
            unreachable!()
        };

        // Expecting (cardinality_limit + 1 overflow + Empty attributes) data points.
        assert_eq!(sum.data_points.len(), cardinality_limit + 1 + 1);

        let data_point =
            find_overflow_sum_datapoint(&sum.data_points).expect("overflow point expected");
        assert_eq!(data_point.value, 300);

        // let empty_attrs_data_point = &sum.data_points[0];
        let empty_attrs_data_point = find_sum_datapoint_with_no_attributes(&sum.data_points)
            .expect("Empty attributes point expected");
        assert!(
            empty_attrs_data_point.attributes.is_empty(),
            "Non-empty attribute set"
        );
        assert_eq!(
            empty_attrs_data_point.value, 6,
            "Empty attributes value should be 3+3=6"
        );

        // Phase 2 - for delta temporality, collect_and_reset uses in-place eviction:
        // the first collect marks entries as not-updated, and the second collect evicts
        // those still-stale entries. We need an extra flush to trigger that eviction
        // before adding new measurements that should fit under the cardinality limit.
        test_context.reset_metrics();
        if temporality == Temporality::Delta {
            test_context.flush_metrics();
            test_context.reset_metrics();
        }
        // The following should be aggregated normally for Delta,
        // and should go into overflow for Cumulative.
        counter.add(100, &[KeyValue::new("A", "foo")]);
        counter.add(100, &[KeyValue::new("A", "another")]);
        counter.add(100, &[KeyValue::new("A", "yet_another")]);
        test_context.flush_metrics();

        let MetricData::Sum(sum) = test_context.get_aggregation::<u64>("my_counter", None) else {
            unreachable!()
        };

        if temporality == Temporality::Delta {
            assert_eq!(sum.data_points.len(), 3);

            let data_point = find_sum_datapoint_with_key_value(&sum.data_points, "A", "foo")
                .expect("point expected");
            assert_eq!(data_point.value, 100);

            let data_point = find_sum_datapoint_with_key_value(&sum.data_points, "A", "another")
                .expect("point expected");
            assert_eq!(data_point.value, 100);

            let data_point =
                find_sum_datapoint_with_key_value(&sum.data_points, "A", "yet_another")
                    .expect("point expected");
            assert_eq!(data_point.value, 100);
        } else {
            // For cumulative, overflow should still be there, and new points should not be added.
            assert_eq!(sum.data_points.len(), cardinality_limit + 1 + 1);
            let data_point =
                find_overflow_sum_datapoint(&sum.data_points).expect("overflow point expected");
            assert_eq!(data_point.value, 600);

            let data_point = find_sum_datapoint_with_key_value(&sum.data_points, "A", "foo");
            assert!(data_point.is_none(), "point should not be present");

            let data_point = find_sum_datapoint_with_key_value(&sum.data_points, "A", "another");
            assert!(data_point.is_none(), "point should not be present");

            let data_point =
                find_sum_datapoint_with_key_value(&sum.data_points, "A", "yet_another");
            assert!(data_point.is_none(), "point should not be present");
        }
    }

    fn counter_aggregation_attribute_order_helper(temporality: Temporality, start_sorted: bool) {
        // Arrange
        let mut test_context = TestContext::new(temporality);
        let counter = test_context.u64_counter("test", "my_counter", None);

        // Act
        // Add the same set of attributes in different order. (they are expected
        // to be treated as same attributes)
        // start with sorted order
        if start_sorted {
            counter.add(
                1,
                &[
                    KeyValue::new("A", "a"),
                    KeyValue::new("B", "b"),
                    KeyValue::new("C", "c"),
                ],
            );
        } else {
            counter.add(
                1,
                &[
                    KeyValue::new("A", "a"),
                    KeyValue::new("C", "c"),
                    KeyValue::new("B", "b"),
                ],
            );
        }

        counter.add(
            1,
            &[
                KeyValue::new("A", "a"),
                KeyValue::new("C", "c"),
                KeyValue::new("B", "b"),
            ],
        );
        counter.add(
            1,
            &[
                KeyValue::new("B", "b"),
                KeyValue::new("A", "a"),
                KeyValue::new("C", "c"),
            ],
        );
        counter.add(
            1,
            &[
                KeyValue::new("B", "b"),
                KeyValue::new("C", "c"),
                KeyValue::new("A", "a"),
            ],
        );
        counter.add(
            1,
            &[
                KeyValue::new("C", "c"),
                KeyValue::new("B", "b"),
                KeyValue::new("A", "a"),
            ],
        );
        counter.add(
            1,
            &[
                KeyValue::new("C", "c"),
                KeyValue::new("A", "a"),
                KeyValue::new("B", "b"),
            ],
        );
        test_context.flush_metrics();

        let MetricData::Sum(sum) = test_context.get_aggregation::<u64>("my_counter", None) else {
            unreachable!()
        };

        // Expecting 1 time-series.
        assert_eq!(sum.data_points.len(), 1);

        // validate the sole datapoint
        let data_point1 = &sum.data_points[0];
        assert_eq!(data_point1.value, 6);
    }

    fn updown_counter_aggregation_helper(temporality: Temporality) {
        // Arrange
        let mut test_context = TestContext::new(temporality);
        let counter = test_context.i64_up_down_counter("test", "my_updown_counter", None);

        // Act
        counter.add(10, &[KeyValue::new("key1", "value1")]);
        counter.add(-1, &[KeyValue::new("key1", "value1")]);
        counter.add(-5, &[KeyValue::new("key1", "value1")]);
        counter.add(0, &[KeyValue::new("key1", "value1")]);
        counter.add(1, &[KeyValue::new("key1", "value1")]);

        counter.add(10, &[KeyValue::new("key1", "value2")]);
        counter.add(0, &[KeyValue::new("key1", "value2")]);
        counter.add(-3, &[KeyValue::new("key1", "value2")]);

        test_context.flush_metrics();

        // Assert
        let MetricData::Sum(sum) = test_context.get_aggregation::<i64>("my_updown_counter", None)
        else {
            unreachable!()
        };
        // Expecting 2 time-series.
        assert_eq!(sum.data_points.len(), 2);
        assert!(
            !sum.is_monotonic,
            "UpDownCounter should produce non-monotonic."
        );
        assert_eq!(
            sum.temporality,
            Temporality::Cumulative,
            "Should produce Cumulative for UpDownCounter"
        );

        // find and validate key1=value2 datapoint
        let data_point1 = find_sum_datapoint_with_key_value(&sum.data_points, "key1", "value1")
            .expect("datapoint with key1=value1 expected");
        assert_eq!(data_point1.value, 5);

        let data_point1 = find_sum_datapoint_with_key_value(&sum.data_points, "key1", "value2")
            .expect("datapoint with key1=value2 expected");
        assert_eq!(data_point1.value, 7);

        // Reset and report more measurements
        test_context.reset_metrics();
        counter.add(10, &[KeyValue::new("key1", "value1")]);
        counter.add(-1, &[KeyValue::new("key1", "value1")]);
        counter.add(-5, &[KeyValue::new("key1", "value1")]);
        counter.add(0, &[KeyValue::new("key1", "value1")]);
        counter.add(1, &[KeyValue::new("key1", "value1")]);

        counter.add(10, &[KeyValue::new("key1", "value2")]);
        counter.add(0, &[KeyValue::new("key1", "value2")]);
        counter.add(-3, &[KeyValue::new("key1", "value2")]);

        test_context.flush_metrics();

        let MetricData::Sum(sum) = test_context.get_aggregation::<i64>("my_updown_counter", None)
        else {
            unreachable!()
        };
        assert_eq!(sum.data_points.len(), 2);
        let data_point1 = find_sum_datapoint_with_key_value(&sum.data_points, "key1", "value1")
            .expect("datapoint with key1=value1 expected");
        assert_eq!(data_point1.value, 10);

        let data_point1 = find_sum_datapoint_with_key_value(&sum.data_points, "key1", "value2")
            .expect("datapoint with key1=value2 expected");
        assert_eq!(data_point1.value, 14);
    }

    fn find_sum_datapoint_with_key_value<'a, T>(
        data_points: &'a [SumDataPoint<T>],
        key: &str,
        value: &str,
    ) -> Option<&'a SumDataPoint<T>> {
        data_points.iter().find(|&datapoint| {
            datapoint
                .attributes
                .iter()
                .any(|kv| kv.key.as_str() == key && kv.value.as_str() == value)
        })
    }

    fn find_overflow_sum_datapoint<T>(data_points: &[SumDataPoint<T>]) -> Option<&SumDataPoint<T>> {
        data_points.iter().find(|&datapoint| {
            datapoint.attributes.iter().any(|kv| {
                kv.key.as_str() == "otel.metric.overflow" && kv.value == Value::Bool(true)
            })
        })
    }

    fn find_gauge_datapoint_with_key_value<'a, T>(
        data_points: &'a [GaugeDataPoint<T>],
        key: &str,
        value: &str,
    ) -> Option<&'a GaugeDataPoint<T>> {
        data_points.iter().find(|&datapoint| {
            datapoint
                .attributes
                .iter()
                .any(|kv| kv.key.as_str() == key && kv.value.as_str() == value)
        })
    }

    fn find_sum_datapoint_with_no_attributes<T>(
        data_points: &[SumDataPoint<T>],
    ) -> Option<&SumDataPoint<T>> {
        data_points
            .iter()
            .find(|&datapoint| datapoint.attributes.is_empty())
    }

    fn find_gauge_datapoint_with_no_attributes<T>(
        data_points: &[GaugeDataPoint<T>],
    ) -> Option<&GaugeDataPoint<T>> {
        data_points
            .iter()
            .find(|&datapoint| datapoint.attributes.is_empty())
    }

    fn find_histogram_datapoint_with_key_value<'a, T>(
        data_points: &'a [HistogramDataPoint<T>],
        key: &str,
        value: &str,
    ) -> Option<&'a HistogramDataPoint<T>> {
        data_points.iter().find(|&datapoint| {
            datapoint
                .attributes
                .iter()
                .any(|kv| kv.key.as_str() == key && kv.value.as_str() == value)
        })
    }

    fn find_histogram_datapoint_with_no_attributes<T>(
        data_points: &[HistogramDataPoint<T>],
    ) -> Option<&HistogramDataPoint<T>> {
        data_points
            .iter()
            .find(|&datapoint| datapoint.attributes.is_empty())
    }

    #[cfg(feature = "experimental_metrics_bound_instruments")]
    fn find_overflow_histogram_datapoint<T>(
        data_points: &[HistogramDataPoint<T>],
    ) -> Option<&HistogramDataPoint<T>> {
        data_points.iter().find(|&datapoint| {
            datapoint.attributes.iter().any(|kv| {
                kv.key.as_str() == "otel.metric.overflow" && kv.value == Value::Bool(true)
            })
        })
    }

    #[cfg(feature = "experimental_metrics_bound_instruments")]
    fn find_overflow_exponential_histogram_datapoint<T>(
        data_points: &[ExponentialHistogramDataPoint<T>],
    ) -> Option<&ExponentialHistogramDataPoint<T>> {
        data_points.iter().find(|&datapoint| {
            datapoint.attributes.iter().any(|kv| {
                kv.key.as_str() == "otel.metric.overflow" && kv.value == Value::Bool(true)
            })
        })
    }

    fn find_scope_metric<'a>(
        metrics: &'a [ScopeMetrics],
        name: &'a str,
    ) -> Option<&'a ScopeMetrics> {
        metrics
            .iter()
            .find(|&scope_metric| scope_metric.scope.name() == name)
    }

    struct TestContext {
        exporter: InMemoryMetricExporter,
        meter_provider: SdkMeterProvider,

        // Saving this on the test context for lifetime simplicity
        resource_metrics: Vec<ResourceMetrics>,
    }

    impl TestContext {
        fn new(temporality: Temporality) -> Self {
            let exporter = InMemoryMetricExporterBuilder::new().with_temporality(temporality);
            let exporter = exporter.build();
            let meter_provider = SdkMeterProvider::builder()
                .with_periodic_exporter(exporter.clone())
                .build();

            TestContext {
                exporter,
                meter_provider,
                resource_metrics: vec![],
            }
        }

        fn new_with_view<T>(temporality: Temporality, view: T) -> Self
        where
            T: Fn(&Instrument) -> Option<Stream> + Send + Sync + 'static,
        {
            let exporter = InMemoryMetricExporterBuilder::new().with_temporality(temporality);
            let exporter = exporter.build();
            let meter_provider = SdkMeterProvider::builder()
                .with_periodic_exporter(exporter.clone())
                .with_view(view)
                .build();

            TestContext {
                exporter,
                meter_provider,
                resource_metrics: vec![],
            }
        }

        fn u64_counter(
            &self,
            meter_name: &'static str,
            counter_name: &'static str,
            unit: Option<&'static str>,
        ) -> Counter<u64> {
            let meter = self.meter_provider.meter(meter_name);
            let mut counter_builder = meter.u64_counter(counter_name);
            if let Some(unit_name) = unit {
                counter_builder = counter_builder.with_unit(unit_name);
            }
            counter_builder.build()
        }

        fn i64_up_down_counter(
            &self,
            meter_name: &'static str,
            counter_name: &'static str,
            unit: Option<&'static str>,
        ) -> UpDownCounter<i64> {
            let meter = self.meter_provider.meter(meter_name);
            let mut updown_counter_builder = meter.i64_up_down_counter(counter_name);
            if let Some(unit_name) = unit {
                updown_counter_builder = updown_counter_builder.with_unit(unit_name);
            }
            updown_counter_builder.build()
        }

        fn meter(&self) -> Meter {
            self.meter_provider.meter("test")
        }

        fn flush_metrics(&self) {
            self.meter_provider.force_flush().unwrap();
        }

        fn reset_metrics(&self) {
            self.exporter.reset();
        }

        fn check_no_metrics(&self) {
            let resource_metrics = self
                .exporter
                .get_finished_metrics()
                .expect("metrics expected to be exported"); // TODO: Need to fix InMemoryMetricExporter to return None.

            assert!(resource_metrics.is_empty(), "no metrics should be exported");
        }

        fn get_aggregation<T: Number>(
            &mut self,
            counter_name: &str,
            unit_name: Option<&str>,
        ) -> &MetricData<T> {
            self.resource_metrics = self
                .exporter
                .get_finished_metrics()
                .expect("metrics expected to be exported");

            assert!(
                !self.resource_metrics.is_empty(),
                "no metrics were exported"
            );

            assert!(
                self.resource_metrics.len() == 1,
                "Expected single resource metrics."
            );
            let resource_metric = self
                .resource_metrics
                .first()
                .expect("This should contain exactly one resource metric, as validated above.");

            assert!(
                !resource_metric.scope_metrics.is_empty(),
                "No scope metrics in latest export"
            );
            assert!(!resource_metric.scope_metrics[0].metrics.is_empty());

            let metric = &resource_metric.scope_metrics[0].metrics[0];
            assert_eq!(metric.name, counter_name);
            if let Some(expected_unit) = unit_name {
                assert_eq!(metric.unit, expected_unit);
            }

            T::extract_metrics_data_ref(&metric.data)
                .expect("Failed to cast aggregation to expected type")
        }

        fn get_from_multiple_aggregations<T: Number>(
            &mut self,
            counter_name: &str,
            unit_name: Option<&str>,
            invocation_count: usize,
        ) -> Vec<&MetricData<T>> {
            self.resource_metrics = self
                .exporter
                .get_finished_metrics()
                .expect("metrics expected to be exported");

            assert!(
                !self.resource_metrics.is_empty(),
                "no metrics were exported"
            );

            assert_eq!(
                self.resource_metrics.len(),
                invocation_count,
                "Expected collect to be called {invocation_count} times"
            );

            let result = self
                .resource_metrics
                .iter()
                .map(|resource_metric| {
                    assert!(
                        !resource_metric.scope_metrics.is_empty(),
                        "An export with no scope metrics occurred"
                    );

                    assert!(!resource_metric.scope_metrics[0].metrics.is_empty());

                    let metric = &resource_metric.scope_metrics[0].metrics[0];
                    assert_eq!(metric.name, counter_name);

                    if let Some(expected_unit) = unit_name {
                        assert_eq!(metric.unit, expected_unit);
                    }

                    let aggregation = T::extract_metrics_data_ref(&metric.data)
                        .expect("Failed to cast aggregation to expected type");
                    aggregation
                })
                .collect::<Vec<_>>();

            result
        }
    }

    #[test]
    fn parse_valid_temporality_values() {
        assert_eq!(
            "cumulative".parse::<Temporality>(),
            Ok(Temporality::Cumulative)
        );
        assert_eq!("delta".parse::<Temporality>(), Ok(Temporality::Delta));
        assert_eq!(
            "lowmemory".parse::<Temporality>(),
            Ok(Temporality::LowMemory)
        );
    }

    #[test]
    fn parse_temporality_case_insensitive() {
        assert_eq!(
            "Cumulative".parse::<Temporality>(),
            Ok(Temporality::Cumulative)
        );
        assert_eq!("DELTA".parse::<Temporality>(), Ok(Temporality::Delta));
        assert_eq!(
            "LowMemory".parse::<Temporality>(),
            Ok(Temporality::LowMemory)
        );
        assert_eq!(
            "LOWMEMORY".parse::<Temporality>(),
            Ok(Temporality::LowMemory)
        );
    }

    #[test]
    fn parse_invalid_temporality_returns_err() {
        assert!("unknown".parse::<Temporality>().is_err());
        assert!("".parse::<Temporality>().is_err());
        assert!("cumulativ".parse::<Temporality>().is_err());
    }

    #[cfg(feature = "experimental_metrics_bound_instruments")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn bound_counter_cumulative() {
        let mut test_context = TestContext::new(Temporality::Cumulative);
        let counter = test_context.u64_counter("test", "my_counter", None);
        let attrs = vec![KeyValue::new("key1", "bound_value")];
        let bound = counter.bind(&attrs);

        bound.add(10);
        bound.add(20);
        bound.add(30);
        test_context.flush_metrics();

        let MetricData::Sum(sum) = test_context.get_aggregation::<u64>("my_counter", None) else {
            unreachable!()
        };

        assert_eq!(sum.data_points.len(), 1, "Expected one data point");
        assert!(sum.is_monotonic);
        assert_eq!(sum.temporality, Temporality::Cumulative);

        let data_point = &sum.data_points[0];
        assert_eq!(data_point.value, 60);
        assert_eq!(
            data_point.attributes,
            vec![KeyValue::new("key1", "bound_value")]
        );
    }

    #[cfg(feature = "experimental_metrics_bound_instruments")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn bound_counter_delta() {
        let mut test_context = TestContext::new(Temporality::Delta);
        let counter = test_context.u64_counter("test", "my_counter", None);
        let attrs = vec![KeyValue::new("key1", "bound_value")];
        let bound = counter.bind(&attrs);

        bound.add(50);
        test_context.flush_metrics();

        let MetricData::Sum(sum) = test_context.get_aggregation::<u64>("my_counter", None) else {
            unreachable!()
        };
        assert_eq!(sum.temporality, Temporality::Delta);
        assert_eq!(sum.data_points.len(), 1);
        assert_eq!(sum.data_points[0].value, 50);

        // After delta collect, add more and collect again
        test_context.reset_metrics();
        bound.add(25);
        test_context.flush_metrics();

        let MetricData::Sum(sum) = test_context.get_aggregation::<u64>("my_counter", None) else {
            unreachable!()
        };
        assert_eq!(sum.data_points.len(), 1);
        assert_eq!(
            sum.data_points[0].value, 25,
            "Delta should reset between collections"
        );
    }

    #[cfg(feature = "experimental_metrics_bound_instruments")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn bound_histogram_cumulative() {
        let mut test_context = TestContext::new(Temporality::Cumulative);
        let histogram = test_context
            .meter()
            .f64_histogram("my_histogram")
            .with_boundaries(vec![5.0, 10.0, 25.0, 50.0])
            .build();
        let attrs = vec![KeyValue::new("key1", "bound_value")];
        let bound = histogram.bind(&attrs);

        bound.record(1.0);
        bound.record(7.5);
        bound.record(15.0);
        bound.record(30.0);
        test_context.flush_metrics();

        let MetricData::Histogram(histogram_data) =
            test_context.get_aggregation::<f64>("my_histogram", None)
        else {
            unreachable!()
        };

        assert_eq!(histogram_data.data_points.len(), 1);
        assert_eq!(histogram_data.temporality, Temporality::Cumulative);

        let dp = &histogram_data.data_points[0];
        assert_eq!(dp.count, 4);
        assert_eq!(dp.sum, 53.5);
        assert_eq!(dp.min.unwrap(), 1.0);
        assert_eq!(dp.max.unwrap(), 30.0);
        assert_eq!(dp.attributes, vec![KeyValue::new("key1", "bound_value")]);
    }

    #[cfg(feature = "experimental_metrics_bound_instruments")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn bound_counter_matches_unbound() {
        let mut test_context = TestContext::new(Temporality::Cumulative);
        let counter = test_context.u64_counter("test", "my_counter", None);
        let attrs = vec![KeyValue::new("key1", "shared")];
        let bound = counter.bind(&attrs);

        // Mix bound and unbound additions to the same attribute set
        counter.add(10, &attrs);
        bound.add(20);
        counter.add(30, &attrs);
        bound.add(40);
        test_context.flush_metrics();

        let MetricData::Sum(sum) = test_context.get_aggregation::<u64>("my_counter", None) else {
            unreachable!()
        };

        assert_eq!(
            sum.data_points.len(),
            1,
            "Bound and unbound should share the same data point"
        );
        assert_eq!(sum.data_points[0].value, 100);
    }

    #[cfg(feature = "experimental_metrics_bound_instruments")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn bound_counter_delta_no_update_no_export() {
        let mut test_context = TestContext::new(Temporality::Delta);
        let counter = test_context.u64_counter("test", "my_counter", None);
        let attrs = vec![KeyValue::new("key1", "bound_value")];
        let bound = counter.bind(&attrs);

        // Cycle 1: add and collect
        bound.add(10);
        test_context.flush_metrics();
        let MetricData::Sum(sum) = test_context.get_aggregation::<u64>("my_counter", None) else {
            unreachable!()
        };
        assert_eq!(sum.data_points.len(), 1);
        assert_eq!(sum.data_points[0].value, 10);

        // Cycle 2: no add, collect — should export nothing
        test_context.reset_metrics();
        test_context.flush_metrics();
        let resource_metrics = test_context
            .exporter
            .get_finished_metrics()
            .expect("metrics export should succeed");
        assert!(
            resource_metrics.is_empty(),
            "Bound handle with no updates should not export"
        );

        // Cycle 3: add again — handle is still alive, produces fresh delta
        test_context.reset_metrics();
        bound.add(5);
        test_context.flush_metrics();
        let MetricData::Sum(sum) = test_context.get_aggregation::<u64>("my_counter", None) else {
            unreachable!()
        };
        assert_eq!(sum.data_points.len(), 1);
        assert_eq!(
            sum.data_points[0].value, 5,
            "Bound handle should produce fresh delta after quiet cycle"
        );
    }

    #[cfg(feature = "experimental_metrics_bound_instruments")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn bound_counter_at_overflow_attributes_to_overflow_bucket() {
        let cardinality_limit = 3;
        let view = move |i: &Instrument| {
            if i.name() == "my_counter" {
                Stream::builder()
                    .with_name("my_counter")
                    .with_cardinality_limit(cardinality_limit)
                    .build()
                    .ok()
            } else {
                None
            }
        };
        let mut test_context = TestContext::new_with_view(Temporality::Delta, view);
        let counter = test_context.u64_counter("test", "my_counter", None);

        // Fill to cardinality limit with unbound calls
        for v in 0..cardinality_limit {
            counter.add(1, &[KeyValue::new("A", v.to_string())]);
        }

        // bind() at overflow — handle binds directly to the overflow tracker
        let overflow_attrs = vec![KeyValue::new("A", "overflow_bind")];
        let bound = counter.bind(&overflow_attrs);
        bound.add(42);

        test_context.flush_metrics();
        let MetricData::Sum(sum) = test_context.get_aggregation::<u64>("my_counter", None) else {
            unreachable!()
        };

        // Expect: cardinality_limit unique + 1 overflow = cardinality_limit + 1
        assert_eq!(
            sum.data_points.len(),
            cardinality_limit + 1,
            "Expected {} unique + 1 overflow data points",
            cardinality_limit
        );

        // The bound handle's value should appear in the overflow bucket
        let overflow_dp =
            find_overflow_sum_datapoint(&sum.data_points).expect("overflow point expected");
        assert_eq!(
            overflow_dp.value, 42,
            "Bound-at-overflow data should go to overflow bucket"
        );
    }

    #[cfg(feature = "experimental_metrics_bound_instruments")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn bound_counter_overflow_recovery_after_delta_eviction() {
        let cardinality_limit = 3;
        let view = move |i: &Instrument| {
            if i.name() == "my_counter" {
                Stream::builder()
                    .with_name("my_counter")
                    .with_cardinality_limit(cardinality_limit)
                    .build()
                    .ok()
            } else {
                None
            }
        };
        let mut test_context = TestContext::new_with_view(Temporality::Delta, view);
        let counter = test_context.u64_counter("test", "my_counter", None);

        // Fill to cardinality limit with unbound calls (these are one-shot, not bound)
        for v in 0..cardinality_limit {
            counter.add(1, &[KeyValue::new("A", v.to_string())]);
        }

        // Collect cycle 1: exports the 3 unique entries, then evicts them (no new updates)
        test_context.flush_metrics();
        let MetricData::Sum(sum) = test_context.get_aggregation::<u64>("my_counter", None) else {
            unreachable!()
        };
        assert_eq!(sum.data_points.len(), cardinality_limit);

        // Cycle 2: no unbound adds, so the stale entries get evicted.
        // Space is now open. A new bind() should get a dedicated tracker.
        test_context.reset_metrics();
        test_context.flush_metrics(); // triggers eviction of stale entries

        let new_attrs = vec![KeyValue::new("A", "recovered")];
        let bound = counter.bind(&new_attrs);
        bound.add(99);

        test_context.reset_metrics();
        test_context.flush_metrics();
        let MetricData::Sum(sum) = test_context.get_aggregation::<u64>("my_counter", None) else {
            unreachable!()
        };

        // The bound handle should have a dedicated tracker, NOT overflow
        assert_eq!(
            sum.data_points.len(),
            1,
            "Only the bound entry should be exported"
        );
        let dp = find_sum_datapoint_with_key_value(&sum.data_points, "A", "recovered")
            .expect("should find dedicated data point for recovered attrs");
        assert_eq!(
            dp.value, 99,
            "Bound handle after recovery should have dedicated tracker"
        );
        assert!(
            find_overflow_sum_datapoint(&sum.data_points).is_none(),
            "Should not have overflow — bind() after eviction should get a dedicated tracker"
        );
    }

    #[cfg(feature = "experimental_metrics_bound_instruments")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn bound_counter_multiple_overflow_handles_share_overflow_bucket() {
        let cardinality_limit = 2;
        let view = move |i: &Instrument| {
            if i.name() == "my_counter" {
                Stream::builder()
                    .with_name("my_counter")
                    .with_cardinality_limit(cardinality_limit)
                    .build()
                    .ok()
            } else {
                None
            }
        };
        let mut test_context = TestContext::new_with_view(Temporality::Delta, view);
        let counter = test_context.u64_counter("test", "my_counter", None);

        // Fill to limit
        counter.add(1, &[KeyValue::new("A", "0")]);
        counter.add(1, &[KeyValue::new("A", "1")]);

        // Bind two distinct attribute sets at overflow
        let bound_a = counter.bind(&[KeyValue::new("A", "overflow_a")]);
        let bound_b = counter.bind(&[KeyValue::new("A", "overflow_b")]);

        bound_a.add(10);
        bound_b.add(20);
        bound_a.add(5);

        test_context.flush_metrics();
        let MetricData::Sum(sum) = test_context.get_aggregation::<u64>("my_counter", None) else {
            unreachable!()
        };

        let overflow_dp =
            find_overflow_sum_datapoint(&sum.data_points).expect("overflow point expected");
        assert_eq!(
            overflow_dp.value, 35,
            "All overflow-bound measurements should accumulate in overflow bucket"
        );

        // Cycle 2: bound handles still work after delta collect
        test_context.reset_metrics();
        bound_a.add(7);
        bound_b.add(3);
        test_context.flush_metrics();

        let MetricData::Sum(sum) = test_context.get_aggregation::<u64>("my_counter", None) else {
            unreachable!()
        };

        let overflow_dp =
            find_overflow_sum_datapoint(&sum.data_points).expect("overflow point expected");
        assert_eq!(
            overflow_dp.value, 10,
            "Overflow-bound handles should continue working across delta cycles"
        );
    }

    #[cfg(feature = "experimental_metrics_bound_instruments")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn bound_counter_overflow_persists_across_eviction_cycles() {
        // Once a bind() lands in overflow, the handle's writes must continue
        // landing in overflow for the lifetime of the handle — even after
        // delta eviction frees space. This is the predictability guarantee:
        // a user inspecting their data should see the bound handle's
        // attribution as a single, stable bucket. The recovery story is
        // explicit (drop and re-bind), not implicit (silent self-healing).
        let cardinality_limit = 3;
        let view = move |i: &Instrument| {
            if i.name() == "my_counter" {
                Stream::builder()
                    .with_name("my_counter")
                    .with_cardinality_limit(cardinality_limit)
                    .build()
                    .ok()
            } else {
                None
            }
        };
        let mut test_context = TestContext::new_with_view(Temporality::Delta, view);
        let counter = test_context.u64_counter("test", "my_counter", None);

        // Cycle 1: fill cardinality with unbound calls, then bind at overflow.
        for v in 0..cardinality_limit {
            counter.add(1, &[KeyValue::new("A", v.to_string())]);
        }
        let stuck_attrs = vec![KeyValue::new("A", "stuck_in_overflow")];
        let bound = counter.bind(&stuck_attrs);
        bound.add(10);

        test_context.flush_metrics();
        let MetricData::Sum(sum) = test_context.get_aggregation::<u64>("my_counter", None) else {
            unreachable!()
        };
        let overflow_dp = find_overflow_sum_datapoint(&sum.data_points)
            .expect("cycle 1: bound write at overflow should land in overflow bucket");
        assert_eq!(overflow_dp.value, 10);

        // Cycle 2: no calls. The 3 unbound entries become stale and are evicted,
        // freeing all of the cardinality budget.
        test_context.reset_metrics();
        test_context.flush_metrics();

        // Cycle 3: the SAME bound handle is used again. Even though space is
        // available, its writes must still land in overflow — the handle is
        // permanently bound to overflow, not silently re-resolved.
        test_context.reset_metrics();
        bound.add(99);
        test_context.flush_metrics();
        let MetricData::Sum(sum) = test_context.get_aggregation::<u64>("my_counter", None) else {
            unreachable!()
        };
        let overflow_dp = find_overflow_sum_datapoint(&sum.data_points)
            .expect("cycle 3: bound write must still land in overflow even after space frees up");
        assert_eq!(
            overflow_dp.value, 99,
            "Bound-at-overflow handle must keep writing to overflow even after delta eviction"
        );
        assert!(
            find_sum_datapoint_with_key_value(&sum.data_points, "A", "stuck_in_overflow").is_none(),
            "Bound-at-overflow handle must not silently self-heal to a dedicated tracker"
        );
    }

    #[cfg(feature = "experimental_metrics_bound_instruments")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn bound_histogram_delta() {
        let mut test_context = TestContext::new(Temporality::Delta);
        let histogram = test_context
            .meter()
            .f64_histogram("my_histogram")
            .with_boundaries(vec![5.0, 10.0, 25.0, 50.0])
            .build();
        let attrs = vec![KeyValue::new("key1", "bound_value")];
        let bound = histogram.bind(&attrs);

        // Cycle 1: record and collect
        bound.record(3.0);
        bound.record(12.0);
        test_context.flush_metrics();

        let MetricData::Histogram(hist) = test_context.get_aggregation::<f64>("my_histogram", None)
        else {
            unreachable!()
        };
        assert_eq!(hist.temporality, Temporality::Delta);
        assert_eq!(hist.data_points.len(), 1);
        assert_eq!(hist.data_points[0].count, 2);
        assert_eq!(hist.data_points[0].sum, 15.0);

        // Cycle 2: delta resets, new values
        test_context.reset_metrics();
        bound.record(40.0);
        test_context.flush_metrics();

        let MetricData::Histogram(hist) = test_context.get_aggregation::<f64>("my_histogram", None)
        else {
            unreachable!()
        };
        assert_eq!(hist.data_points.len(), 1);
        assert_eq!(
            hist.data_points[0].count, 1,
            "Delta should reset count between collections"
        );
        assert_eq!(
            hist.data_points[0].sum, 40.0,
            "Delta should reset sum between collections"
        );
    }

    #[cfg(feature = "experimental_metrics_bound_instruments")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn bound_histogram_matches_unbound() {
        let mut test_context = TestContext::new(Temporality::Cumulative);
        let histogram = test_context
            .meter()
            .f64_histogram("my_histogram")
            .with_boundaries(vec![10.0, 50.0])
            .build();
        let attrs = vec![KeyValue::new("key1", "shared")];
        let bound = histogram.bind(&attrs);

        // Mix bound and unbound recordings to the same attribute set
        histogram.record(5.0, &attrs);
        bound.record(15.0);
        histogram.record(25.0, &attrs);
        bound.record(35.0);
        test_context.flush_metrics();

        let MetricData::Histogram(hist) = test_context.get_aggregation::<f64>("my_histogram", None)
        else {
            unreachable!()
        };

        assert_eq!(
            hist.data_points.len(),
            1,
            "Bound and unbound should share the same data point"
        );
        assert_eq!(hist.data_points[0].count, 4);
        assert_eq!(hist.data_points[0].sum, 80.0);
        assert_eq!(hist.data_points[0].min.unwrap(), 5.0);
        assert_eq!(hist.data_points[0].max.unwrap(), 35.0);
    }

    #[cfg(feature = "experimental_metrics_bound_instruments")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn bound_histogram_delta_no_update_no_export() {
        let mut test_context = TestContext::new(Temporality::Delta);
        let histogram = test_context
            .meter()
            .f64_histogram("my_histogram")
            .with_boundaries(vec![10.0])
            .build();
        let attrs = vec![KeyValue::new("key1", "bound_value")];
        let bound = histogram.bind(&attrs);

        // Cycle 1: record and collect
        bound.record(5.0);
        test_context.flush_metrics();
        let MetricData::Histogram(hist) = test_context.get_aggregation::<f64>("my_histogram", None)
        else {
            unreachable!()
        };
        assert_eq!(hist.data_points.len(), 1);
        assert_eq!(hist.data_points[0].count, 1);

        // Cycle 2: no recordings — should export nothing
        test_context.reset_metrics();
        test_context.flush_metrics();
        let resource_metrics = test_context
            .exporter
            .get_finished_metrics()
            .expect("metrics export should succeed");
        assert!(
            resource_metrics.is_empty(),
            "Bound histogram with no updates should not export"
        );

        // Cycle 3: record again — handle still alive
        test_context.reset_metrics();
        bound.record(20.0);
        test_context.flush_metrics();
        let MetricData::Histogram(hist) = test_context.get_aggregation::<f64>("my_histogram", None)
        else {
            unreachable!()
        };
        assert_eq!(hist.data_points.len(), 1);
        assert_eq!(
            hist.data_points[0].count, 1,
            "Fresh delta after quiet cycle"
        );
        assert_eq!(hist.data_points[0].sum, 20.0);
    }

    #[cfg(feature = "experimental_metrics_bound_instruments")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn bound_histogram_at_overflow_attributes_to_overflow_bucket() {
        let cardinality_limit = 3;
        let view = move |i: &Instrument| {
            if i.name() == "my_histogram" {
                Stream::builder()
                    .with_name("my_histogram")
                    .with_cardinality_limit(cardinality_limit)
                    .build()
                    .ok()
            } else {
                None
            }
        };
        let mut test_context = TestContext::new_with_view(Temporality::Delta, view);
        let histogram = test_context
            .meter()
            .f64_histogram("my_histogram")
            .with_boundaries(vec![10.0, 50.0])
            .build();

        // Fill to cardinality limit with unbound calls
        for v in 0..cardinality_limit {
            histogram.record(1.0, &[KeyValue::new("A", v.to_string())]);
        }

        // bind() at overflow — handle binds directly to the overflow tracker
        let overflow_attrs = vec![KeyValue::new("A", "overflow_bind")];
        let bound = histogram.bind(&overflow_attrs);
        bound.record(42.0);

        test_context.flush_metrics();
        let MetricData::Histogram(hist) = test_context.get_aggregation::<f64>("my_histogram", None)
        else {
            unreachable!()
        };

        assert_eq!(
            hist.data_points.len(),
            cardinality_limit + 1,
            "Expected {} unique + 1 overflow data points",
            cardinality_limit
        );

        let overflow_dp =
            find_overflow_histogram_datapoint(&hist.data_points).expect("overflow point expected");
        assert_eq!(
            overflow_dp.sum, 42.0,
            "Bound-at-overflow data should go to overflow bucket"
        );
        assert_eq!(overflow_dp.count, 1);
    }

    #[cfg(feature = "experimental_metrics_bound_instruments")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn bound_histogram_overflow_persists_across_eviction_cycles() {
        // Mirror of bound_counter_overflow_persists_across_eviction_cycles for
        // histograms: a bound-at-overflow handle must keep landing in overflow
        // even after delta eviction frees space, for the lifetime of the handle.
        let cardinality_limit = 3;
        let view = move |i: &Instrument| {
            if i.name() == "my_histogram" {
                Stream::builder()
                    .with_name("my_histogram")
                    .with_cardinality_limit(cardinality_limit)
                    .build()
                    .ok()
            } else {
                None
            }
        };
        let mut test_context = TestContext::new_with_view(Temporality::Delta, view);
        let histogram = test_context
            .meter()
            .f64_histogram("my_histogram")
            .with_boundaries(vec![10.0, 50.0])
            .build();

        // Cycle 1: fill cardinality with unbound calls, then bind at overflow.
        for v in 0..cardinality_limit {
            histogram.record(1.0, &[KeyValue::new("A", v.to_string())]);
        }
        let stuck_attrs = vec![KeyValue::new("A", "stuck_in_overflow")];
        let bound = histogram.bind(&stuck_attrs);
        bound.record(15.0);

        test_context.flush_metrics();
        let MetricData::Histogram(hist) = test_context.get_aggregation::<f64>("my_histogram", None)
        else {
            unreachable!()
        };
        let overflow_dp = find_overflow_histogram_datapoint(&hist.data_points)
            .expect("cycle 1: bound write at overflow should land in overflow bucket");
        assert_eq!(overflow_dp.sum, 15.0);

        // Cycle 2: no calls. Stale unbound entries get evicted, freeing space.
        test_context.reset_metrics();
        test_context.flush_metrics();

        // Cycle 3: same bound handle. Even though space is free, writes must
        // still land in overflow.
        test_context.reset_metrics();
        bound.record(99.0);
        test_context.flush_metrics();
        let MetricData::Histogram(hist) = test_context.get_aggregation::<f64>("my_histogram", None)
        else {
            unreachable!()
        };
        let overflow_dp = find_overflow_histogram_datapoint(&hist.data_points)
            .expect("cycle 3: bound write must still land in overflow even after space frees up");
        assert_eq!(
            overflow_dp.sum, 99.0,
            "Bound-at-overflow histogram must keep writing to overflow even after delta eviction"
        );
        assert_eq!(overflow_dp.count, 1);
    }

    #[cfg(feature = "experimental_metrics_bound_instruments")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn bound_exponential_histogram_delta() {
        // Histogram configured with Base2ExponentialHistogram aggregation goes
        // through ExpoHistogram internally. Verify bind() returns a handle
        // whose direct writes accumulate correctly.
        let view = |i: &Instrument| {
            if i.name() == "my_histogram" {
                Stream::builder()
                    .with_aggregation(Aggregation::Base2ExponentialHistogram {
                        max_size: 160,
                        max_scale: 20,
                        record_min_max: true,
                    })
                    .build()
                    .ok()
            } else {
                None
            }
        };
        let mut test_context = TestContext::new_with_view(Temporality::Delta, view);
        let histogram = test_context.meter().f64_histogram("my_histogram").build();
        let attrs = vec![KeyValue::new("key1", "bound_value")];
        let bound = histogram.bind(&attrs);

        bound.record(2.0);
        bound.record(4.0);
        bound.record(8.0);
        // NaN/inf must be filtered just like the unbound path.
        bound.record(f64::NAN);
        bound.record(f64::INFINITY);

        test_context.flush_metrics();
        let MetricData::ExponentialHistogram(hist) =
            test_context.get_aggregation::<f64>("my_histogram", None)
        else {
            panic!("expected ExponentialHistogram aggregation");
        };
        assert_eq!(hist.data_points.len(), 1);
        let dp = &hist.data_points[0];
        assert_eq!(dp.count, 3, "NaN and infinity should be dropped");
        assert_eq!(dp.sum, 14.0);
        assert_eq!(dp.min, Some(2.0));
        assert_eq!(dp.max, Some(8.0));
    }

    #[cfg(feature = "experimental_metrics_bound_instruments")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn bound_exponential_histogram_at_overflow_attributes_to_overflow_bucket() {
        let cardinality_limit = 3;
        let view = move |i: &Instrument| {
            if i.name() == "my_histogram" {
                Stream::builder()
                    .with_aggregation(Aggregation::Base2ExponentialHistogram {
                        max_size: 160,
                        max_scale: 20,
                        record_min_max: true,
                    })
                    .with_cardinality_limit(cardinality_limit)
                    .build()
                    .ok()
            } else {
                None
            }
        };
        let mut test_context = TestContext::new_with_view(Temporality::Delta, view);
        let histogram = test_context.meter().f64_histogram("my_histogram").build();

        for v in 0..cardinality_limit {
            histogram.record(1.0, &[KeyValue::new("A", v.to_string())]);
        }

        // bind() at overflow — handle binds directly to the overflow tracker
        let overflow_attrs = vec![KeyValue::new("A", "overflow_bind")];
        let bound = histogram.bind(&overflow_attrs);
        bound.record(42.0);

        test_context.flush_metrics();
        let MetricData::ExponentialHistogram(hist) =
            test_context.get_aggregation::<f64>("my_histogram", None)
        else {
            panic!("expected ExponentialHistogram aggregation");
        };
        let overflow_dp = find_overflow_exponential_histogram_datapoint(&hist.data_points)
            .expect("overflow point expected");
        assert_eq!(
            overflow_dp.sum, 42.0,
            "Bound-at-overflow data should go to overflow bucket"
        );
        assert_eq!(overflow_dp.count, 1);
    }

    #[cfg(feature = "experimental_metrics_bound_instruments")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn bound_exponential_histogram_overflow_persists_across_eviction_cycles() {
        // Same predictability invariant the counter/histogram tests assert,
        // verified for the exponential histogram aggregator path.
        let cardinality_limit = 3;
        let view = move |i: &Instrument| {
            if i.name() == "my_histogram" {
                Stream::builder()
                    .with_aggregation(Aggregation::Base2ExponentialHistogram {
                        max_size: 160,
                        max_scale: 20,
                        record_min_max: true,
                    })
                    .with_cardinality_limit(cardinality_limit)
                    .build()
                    .ok()
            } else {
                None
            }
        };
        let mut test_context = TestContext::new_with_view(Temporality::Delta, view);
        let histogram = test_context.meter().f64_histogram("my_histogram").build();

        for v in 0..cardinality_limit {
            histogram.record(1.0, &[KeyValue::new("A", v.to_string())]);
        }
        let stuck_attrs = vec![KeyValue::new("A", "stuck_in_overflow")];
        let bound = histogram.bind(&stuck_attrs);
        bound.record(15.0);

        test_context.flush_metrics();
        let MetricData::ExponentialHistogram(hist) =
            test_context.get_aggregation::<f64>("my_histogram", None)
        else {
            panic!("expected ExponentialHistogram aggregation");
        };
        let overflow_dp = find_overflow_exponential_histogram_datapoint(&hist.data_points)
            .expect("cycle 1: bound write at overflow should land in overflow bucket");
        assert_eq!(overflow_dp.sum, 15.0);

        // Cycle 2: evict stale entries, freeing space.
        test_context.reset_metrics();
        test_context.flush_metrics();

        // Cycle 3: bound handle keeps writing to overflow.
        test_context.reset_metrics();
        bound.record(99.0);
        test_context.flush_metrics();
        let MetricData::ExponentialHistogram(hist) =
            test_context.get_aggregation::<f64>("my_histogram", None)
        else {
            panic!("expected ExponentialHistogram aggregation");
        };
        let overflow_dp = find_overflow_exponential_histogram_datapoint(&hist.data_points)
            .expect("cycle 3: bound write must still land in overflow even after space frees up");
        assert_eq!(
            overflow_dp.sum, 99.0,
            "Bound-at-overflow ExpoHistogram must keep writing to overflow even after delta eviction"
        );
        assert_eq!(overflow_dp.count, 1);
    }

    #[cfg(feature = "experimental_metrics_bound_instruments")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn bound_counter_drop_enables_eviction() {
        let mut test_context = TestContext::new(Temporality::Delta);
        let counter = test_context.u64_counter("test", "my_counter", None);
        let attrs = vec![KeyValue::new("key1", "ephemeral")];

        {
            let bound = counter.bind(&attrs);
            bound.add(100);
            test_context.flush_metrics();

            let MetricData::Sum(sum) = test_context.get_aggregation::<u64>("my_counter", None)
            else {
                unreachable!()
            };
            assert_eq!(sum.data_points.len(), 1);
            assert_eq!(sum.data_points[0].value, 100);
            // bound drops here
        }

        // Cycle 2: no updates, bound handle dropped — entry becomes stale and evictable
        test_context.reset_metrics();
        test_context.flush_metrics();

        // Cycle 3: the stale entry should have been evicted, so a new unbound add
        // should be the only data point
        test_context.reset_metrics();
        counter.add(1, &[KeyValue::new("key1", "new_entry")]);
        test_context.flush_metrics();

        let MetricData::Sum(sum) = test_context.get_aggregation::<u64>("my_counter", None) else {
            unreachable!()
        };

        // Only the new entry should be present — the old "ephemeral" entry was evicted
        assert_eq!(sum.data_points.len(), 1);
        let dp = find_sum_datapoint_with_key_value(&sum.data_points, "key1", "new_entry")
            .expect("new_entry should be present");
        assert_eq!(dp.value, 1);
        assert!(
            find_sum_datapoint_with_key_value(&sum.data_points, "key1", "ephemeral").is_none(),
            "Dropped bound entry should have been evicted"
        );
    }

    #[cfg(feature = "experimental_metrics_bound_instruments")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn bound_counter_multiple_handles_same_attrs() {
        let mut test_context = TestContext::new(Temporality::Delta);
        let counter = test_context.u64_counter("test", "my_counter", None);
        let attrs = vec![KeyValue::new("key1", "shared")];

        let bound1 = counter.bind(&attrs);
        let bound2 = counter.bind(&attrs);

        bound1.add(10);
        bound2.add(20);
        test_context.flush_metrics();

        let MetricData::Sum(sum) = test_context.get_aggregation::<u64>("my_counter", None) else {
            unreachable!()
        };
        assert_eq!(
            sum.data_points.len(),
            1,
            "Multiple handles to same attrs should share data point"
        );
        assert_eq!(sum.data_points[0].value, 30);

        // Drop one handle — entry should NOT be evicted
        drop(bound1);
        test_context.reset_metrics();
        test_context.flush_metrics(); // idle cycle, but bound2 still holds it

        // bound2 still works
        test_context.reset_metrics();
        bound2.add(5);
        test_context.flush_metrics();

        let MetricData::Sum(sum) = test_context.get_aggregation::<u64>("my_counter", None) else {
            unreachable!()
        };
        assert_eq!(sum.data_points.len(), 1);
        assert_eq!(
            sum.data_points[0].value, 5,
            "Entry should persist while any handle is alive"
        );
    }

    #[cfg(feature = "experimental_metrics_bound_instruments")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn bound_counter_empty_attributes() {
        let mut test_context = TestContext::new(Temporality::Cumulative);
        let counter = test_context.u64_counter("test", "my_counter", None);
        let bound = counter.bind(&[]);

        bound.add(10);
        bound.add(30);
        test_context.flush_metrics();

        let MetricData::Sum(sum) = test_context.get_aggregation::<u64>("my_counter", None) else {
            unreachable!()
        };

        assert_eq!(sum.data_points.len(), 1);
        assert_eq!(sum.data_points[0].value, 40);
    }

    #[cfg(feature = "experimental_metrics_bound_instruments")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn bound_counter_empty_attributes_shares_with_unbound() {
        let mut test_context = TestContext::new(Temporality::Cumulative);
        let counter = test_context.u64_counter("test", "my_counter", None);
        let bound = counter.bind(&[]);

        // Mix bound and unbound calls with empty attributes — they must share
        // the same data point (both route to no_attribute_tracker).
        counter.add(10, &[]);
        bound.add(20);
        counter.add(30, &[]);
        bound.add(40);
        test_context.flush_metrics();

        let MetricData::Sum(sum) = test_context.get_aggregation::<u64>("my_counter", None) else {
            unreachable!()
        };

        assert_eq!(
            sum.data_points.len(),
            1,
            "Bound and unbound with empty attributes must share the same data point"
        );
        assert_eq!(sum.data_points[0].value, 100);
    }

    #[cfg(feature = "experimental_metrics_bound_instruments")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn bound_histogram_empty_attributes_shares_with_unbound() {
        let mut test_context = TestContext::new(Temporality::Cumulative);
        let histogram = test_context
            .meter()
            .u64_histogram("my_histogram")
            .with_boundaries(vec![5.0, 10.0, 25.0])
            .build();
        let bound = histogram.bind(&[]);

        histogram.record(3, &[]);
        bound.record(7);
        histogram.record(20, &[]);
        test_context.flush_metrics();

        let MetricData::Histogram(hist) = test_context.get_aggregation::<u64>("my_histogram", None)
        else {
            unreachable!()
        };

        assert_eq!(
            hist.data_points.len(),
            1,
            "Bound and unbound with empty attributes must share the same data point"
        );
        let dp = &hist.data_points[0];
        assert!(dp.attributes.is_empty());
        assert_eq!(dp.count, 3);
        assert_eq!(dp.sum, 30);
    }

    #[cfg(feature = "experimental_metrics_bound_instruments")]
    #[cfg(feature = "spec_unstable_metrics_views")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn bound_counter_view_filters_attributes_at_bind_time() {
        use opentelemetry::Key;

        let exporter = InMemoryMetricExporter::default();
        let view = |i: &Instrument| {
            if i.name() == "my_counter" {
                Stream::builder()
                    .with_allowed_attribute_keys(vec![Key::new("k1"), Key::new("k2")])
                    .build()
                    .ok()
            } else {
                None
            }
        };
        let meter_provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .with_view(view)
            .build();
        let meter = meter_provider.meter("test");
        let counter = meter.u64_counter("my_counter").build();

        // bind with k3 included — view should drop it at bind time
        let bound = counter.bind(&[
            KeyValue::new("k1", "v1"),
            KeyValue::new("k2", "v2"),
            KeyValue::new("k3", "v3"),
        ]);
        bound.add(10);
        bound.add(20);

        // unbound call with a *different* k3 value: after view filtering both
        // bound and unbound must collapse into the same data point.
        counter.add(
            7,
            &[
                KeyValue::new("k1", "v1"),
                KeyValue::new("k2", "v2"),
                KeyValue::new("k3", "different"),
            ],
        );

        meter_provider.force_flush().unwrap();
        let resource_metrics = exporter
            .get_finished_metrics()
            .expect("metrics are expected to be exported.");
        let metric = &resource_metrics[0].scope_metrics[0].metrics[0];
        let data::AggregatedMetrics::U64(MetricData::Sum(sum)) = &metric.data else {
            unreachable!()
        };

        assert_eq!(
            sum.data_points.len(),
            1,
            "view should filter k3, leaving bound+unbound to aggregate together"
        );
        assert_eq!(sum.data_points[0].value, 37);
        let attrs = &sum.data_points[0].attributes;
        assert_eq!(attrs.len(), 2);
        assert!(attrs.iter().any(|kv| kv.key.as_str() == "k1"));
        assert!(attrs.iter().any(|kv| kv.key.as_str() == "k2"));
        assert!(!attrs.iter().any(|kv| kv.key.as_str() == "k3"));
    }

    #[cfg(feature = "experimental_metrics_bound_instruments")]
    #[cfg(feature = "spec_unstable_metrics_views")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn bound_histogram_view_filters_attributes_at_bind_time() {
        use opentelemetry::Key;

        let exporter = InMemoryMetricExporter::default();
        let view = |i: &Instrument| {
            if i.name() == "my_hist" {
                Stream::builder()
                    .with_allowed_attribute_keys(vec![Key::new("k1"), Key::new("k2")])
                    .build()
                    .ok()
            } else {
                None
            }
        };
        let meter_provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .with_view(view)
            .build();
        let meter = meter_provider.meter("test");
        let histogram = meter
            .u64_histogram("my_hist")
            .with_boundaries(vec![5.0, 10.0, 25.0])
            .build();

        let bound = histogram.bind(&[
            KeyValue::new("k1", "v1"),
            KeyValue::new("k2", "v2"),
            KeyValue::new("k3", "v3"),
        ]);
        bound.record(3);
        bound.record(20);
        histogram.record(
            7,
            &[
                KeyValue::new("k1", "v1"),
                KeyValue::new("k2", "v2"),
                KeyValue::new("k3", "different"),
            ],
        );

        meter_provider.force_flush().unwrap();
        let resource_metrics = exporter
            .get_finished_metrics()
            .expect("metrics are expected to be exported.");
        let metric = &resource_metrics[0].scope_metrics[0].metrics[0];
        let data::AggregatedMetrics::U64(MetricData::Histogram(hist)) = &metric.data else {
            unreachable!()
        };

        assert_eq!(
            hist.data_points.len(),
            1,
            "view should filter k3, leaving bound+unbound to aggregate together"
        );
        let dp = &hist.data_points[0];
        assert_eq!(dp.count, 3);
        assert_eq!(dp.sum, 30);
        assert_eq!(dp.attributes.len(), 2);
        assert!(dp.attributes.iter().any(|kv| kv.key.as_str() == "k1"));
        assert!(dp.attributes.iter().any(|kv| kv.key.as_str() == "k2"));
        assert!(!dp.attributes.iter().any(|kv| kv.key.as_str() == "k3"));
    }

    #[cfg(feature = "experimental_metrics_bound_instruments")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn bound_counter_at_overflow_attributes_to_overflow_bucket_cumulative() {
        // Cumulative: cardinality only grows, never evicts. A bind() at the
        // limit lands in overflow and accumulates there forever. Verifies the
        // bound handle's cumulative writes converge in the overflow bucket
        // across multiple collection cycles.
        let cardinality_limit = 3;
        let view = move |i: &Instrument| {
            if i.name() == "my_counter" {
                Stream::builder()
                    .with_name("my_counter")
                    .with_cardinality_limit(cardinality_limit)
                    .build()
                    .ok()
            } else {
                None
            }
        };
        let mut test_context = TestContext::new_with_view(Temporality::Cumulative, view);
        let counter = test_context.u64_counter("test", "my_counter", None);

        for v in 0..cardinality_limit {
            counter.add(1, &[KeyValue::new("A", v.to_string())]);
        }
        let bound = counter.bind(&[KeyValue::new("A", "overflow_bind")]);
        bound.add(10);

        test_context.flush_metrics();
        let MetricData::Sum(sum) = test_context.get_aggregation::<u64>("my_counter", None) else {
            unreachable!()
        };
        let overflow_dp = find_overflow_sum_datapoint(&sum.data_points)
            .expect("cycle 1: overflow point expected");
        assert_eq!(overflow_dp.value, 10);

        // Cycle 2: cumulative state accumulates internally; reset the exporter
        // so the assertion sees a single export rather than two appended ones.
        test_context.reset_metrics();
        bound.add(7);
        test_context.flush_metrics();
        let MetricData::Sum(sum) = test_context.get_aggregation::<u64>("my_counter", None) else {
            unreachable!()
        };
        let overflow_dp = find_overflow_sum_datapoint(&sum.data_points)
            .expect("cycle 2: overflow point expected");
        assert_eq!(
            overflow_dp.value, 17,
            "cumulative overflow-bound writes must accumulate"
        );
    }

    #[cfg(feature = "experimental_metrics_bound_instruments")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn bound_histogram_at_overflow_attributes_to_overflow_bucket_cumulative() {
        let cardinality_limit = 3;
        let view = move |i: &Instrument| {
            if i.name() == "my_histogram" {
                Stream::builder()
                    .with_name("my_histogram")
                    .with_cardinality_limit(cardinality_limit)
                    .build()
                    .ok()
            } else {
                None
            }
        };
        let mut test_context = TestContext::new_with_view(Temporality::Cumulative, view);
        let histogram = test_context
            .meter()
            .f64_histogram("my_histogram")
            .with_boundaries(vec![10.0, 50.0])
            .build();

        for v in 0..cardinality_limit {
            histogram.record(1.0, &[KeyValue::new("A", v.to_string())]);
        }
        let bound = histogram.bind(&[KeyValue::new("A", "overflow_bind")]);
        bound.record(20.0);

        test_context.flush_metrics();
        let MetricData::Histogram(hist) = test_context.get_aggregation::<f64>("my_histogram", None)
        else {
            unreachable!()
        };
        let overflow_dp = find_overflow_histogram_datapoint(&hist.data_points)
            .expect("cycle 1: overflow point expected");
        assert_eq!(overflow_dp.sum, 20.0);
        assert_eq!(overflow_dp.count, 1);

        // Cycle 2: cumulative accumulates internally; reset exporter to see a
        // single fresh export rather than two appended ones.
        test_context.reset_metrics();
        bound.record(30.0);
        test_context.flush_metrics();
        let MetricData::Histogram(hist) = test_context.get_aggregation::<f64>("my_histogram", None)
        else {
            unreachable!()
        };
        let overflow_dp = find_overflow_histogram_datapoint(&hist.data_points)
            .expect("cycle 2: overflow point expected");
        assert_eq!(
            overflow_dp.sum, 50.0,
            "cumulative overflow-bound writes must accumulate"
        );
        assert_eq!(overflow_dp.count, 2);
    }

    #[cfg(feature = "experimental_metrics_bound_instruments")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn bound_exponential_histogram_at_overflow_attributes_to_overflow_bucket_cumulative() {
        let cardinality_limit = 3;
        let view = move |i: &Instrument| {
            if i.name() == "my_histogram" {
                Stream::builder()
                    .with_aggregation(Aggregation::Base2ExponentialHistogram {
                        max_size: 160,
                        max_scale: 20,
                        record_min_max: true,
                    })
                    .with_cardinality_limit(cardinality_limit)
                    .build()
                    .ok()
            } else {
                None
            }
        };
        let mut test_context = TestContext::new_with_view(Temporality::Cumulative, view);
        let histogram = test_context.meter().f64_histogram("my_histogram").build();

        for v in 0..cardinality_limit {
            histogram.record(1.0, &[KeyValue::new("A", v.to_string())]);
        }
        let bound = histogram.bind(&[KeyValue::new("A", "overflow_bind")]);
        bound.record(20.0);

        test_context.flush_metrics();
        let MetricData::ExponentialHistogram(hist) =
            test_context.get_aggregation::<f64>("my_histogram", None)
        else {
            panic!("expected ExponentialHistogram aggregation");
        };
        let overflow_dp = find_overflow_exponential_histogram_datapoint(&hist.data_points)
            .expect("cycle 1: overflow point expected");
        assert_eq!(overflow_dp.sum, 20.0);
        assert_eq!(overflow_dp.count, 1);

        // Cycle 2: cumulative accumulates internally; reset exporter to see a
        // single fresh export rather than two appended ones.
        test_context.reset_metrics();
        bound.record(30.0);
        test_context.flush_metrics();
        let MetricData::ExponentialHistogram(hist) =
            test_context.get_aggregation::<f64>("my_histogram", None)
        else {
            panic!("expected ExponentialHistogram aggregation");
        };
        let overflow_dp = find_overflow_exponential_histogram_datapoint(&hist.data_points)
            .expect("cycle 2: overflow point expected");
        assert_eq!(
            overflow_dp.sum, 50.0,
            "cumulative overflow-bound writes must accumulate"
        );
        assert_eq!(overflow_dp.count, 2);
    }

    #[cfg(feature = "experimental_metrics_bound_instruments")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn bound_exponential_histogram_view_filters_attributes_at_bind_time() {
        use opentelemetry::Key;

        let exporter = InMemoryMetricExporter::default();
        let view = |i: &Instrument| {
            if i.name() == "my_hist" {
                Stream::builder()
                    .with_aggregation(Aggregation::Base2ExponentialHistogram {
                        max_size: 160,
                        max_scale: 20,
                        record_min_max: true,
                    })
                    .with_allowed_attribute_keys(vec![Key::new("k1"), Key::new("k2")])
                    .build()
                    .ok()
            } else {
                None
            }
        };
        let meter_provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .with_view(view)
            .build();
        let meter = meter_provider.meter("test");
        let histogram = meter.f64_histogram("my_hist").build();

        let bound = histogram.bind(&[
            KeyValue::new("k1", "v1"),
            KeyValue::new("k2", "v2"),
            KeyValue::new("k3", "v3"),
        ]);
        bound.record(3.0);
        bound.record(20.0);
        // Unbound call with a different k3: after view filtering, bound and unbound
        // must collapse into the same exponential histogram data point.
        histogram.record(
            7.0,
            &[
                KeyValue::new("k1", "v1"),
                KeyValue::new("k2", "v2"),
                KeyValue::new("k3", "different"),
            ],
        );

        meter_provider.force_flush().unwrap();
        let resource_metrics = exporter
            .get_finished_metrics()
            .expect("metrics are expected to be exported.");
        let metric = &resource_metrics[0].scope_metrics[0].metrics[0];
        let data::AggregatedMetrics::F64(MetricData::ExponentialHistogram(hist)) = &metric.data
        else {
            panic!("expected ExponentialHistogram aggregation");
        };

        assert_eq!(
            hist.data_points.len(),
            1,
            "view should filter k3, leaving bound+unbound to aggregate together"
        );
        let dp = &hist.data_points[0];
        assert_eq!(dp.count, 3);
        assert_eq!(dp.sum, 30.0);
        assert_eq!(dp.attributes.len(), 2);
        assert!(dp.attributes.iter().any(|kv| kv.key.as_str() == "k1"));
        assert!(dp.attributes.iter().any(|kv| kv.key.as_str() == "k2"));
        assert!(!dp.attributes.iter().any(|kv| kv.key.as_str() == "k3"));
    }
}
