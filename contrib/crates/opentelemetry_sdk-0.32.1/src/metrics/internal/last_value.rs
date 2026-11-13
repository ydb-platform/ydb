use crate::metrics::{
    data::{self, AggregatedMetrics, GaugeDataPoint, MetricData},
    Temporality,
};
use opentelemetry::KeyValue;
#[cfg(feature = "experimental_metrics_bound_instruments")]
use std::sync::atomic::Ordering;
#[cfg(feature = "experimental_metrics_bound_instruments")]
use std::sync::Arc;

use super::{
    aggregate::{AggregateTimeInitiator, AttributeSetFilter},
    Aggregator, AtomicTracker, AtomicallyUpdate, ComputeAggregation, Measure, Number, ValueMap,
};
#[cfg(feature = "experimental_metrics_bound_instruments")]
use super::{BoundMeasure, NoopBoundMeasure, TrackerEntry};

/// Pre-bound gauge/last-value handle: writes go directly to a fixed
/// `TrackerEntry` without per-call attribute lookup. The `tracker` is either
/// a dedicated entry for the bound attribute set, or — if bind() hit the
/// cardinality limit — the shared overflow tracker.
#[cfg(feature = "experimental_metrics_bound_instruments")]
struct BoundLastValueHandle<T: Number> {
    tracker: Arc<TrackerEntry<Assign<T>>>,
}

#[cfg(feature = "experimental_metrics_bound_instruments")]
impl<T: Number> BoundMeasure<T> for BoundLastValueHandle<T> {
    fn call(&self, measurement: T) {
        self.tracker.aggregator.update(measurement);
        self.tracker.has_been_updated.store(true, Ordering::Release);
    }
}

#[cfg(feature = "experimental_metrics_bound_instruments")]
impl<T: Number> Drop for BoundLastValueHandle<T> {
    fn drop(&mut self) {
        self.tracker.bound_count.fetch_sub(1, Ordering::Relaxed);
    }
}

/// this is reused by PrecomputedSum
pub(crate) struct Assign<T>
where
    T: AtomicallyUpdate<T>,
{
    pub(crate) value: T::AtomicTracker,
}

impl<T> Aggregator for Assign<T>
where
    T: Number,
{
    type InitConfig = ();
    type PreComputedValue = T;

    fn create(_init: &()) -> Self {
        Self {
            value: T::new_atomic_tracker(T::default()),
        }
    }

    fn update(&self, value: T) {
        self.value.store(value)
    }

    fn clone_and_reset(&self, _: &()) -> Self {
        Self {
            value: T::new_atomic_tracker(self.value.get_and_reset_value()),
        }
    }
}

/// Summarizes a set of measurements as the last one made.
pub(crate) struct LastValue<T: Number> {
    value_map: ValueMap<Assign<T>>,
    init_time: AggregateTimeInitiator,
    temporality: Temporality,
    filter: AttributeSetFilter,
}

impl<T: Number> LastValue<T> {
    pub(crate) fn new(
        temporality: Temporality,
        filter: AttributeSetFilter,
        cardinality_limit: usize,
    ) -> Self {
        LastValue {
            value_map: ValueMap::new((), cardinality_limit),
            init_time: AggregateTimeInitiator::default(),
            temporality,
            filter,
        }
    }

    pub(crate) fn delta(&self, dest: Option<&mut MetricData<T>>) -> (usize, Option<MetricData<T>>) {
        let time = self.init_time.delta();

        let s_data = dest.and_then(|d| {
            if let MetricData::Gauge(gauge) = d {
                Some(gauge)
            } else {
                None
            }
        });
        let mut new_agg = if s_data.is_none() {
            Some(data::Gauge {
                data_points: vec![],
                start_time: Some(time.start),
                time: time.current,
            })
        } else {
            None
        };
        let s_data = s_data.unwrap_or_else(|| new_agg.as_mut().expect("present if s_data is none"));
        s_data.start_time = Some(time.start);
        s_data.time = time.current;

        self.value_map
            .collect_and_reset(&mut s_data.data_points, |attributes, aggr| GaugeDataPoint {
                attributes,
                value: aggr.value.get_and_reset_value(),
                exemplars: vec![],
            });

        (s_data.data_points.len(), new_agg.map(Into::into))
    }

    pub(crate) fn cumulative(
        &self,
        dest: Option<&mut MetricData<T>>,
    ) -> (usize, Option<MetricData<T>>) {
        let time = self.init_time.cumulative();
        let s_data = dest.and_then(|d| {
            if let MetricData::Gauge(gauge) = d {
                Some(gauge)
            } else {
                None
            }
        });
        let mut new_agg = if s_data.is_none() {
            Some(data::Gauge {
                data_points: vec![],
                start_time: Some(time.start),
                time: time.current,
            })
        } else {
            None
        };
        let s_data = s_data.unwrap_or_else(|| new_agg.as_mut().expect("present if s_data is none"));

        s_data.start_time = Some(time.start);
        s_data.time = time.current;

        self.value_map
            .collect_readonly(&mut s_data.data_points, |attributes, aggr| GaugeDataPoint {
                attributes,
                value: aggr.value.get_value(),
                exemplars: vec![],
            });

        (s_data.data_points.len(), new_agg.map(Into::into))
    }
}

impl<T> Measure<T> for LastValue<T>
where
    T: Number,
{
    fn call(&self, measurement: T, attrs: &[KeyValue]) {
        self.filter.apply(attrs, |filtered| {
            self.value_map.measure(measurement, filtered);
        })
    }

    #[cfg(feature = "experimental_metrics_bound_instruments")]
    fn bind(&self, attrs: &[KeyValue]) -> Box<dyn BoundMeasure<T>> {
        let mut bound_attrs = Vec::new();
        self.filter.apply(attrs, |filtered| {
            bound_attrs = filtered.to_vec();
        });
        match self.value_map.bind(&bound_attrs) {
            Some(tracker) => Box::new(BoundLastValueHandle { tracker }),
            None => Box::new(NoopBoundMeasure::new()),
        }
    }
}

impl<T> ComputeAggregation for LastValue<T>
where
    T: Number,
{
    fn call(&self, dest: Option<&mut AggregatedMetrics>) -> (usize, Option<AggregatedMetrics>) {
        let data = dest.and_then(|d| T::extract_metrics_data_mut(d));
        let (len, new) = match self.temporality {
            Temporality::Delta => self.delta(data),
            _ => self.cumulative(data),
        };
        (len, new.map(T::make_aggregated_metrics))
    }
}

#[cfg(all(test, feature = "experimental_metrics_bound_instruments"))]
mod tests {
    use super::*;
    use crate::metrics::data::{AggregatedMetrics, Gauge, MetricData};

    fn extract_gauge(agg: AggregatedMetrics) -> Gauge<u64> {
        match agg {
            AggregatedMetrics::U64(MetricData::Gauge(g)) => g,
            _ => panic!("expected u64 Gauge"),
        }
    }

    /// Direct unit coverage for `LastValue::bind`. Sync `Gauge::bind()` is not yet
    /// exposed in the public API, so the only callers of this code path today
    /// are Views that remap an instrument to `Aggregation::LastValue`. This test
    /// constructs the aggregator directly and exercises the bound handle through
    /// the `Measure` / `BoundMeasure` traits to keep the impl honest.
    #[test]
    fn bind_writes_through_bound_handle() {
        let last_value =
            LastValue::<u64>::new(Temporality::Cumulative, AttributeSetFilter::new(None), 100);
        let attrs = [KeyValue::new("k", "v")];
        let bound = Measure::bind(&last_value, &attrs);

        bound.call(7);
        bound.call(42); // overwrites previous value (LastValue semantics)

        let (count, agg) = ComputeAggregation::call(&last_value, None);
        assert_eq!(count, 1);
        let gauge = extract_gauge(agg.expect("aggregation produced"));
        assert_eq!(gauge.data_points.len(), 1);
        assert_eq!(gauge.data_points[0].value, 42);
        assert_eq!(gauge.data_points[0].attributes, attrs.to_vec());
    }

    #[test]
    fn bound_handle_drop_decrements_bound_count() {
        let last_value =
            LastValue::<u64>::new(Temporality::Delta, AttributeSetFilter::new(None), 100);
        let attrs = [KeyValue::new("k", "v")];

        let bound = Measure::bind(&last_value, &attrs);
        bound.call(5);

        // While the handle exists, the entry's bound_count is 1.
        let trackers = last_value.value_map.trackers.read().unwrap();
        let entry = trackers
            .values()
            .next()
            .expect("entry should exist after bind+call");
        assert_eq!(
            entry.bound_count.load(Ordering::Relaxed),
            1,
            "bound_count should reflect a live handle"
        );
        drop(trackers);

        drop(bound);

        let trackers = last_value.value_map.trackers.read().unwrap();
        let entry = trackers
            .values()
            .next()
            .expect("entry should still exist post-drop");
        assert_eq!(
            entry.bound_count.load(Ordering::Relaxed),
            0,
            "bound_count should drop to 0 after handle drops"
        );
    }
}
