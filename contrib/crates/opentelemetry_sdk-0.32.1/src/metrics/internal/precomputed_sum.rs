use opentelemetry::KeyValue;

use crate::metrics::data::{self, AggregatedMetrics, MetricData, SumDataPoint};
use crate::metrics::Temporality;
#[cfg(feature = "experimental_metrics_bound_instruments")]
use std::sync::atomic::Ordering;
#[cfg(feature = "experimental_metrics_bound_instruments")]
use std::sync::Arc;

use super::aggregate::{AggregateTimeInitiator, AttributeSetFilter};
#[cfg(feature = "experimental_metrics_bound_instruments")]
use super::Aggregator;
use super::{last_value::Assign, AtomicTracker, Number, ValueMap};
#[cfg(feature = "experimental_metrics_bound_instruments")]
use super::{BoundMeasure, NoopBoundMeasure, TrackerEntry};
use super::{ComputeAggregation, Measure};
use std::{collections::HashMap, sync::Mutex};

/// Pre-bound precomputed-sum handle. Writes go directly to a fixed
/// `TrackerEntry`. PrecomputedSum is used by asynchronous instruments
/// (ObservableCounter / ObservableUpDownCounter), which do not expose `bind()`
/// to user code, so this impl exists to satisfy the `Measure` trait and is
/// not reachable via the public API today. The implementation mirrors
/// `BoundLastValueHandle` since both share the `Assign<T>` aggregator.
#[cfg(feature = "experimental_metrics_bound_instruments")]
struct BoundPrecomputedSumHandle<T: Number> {
    tracker: Arc<TrackerEntry<Assign<T>>>,
}

#[cfg(feature = "experimental_metrics_bound_instruments")]
impl<T: Number> BoundMeasure<T> for BoundPrecomputedSumHandle<T> {
    fn call(&self, measurement: T) {
        self.tracker.aggregator.update(measurement);
        self.tracker.has_been_updated.store(true, Ordering::Release);
    }
}

#[cfg(feature = "experimental_metrics_bound_instruments")]
impl<T: Number> Drop for BoundPrecomputedSumHandle<T> {
    fn drop(&mut self) {
        self.tracker.bound_count.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Summarizes a set of pre-computed sums as their arithmetic sum.
pub(crate) struct PrecomputedSum<T: Number> {
    value_map: ValueMap<Assign<T>>,
    init_time: AggregateTimeInitiator,
    temporality: Temporality,
    filter: AttributeSetFilter,
    monotonic: bool,
    reported: Mutex<HashMap<Vec<KeyValue>, T>>,
}

impl<T: Number> PrecomputedSum<T> {
    pub(crate) fn new(
        temporality: Temporality,
        filter: AttributeSetFilter,
        monotonic: bool,
        cardinality_limit: usize,
    ) -> Self {
        PrecomputedSum {
            value_map: ValueMap::new((), cardinality_limit),
            init_time: AggregateTimeInitiator::default(),
            temporality,
            filter,
            monotonic,
            reported: Mutex::new(Default::default()),
        }
    }

    pub(crate) fn delta(&self, dest: Option<&mut MetricData<T>>) -> (usize, Option<MetricData<T>>) {
        let time = self.init_time.delta();

        let s_data = dest.and_then(|d| {
            if let MetricData::Sum(sum) = d {
                Some(sum)
            } else {
                None
            }
        });
        let mut new_agg = if s_data.is_none() {
            Some(data::Sum {
                data_points: vec![],
                start_time: time.start,
                time: time.current,
                temporality: Temporality::Delta,
                is_monotonic: self.monotonic,
            })
        } else {
            None
        };
        let s_data = s_data.unwrap_or_else(|| new_agg.as_mut().expect("present if s_data is none"));
        s_data.start_time = time.start;
        s_data.time = time.current;
        s_data.temporality = Temporality::Delta;
        s_data.is_monotonic = self.monotonic;

        let mut reported = match self.reported.lock() {
            Ok(r) => r,
            Err(_) => return (0, None),
        };
        let mut new_reported = HashMap::with_capacity(reported.len());

        self.value_map
            .drain_and_reset(&mut s_data.data_points, |attributes, aggr| {
                let value = aggr.value.get_value();
                new_reported.insert(attributes.clone(), value);
                let delta = value - *reported.get(&attributes).unwrap_or(&T::default());
                SumDataPoint {
                    attributes,
                    value: delta,
                    exemplars: vec![],
                }
            });

        *reported = new_reported;
        drop(reported); // drop before values guard is dropped

        (s_data.data_points.len(), new_agg.map(Into::into))
    }

    pub(crate) fn cumulative(
        &self,
        dest: Option<&mut MetricData<T>>,
    ) -> (usize, Option<MetricData<T>>) {
        let time = self.init_time.cumulative();

        let s_data = dest.and_then(|d| {
            if let MetricData::Sum(sum) = d {
                Some(sum)
            } else {
                None
            }
        });
        let mut new_agg = if s_data.is_none() {
            Some(data::Sum {
                data_points: vec![],
                start_time: time.start,
                time: time.current,
                temporality: Temporality::Cumulative,
                is_monotonic: self.monotonic,
            })
        } else {
            None
        };
        let s_data = s_data.unwrap_or_else(|| new_agg.as_mut().expect("present if s_data is none"));
        s_data.start_time = time.start;
        s_data.time = time.current;
        s_data.temporality = Temporality::Cumulative;
        s_data.is_monotonic = self.monotonic;

        // Use drain_and_reset to remove stale attributes (not observed in current callback)
        // For cumulative, report absolute values (no delta calculation needed)
        self.value_map
            .drain_and_reset(&mut s_data.data_points, |attributes, aggr| SumDataPoint {
                attributes,
                value: aggr.value.get_value(),
                exemplars: vec![],
            });

        (s_data.data_points.len(), new_agg.map(Into::into))
    }
}

impl<T> Measure<T> for PrecomputedSum<T>
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
            Some(tracker) => Box::new(BoundPrecomputedSumHandle { tracker }),
            None => Box::new(NoopBoundMeasure::new()),
        }
    }
}

impl<T> ComputeAggregation for PrecomputedSum<T>
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
    use crate::metrics::data::{AggregatedMetrics, MetricData, Sum};
    use std::sync::atomic::Ordering;

    fn extract_sum(agg: AggregatedMetrics) -> Sum<u64> {
        match agg {
            AggregatedMetrics::U64(MetricData::Sum(s)) => s,
            _ => panic!("expected u64 Sum"),
        }
    }

    /// PrecomputedSum is used by ObservableCounter / ObservableUpDownCounter, which
    /// do not expose `bind()` to user code. The `Measure::bind` impl exists so the
    /// trait is uniform across all aggregators (and future Observable bind()
    /// extensions are mechanical). This test exercises the impl directly so the
    /// otherwise-unreachable code path stays honest.
    #[test]
    fn bind_writes_through_bound_handle() {
        let pre_sum = PrecomputedSum::<u64>::new(
            Temporality::Cumulative,
            AttributeSetFilter::new(None),
            true,
            100,
        );
        let attrs = [KeyValue::new("k", "v")];
        let bound = Measure::bind(&pre_sum, &attrs);

        bound.call(99); // PrecomputedSum semantics: each call assigns the absolute value

        let (count, agg) = ComputeAggregation::call(&pre_sum, None);
        assert_eq!(count, 1);
        let sum = extract_sum(agg.expect("aggregation produced"));
        assert_eq!(sum.data_points.len(), 1);
        assert_eq!(sum.data_points[0].value, 99);
        assert_eq!(sum.data_points[0].attributes, attrs.to_vec());
    }

    #[test]
    fn bound_handle_drop_decrements_bound_count() {
        let pre_sum = PrecomputedSum::<u64>::new(
            Temporality::Delta,
            AttributeSetFilter::new(None),
            true,
            100,
        );
        let attrs = [KeyValue::new("k", "v")];
        let bound = Measure::bind(&pre_sum, &attrs);
        bound.call(5);

        let trackers = pre_sum.value_map.trackers.read().unwrap();
        let entry = trackers
            .values()
            .next()
            .expect("entry should exist after bind+call");
        assert_eq!(entry.bound_count.load(Ordering::Relaxed), 1);
        drop(trackers);

        drop(bound);

        let trackers = pre_sum.value_map.trackers.read().unwrap();
        let entry = trackers
            .values()
            .next()
            .expect("entry should still exist post-drop");
        assert_eq!(entry.bound_count.load(Ordering::Relaxed), 0);
    }
}
