use std::mem::replace;
use std::ops::DerefMut;
#[cfg(feature = "experimental_metrics_bound_instruments")]
use std::sync::atomic::Ordering;
#[cfg(feature = "experimental_metrics_bound_instruments")]
use std::sync::Arc;
use std::sync::Mutex;

use crate::metrics::data::{self, MetricData};
use crate::metrics::data::{AggregatedMetrics, HistogramDataPoint};
use crate::metrics::Temporality;
use opentelemetry::KeyValue;

use super::aggregate::{AggregateTimeInitiator, AttributeSetFilter};
use super::{Aggregator, ComputeAggregation, Measure, Number, ValueMap};
#[cfg(feature = "experimental_metrics_bound_instruments")]
use super::{BoundMeasure, NoopBoundMeasure, TrackerEntry};

impl<T> Aggregator for Mutex<Buckets<T>>
where
    T: Number,
{
    type InitConfig = usize;
    /// Value and bucket index
    type PreComputedValue = (T, usize);

    fn update(&self, (value, index): (T, usize)) {
        let mut buckets = self.lock().unwrap_or_else(|err| err.into_inner());

        buckets.total += value;
        buckets.count += 1;
        if !buckets.counts.is_empty() {
            buckets.counts[index] += 1;
        }

        if value < buckets.min {
            buckets.min = value;
        }
        if value > buckets.max {
            buckets.max = value
        }
    }

    fn create(count: &usize) -> Self {
        Mutex::new(Buckets::<T>::new(*count))
    }

    fn clone_and_reset(&self, count: &usize) -> Self {
        let mut current = self.lock().unwrap_or_else(|err| err.into_inner());
        Mutex::new(replace(current.deref_mut(), Buckets::new(*count)))
    }
}

#[derive(Default)]
struct Buckets<T> {
    counts: Vec<u64>,
    count: u64,
    total: T,
    min: T,
    max: T,
}

impl<T: Number> Buckets<T> {
    /// returns buckets with `n` bins.
    fn new(n: usize) -> Buckets<T> {
        Buckets {
            counts: vec![0; n],
            min: T::max(),
            max: T::min(),
            ..Default::default()
        }
    }
}

/// Pre-bound histogram handle: writes go directly to a fixed `TrackerEntry`
/// without per-call attribute lookup. The `tracker` is either a dedicated entry
/// for the bound attribute set, or — if bind() hit the cardinality limit — the
/// shared overflow tracker.
#[cfg(feature = "experimental_metrics_bound_instruments")]
struct BoundHistogramHandle<T: Number> {
    tracker: Arc<TrackerEntry<Mutex<Buckets<T>>>>,
    bounds: Vec<f64>,
}

#[cfg(feature = "experimental_metrics_bound_instruments")]
impl<T: Number> BoundMeasure<T> for BoundHistogramHandle<T> {
    fn call(&self, measurement: T) {
        let f = measurement.into_float();
        let index = self.bounds.partition_point(|&x| x < f);
        self.tracker.aggregator.update((measurement, index));
        self.tracker.has_been_updated.store(true, Ordering::Release);
    }
}

#[cfg(feature = "experimental_metrics_bound_instruments")]
impl<T: Number> Drop for BoundHistogramHandle<T> {
    fn drop(&mut self) {
        self.tracker.bound_count.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Summarizes a set of measurements as a histogram with explicitly defined
/// buckets.
pub(crate) struct Histogram<T: Number> {
    value_map: ValueMap<Mutex<Buckets<T>>>,
    init_time: AggregateTimeInitiator,
    temporality: Temporality,
    filter: AttributeSetFilter,
    bounds: Vec<f64>,
    record_min_max: bool,
    record_sum: bool,
}

impl<T: Number> Histogram<T> {
    pub(crate) fn new(
        temporality: Temporality,
        filter: AttributeSetFilter,
        bounds: Vec<f64>,
        record_min_max: bool,
        record_sum: bool,
        cardinality_limit: usize,
    ) -> Self {
        let buckets_count = if bounds.is_empty() {
            0
        } else {
            bounds.len() + 1
        };

        Histogram {
            value_map: ValueMap::new(buckets_count, cardinality_limit),
            init_time: AggregateTimeInitiator::default(),
            temporality,
            filter,
            bounds,
            record_min_max,
            record_sum,
        }
    }

    fn delta(&self, dest: Option<&mut MetricData<T>>) -> (usize, Option<MetricData<T>>) {
        let time = self.init_time.delta();

        let h = dest.and_then(|d| {
            if let MetricData::Histogram(hist) = d {
                Some(hist)
            } else {
                None
            }
        });
        let mut new_agg = if h.is_none() {
            Some(data::Histogram {
                data_points: vec![],
                start_time: time.start,
                time: time.current,
                temporality: Temporality::Delta,
            })
        } else {
            None
        };
        let h = h.unwrap_or_else(|| new_agg.as_mut().expect("present if h is none"));
        h.temporality = Temporality::Delta;
        h.start_time = time.start;
        h.time = time.current;

        let buckets_count = *self.value_map.config();
        self.value_map
            .collect_and_reset(&mut h.data_points, |attributes, aggr| {
                let reset = aggr.clone_and_reset(&buckets_count);
                let b = reset.into_inner().unwrap_or_else(|err| err.into_inner());
                HistogramDataPoint {
                    attributes,
                    count: b.count,
                    bounds: self.bounds.clone(),
                    bucket_counts: b.counts,
                    sum: if self.record_sum {
                        b.total
                    } else {
                        T::default()
                    },
                    min: if self.record_min_max {
                        Some(b.min)
                    } else {
                        None
                    },
                    max: if self.record_min_max {
                        Some(b.max)
                    } else {
                        None
                    },
                    exemplars: vec![],
                }
            });

        (h.data_points.len(), new_agg.map(Into::into))
    }

    fn cumulative(&self, dest: Option<&mut MetricData<T>>) -> (usize, Option<MetricData<T>>) {
        let time = self.init_time.cumulative();
        let h = dest.and_then(|d| {
            if let MetricData::Histogram(hist) = d {
                Some(hist)
            } else {
                None
            }
        });
        let mut new_agg = if h.is_none() {
            Some(data::Histogram {
                data_points: vec![],
                start_time: time.start,
                time: time.current,
                temporality: Temporality::Cumulative,
            })
        } else {
            None
        };
        let h = h.unwrap_or_else(|| new_agg.as_mut().expect("present if h is none"));
        h.temporality = Temporality::Cumulative;
        h.start_time = time.start;
        h.time = time.current;

        self.value_map
            .collect_readonly(&mut h.data_points, |attributes, aggr| {
                let b = aggr.lock().unwrap_or_else(|err| err.into_inner());
                HistogramDataPoint {
                    attributes,
                    count: b.count,
                    bounds: self.bounds.clone(),
                    bucket_counts: b.counts.clone(),
                    sum: if self.record_sum {
                        b.total
                    } else {
                        T::default()
                    },
                    min: if self.record_min_max {
                        Some(b.min)
                    } else {
                        None
                    },
                    max: if self.record_min_max {
                        Some(b.max)
                    } else {
                        None
                    },
                    exemplars: vec![],
                }
            });

        (h.data_points.len(), new_agg.map(Into::into))
    }
}

impl<T> Measure<T> for Histogram<T>
where
    T: Number,
{
    fn call(&self, measurement: T, attrs: &[KeyValue]) {
        let f = measurement.into_float();
        // This search will return an index in the range `[0, bounds.len()]`, where
        // it will return `bounds.len()` if value is greater than the last element
        // of `bounds`. This aligns with the buckets in that the length of buckets
        // is `bounds.len()+1`, with the last bucket representing:
        // `(bounds[bounds.len()-1], +∞)`.
        let index = self.bounds.partition_point(|&x| x < f);

        self.filter.apply(attrs, |filtered| {
            self.value_map.measure((measurement, index), filtered);
        })
    }

    #[cfg(feature = "experimental_metrics_bound_instruments")]
    fn bind(&self, attrs: &[KeyValue]) -> Box<dyn BoundMeasure<T>> {
        let mut bound_attrs = Vec::new();
        self.filter.apply(attrs, |filtered| {
            bound_attrs = filtered.to_vec();
        });
        match self.value_map.bind(&bound_attrs) {
            Some(tracker) => Box::new(BoundHistogramHandle {
                tracker,
                bounds: self.bounds.clone(),
            }),
            // Trackers RwLock is poisoned — return a noop handle so writes
            // silently drop, mirroring `measure()`'s own poison handling.
            None => Box::new(NoopBoundMeasure::new()),
        }
    }
}

impl<T> ComputeAggregation for Histogram<T>
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_buckets_are_selected_correctly() {
        let hist = Histogram::<i64>::new(
            Temporality::Cumulative,
            AttributeSetFilter::new(None),
            vec![1.0, 3.0, 6.0],
            false,
            false,
            2000,
        );
        for v in 1..11 {
            Measure::call(&hist, v, &[]);
        }
        let (count, dp) = ComputeAggregation::call(&hist, None);
        let dp = dp.unwrap();
        let AggregatedMetrics::I64(MetricData::Histogram(dp)) = dp else {
            unreachable!()
        };
        assert_eq!(count, 1);
        assert_eq!(dp.data_points[0].count, 10);
        assert_eq!(dp.data_points[0].bucket_counts.len(), 4);
        assert_eq!(dp.data_points[0].bucket_counts[0], 1); // 1
        assert_eq!(dp.data_points[0].bucket_counts[1], 2); // 2, 3
        assert_eq!(dp.data_points[0].bucket_counts[2], 3); // 4, 5, 6
        assert_eq!(dp.data_points[0].bucket_counts[3], 4); // 7, 8, 9, 10
    }
}
