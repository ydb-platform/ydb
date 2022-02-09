#include "histogram_iter.h"

#include <contrib/libs/hdr_histogram/src/hdr_histogram.h>

namespace NHdr {
    // TBaseHistogramIterator -----------------------------------------------------
    TBaseHistogramIterator::TBaseHistogramIterator()
        : Iter_(new hdr_iter)
    {
    }

    TBaseHistogramIterator::~TBaseHistogramIterator() {
    }

    bool TBaseHistogramIterator::Next() {
        return hdr_iter_next(Iter_.Get());
    }

    i32 TBaseHistogramIterator::GetCountsIndex() const {
        return Iter_->counts_index;
    }

    i32 TBaseHistogramIterator::GetTotalCount() const {
        return Iter_->total_count;
    }

    i64 TBaseHistogramIterator::GetCount() const {
        return Iter_->count;
    }

    i64 TBaseHistogramIterator::GetCumulativeCount() const {
        return Iter_->cumulative_count;
    }

    i64 TBaseHistogramIterator::GetValue() const {
        return Iter_->value;
    }

    i64 TBaseHistogramIterator::GetHighestEquivalentValue() const {
        return Iter_->highest_equivalent_value;
    }

    i64 TBaseHistogramIterator::GetLowestEquivalentValue() const {
        return Iter_->lowest_equivalent_value;
    }

    i64 TBaseHistogramIterator::GetMedianEquivalentValue() const {
        return Iter_->median_equivalent_value;
    }

    i64 TBaseHistogramIterator::GetValueIteratedFrom() const {
        return Iter_->value_iterated_from;
    }

    i64 TBaseHistogramIterator::GetValueIteratedTo() const {
        return Iter_->value_iterated_to;
    }

    // TAllValuesIterator ---------------------------------------------------------

    TAllValuesIterator::TAllValuesIterator(const THistogram& histogram) {
        hdr_iter_init(Iter_.Get(), histogram.GetHdrHistogramImpl());
    }

    // TRecordedValuesIterator ----------------------------------------------------

    TRecordedValuesIterator::TRecordedValuesIterator(const THistogram& histogram) {
        hdr_iter_recorded_init(Iter_.Get(), histogram.GetHdrHistogramImpl());
    }

    i64 TRecordedValuesIterator::GetCountAddedInThisIterationStep() const {
        return Iter_->specifics.recorded.count_added_in_this_iteration_step;
    }

    // TPercentileIterator --------------------------------------------------------

    TPercentileIterator::TPercentileIterator(
        const THistogram& histogram, ui32 ticksPerHalfDistance) {
        hdr_iter_percentile_init(
            Iter_.Get(), histogram.GetHdrHistogramImpl(),
            ticksPerHalfDistance);
    }

    i32 TPercentileIterator::GetTicketsPerHalfDistance() const {
        return Iter_->specifics.percentiles.ticks_per_half_distance;
    }

    double TPercentileIterator::GetPercentileToIterateTo() const {
        return Iter_->specifics.percentiles.percentile_to_iterate_to;
    }

    double TPercentileIterator::GetPercentile() const {
        return Iter_->specifics.percentiles.percentile;
    }

    // TLinearIterator ------------------------------------------------------------

    TLinearIterator::TLinearIterator(
        const THistogram& histogram, i64 valueUnitsPerBucket) {
        hdr_iter_linear_init(
            Iter_.Get(), histogram.GetHdrHistogramImpl(), valueUnitsPerBucket);
    }

    i64 TLinearIterator::GetValueUnitsPerBucket() const {
        return Iter_->specifics.linear.value_units_per_bucket;
    }

    i64 TLinearIterator::GetCountAddedInThisIterationStep() const {
        return Iter_->specifics.linear.count_added_in_this_iteration_step;
    }

    i64 TLinearIterator::GetNextValueReportingLevel() const {
        return Iter_->specifics.linear.next_value_reporting_level;
    }

    i64 TLinearIterator::GetNextValueReportingLevelLowestEquivalent() const {
        return Iter_->specifics.linear.next_value_reporting_level_lowest_equivalent;
    }

    // TLogarithmicIterator -------------------------------------------------------

    TLogarithmicIterator::TLogarithmicIterator(
        const THistogram& histogram, i64 valueUnitsInFirstBucket,
        double logBase) {
        hdr_iter_log_init(
            Iter_.Get(), histogram.GetHdrHistogramImpl(),
            valueUnitsInFirstBucket, logBase);
    }

    double TLogarithmicIterator::GetLogBase() const {
        return Iter_->specifics.log.log_base;
    }

    i64 TLogarithmicIterator::GetCountAddedInThisIterationStep() const {
        return Iter_->specifics.log.count_added_in_this_iteration_step;
    }

    i64 TLogarithmicIterator::GetNextValueReportingLevel() const {
        return Iter_->specifics.log.next_value_reporting_level;
    }

    i64 TLogarithmicIterator::GetNextValueReportingLevelLowestEquivalent() const {
        return Iter_->specifics.log.next_value_reporting_level_lowest_equivalent;
    }

}
