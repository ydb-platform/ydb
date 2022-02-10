#include "histogram.h"

#include <util/generic/cast.h>
#include <util/generic/yexception.h>

#include <contrib/libs/hdr_histogram/src/hdr_histogram.h>

namespace NHdr {
    namespace {
        struct hdr_histogram* CreateHistogram(
            i64 lowestDiscernibleValue, i64 highestTrackableValue,
            i32 numberOfSignificantValueDigits, IAllocator* allocator) {
            struct hdr_histogram_bucket_config cfg;

            int r = hdr_calculate_bucket_config(
                lowestDiscernibleValue, highestTrackableValue,
                numberOfSignificantValueDigits, &cfg);
            if (r) {
                ythrow yexception() << "illegal arguments values";
            }

            size_t histogramSize = sizeof(struct hdr_histogram) +
                                   cfg.counts_len * sizeof(i64);

            IAllocator::TBlock mem = allocator->Allocate(histogramSize);
            struct hdr_histogram* histogram =
                reinterpret_cast<struct hdr_histogram*>(mem.Data);

            // memset will ensure that all of the function pointers are null
            memset(histogram, 0, histogramSize);

            hdr_init_preallocated(histogram, &cfg);
            return histogram;
        }

    }

    THistogram::THistogram(i64 lowestDiscernibleValue, i64 highestTrackableValue,
                           i32 numberOfSignificantValueDigits, IAllocator* allocator)
        : Data_(CreateHistogram(
              lowestDiscernibleValue, highestTrackableValue,
              numberOfSignificantValueDigits, allocator))
        , Allocator_(allocator)
    {
    }

    THistogram::~THistogram() {
        if (Data_) {
            size_t size = GetMemorySize();
            Allocator_->Release({Data_.Release(), size});
        }
    }

    // Histogram structure querying support -----------------------------------

    i64 THistogram::GetLowestDiscernibleValue() const {
        return Data_->lowest_trackable_value;
    }

    i64 THistogram::GetHighestTrackableValue() const {
        return Data_->highest_trackable_value;
    }

    i32 THistogram::GetNumberOfSignificantValueDigits() const {
        return Data_->significant_figures;
    }

    size_t THistogram::GetMemorySize() const {
        return hdr_get_memory_size(Data_.Get());
    }

    i32 THistogram::GetCountsLen() const {
        return Data_->counts_len;
    }

    i64 THistogram::GetTotalCount() const {
        return Data_->total_count;
    }

    // Value recording support ------------------------------------------------

    bool THistogram::RecordValue(i64 value) {
        return hdr_record_value(Data_.Get(), value);
    }

    bool THistogram::RecordValues(i64 value, i64 count) {
        return hdr_record_values(Data_.Get(), value, count);
    }

    bool THistogram::RecordValueWithExpectedInterval(i64 value, i64 expectedInterval) {
        return hdr_record_corrected_value(Data_.Get(), value, expectedInterval);
    }

    bool THistogram::RecordValuesWithExpectedInterval(
        i64 value, i64 count, i64 expectedInterval) {
        return hdr_record_corrected_values(
            Data_.Get(), value, count, expectedInterval);
    }

    i64 THistogram::Add(const THistogram& rhs) {
        return hdr_add(Data_.Get(), rhs.Data_.Get());
    }

    i64 THistogram::AddWithExpectedInterval(const THistogram& rhs, i64 expectedInterval) {
        return hdr_add_while_correcting_for_coordinated_omission(
            Data_.Get(), rhs.Data_.Get(), expectedInterval);
    }

    // Histogram Data access support ------------------------------------------

    i64 THistogram::GetMin() const {
        return hdr_min(Data_.Get());
    }

    i64 THistogram::GetMax() const {
        return hdr_max(Data_.Get());
    }

    double THistogram::GetMean() const {
        return hdr_mean(Data_.Get());
    }

    double THistogram::GetStdDeviation() const {
        return hdr_stddev(Data_.Get());
    }

    i64 THistogram::GetValueAtPercentile(double percentile) const {
        return hdr_value_at_percentile(Data_.Get(), percentile);
    }

    i64 THistogram::GetCountAtValue(i64 value) const {
        return hdr_count_at_value(Data_.Get(), value);
    }

    bool THistogram::ValuesAreEqual(i64 v1, i64 v2) const {
        return hdr_values_are_equivalent(Data_.Get(), v1, v2);
    }

    i64 THistogram::GetLowestEquivalentValue(i64 value) const {
        return hdr_lowest_equivalent_value(Data_.Get(), value);
    }

    i64 THistogram::GetHighestEquivalentValue(i64 value) const {
        return hdr_next_non_equivalent_value(Data_.Get(), value) - 1;
    }

    i64 THistogram::GetMedianEquivalentValue(i64 value) const {
        return hdr_median_equivalent_value(Data_.Get(), value);
    }

    void THistogram::Reset() {
        hdr_reset(Data_.Get());
    }

}
