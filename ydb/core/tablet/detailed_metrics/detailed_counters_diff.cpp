#include "detailed_counters_diff.h"

namespace NKikimr {

void CalculateCountersDiff(
    NKikimrSysView::TDbCounters* diff,
    const NKikimrSysView::TDbCounters& current,
    NKikimrSysView::TDbCounters& prev
) {
    auto simpleSize = current.SimpleSize();
    auto cumulativeSize = current.CumulativeSize();
    auto histogramSize = current.HistogramSize();

    if (prev.SimpleSize() != simpleSize) {
        prev.MutableSimple()->Resize(simpleSize, 0);
    }
    if (prev.CumulativeSize() != cumulativeSize) {
        prev.MutableCumulative()->Resize(cumulativeSize, 0);
    }
    if (prev.HistogramSize() != histogramSize) {
        if (prev.HistogramSize() < histogramSize) {
            auto missing = histogramSize - prev.HistogramSize();
            for (; missing > 0; --missing) {
                prev.AddHistogram();
            }
        }
    }

    diff->MutableSimple()->Reserve(simpleSize);
    diff->MutableCumulative()->Reserve(cumulativeSize);
    diff->MutableHistogram()->Reserve(histogramSize);

    for (size_t i = 0; i < simpleSize; ++i) {
        diff->AddSimple(current.GetSimple(i));
    }

    diff->SetCumulativeCount(cumulativeSize);
    for (size_t i = 0; i < cumulativeSize; ++i) {
        auto value = current.GetCumulative(i) >= prev.GetCumulative(i) ? current.GetCumulative(i) - prev.GetCumulative(i) : 0;
        if (!value) {
            continue;
        }
        diff->AddCumulative(i);
        diff->AddCumulative(value);
    }

    for (size_t i = 0; i < histogramSize; ++i) {
        const auto& currentH = current.GetHistogram(i);
        auto& prevH = *prev.MutableHistogram(i);
        auto bucketCount = currentH.BucketsSize();
        if (prevH.BucketsSize() != bucketCount) {
            prevH.MutableBuckets()->Resize(bucketCount, 0);
        }
        auto* histogram = diff->AddHistogram();
        histogram->MutableBuckets()->Reserve(bucketCount);
        histogram->SetBucketsCount(bucketCount);
        for (size_t b = 0; b < bucketCount; ++b) {
            // A tablet leaving a TABLE-level collapse bucket can shrink an integral
            // percentile aggregate between two packed generations, so the current
            // absolute value can be below the previous one: clamp instead of underflow.
            auto value = currentH.GetBuckets(b) >= prevH.GetBuckets(b) ? currentH.GetBuckets(b) - prevH.GetBuckets(b) : 0;
            if (!value) {
                continue;
            }
            histogram->AddBuckets(b);
            histogram->AddBuckets(value);
        }
    }
}

void CalculateCountersDiff(
    NKikimrSysView::TDbTabletCounters* diff,
    const NKikimrSysView::TDbTabletCounters& current,
    NKikimrSysView::TDbTabletCounters& prev
) {
    diff->SetType(current.GetType());

    CalculateCountersDiff(diff->MutableExecutorCounters(), current.GetExecutorCounters(), *prev.MutableExecutorCounters());
    CalculateCountersDiff(diff->MutableAppCounters(), current.GetAppCounters(), *prev.MutableAppCounters());

    // The Max pair goes through the very same encoding as the sum pair above
    // (Simple absolute, Cumulative sparse with CumulativeCount) because that is
    // what the receiver decodes. Diffing against an empty baseline is precisely
    // the dense->sparse conversion, with no baseline: a max still has no
    // meaningful delta, so the values come out as absolute maxima.
    NKikimrSysView::TDbCounters emptyExecutor;
    NKikimrSysView::TDbCounters emptyApp;
    CalculateCountersDiff(diff->MutableMaxExecutorCounters(), current.GetMaxExecutorCounters(), emptyExecutor);
    CalculateCountersDiff(diff->MutableMaxAppCounters(), current.GetMaxAppCounters(), emptyApp);
}

} // namespace NKikimr
