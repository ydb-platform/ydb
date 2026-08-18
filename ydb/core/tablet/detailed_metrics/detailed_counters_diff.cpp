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
        auto value = current.GetCumulative(i) - prev.GetCumulative(i);
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
            auto value = currentH.GetBuckets(b) - prevH.GetBuckets(b);
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

    // A max has no meaningful delta: copied absolute, same as CopyCounters() does
    // for the very first report in the funnel this is ported from.
    *diff->MutableMaxExecutorCounters() = current.GetMaxExecutorCounters();
    *diff->MutableMaxAppCounters() = current.GetMaxAppCounters();
}

} // namespace NKikimr
