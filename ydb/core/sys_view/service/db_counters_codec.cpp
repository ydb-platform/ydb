#include "db_counters_codec.h"

#include <ydb/library/actors/core/log.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::SYSTEM_VIEWS

namespace NKikimr {
namespace NSysView {

void CopyCounters(NKikimrSysView::TDbCounters* diff,
    const NKikimrSysView::TDbCounters& current)
{
    auto simpleSize = current.SimpleSize();
    auto cumulativeSize = current.CumulativeSize();
    auto histogramSize = current.HistogramSize();

    diff->MutableSimple()->Reserve(simpleSize);
    diff->MutableCumulative()->Reserve(cumulativeSize);
    diff->MutableHistogram()->Reserve(histogramSize);

    for (size_t i = 0; i < simpleSize; ++i) {
        diff->AddSimple(current.GetSimple(i));
    }

    diff->SetCumulativeCount(cumulativeSize);
    for (size_t i = 0; i < cumulativeSize; ++i) {
        auto value = current.GetCumulative(i);
        if (!value) {
            continue;
        }
        diff->AddCumulative(i);
        diff->AddCumulative(value);
    }

    for (size_t i = 0; i < histogramSize; ++i) {
        const auto& currentH = current.GetHistogram(i);
        auto bucketCount = currentH.BucketsSize();

        auto* histogram = diff->AddHistogram();
        histogram->MutableBuckets()->Reserve(bucketCount);
        histogram->SetBucketsCount(bucketCount);
        for (size_t b = 0; b < bucketCount; ++b) {
            auto value = currentH.GetBuckets(b);
            if (!value) {
                continue;
            }
            histogram->AddBuckets(b);
            histogram->AddBuckets(value);
        }
    }
}

void CalculateCountersDiff(NKikimrSysView::TDbCounters* diff,
    const NKikimrSysView::TDbCounters& current,
    NKikimrSysView::TDbCounters& prev)
{
    auto simpleSize = current.SimpleSize();
    auto cumulativeSize = current.CumulativeSize();
    auto histogramSize = current.HistogramSize();

    if (prev.SimpleSize() != simpleSize) {
        YDB_LOG_CRIT("CalculateCountersDiff: simple counter count mismatch",
            {"prevSimpleSize", prev.SimpleSize()},
            {"currentSimpleSize", simpleSize});
        prev.MutableSimple()->Resize(simpleSize, 0);
    }
    if (prev.CumulativeSize() != cumulativeSize) {
        YDB_LOG_CRIT("CalculateCountersDiff: cumulative counter count mismatch",
            {"prevCumulativeSize", prev.CumulativeSize()},
            {"currentCumulativeSize", cumulativeSize});
        prev.MutableCumulative()->Resize(cumulativeSize, 0);
    }
    if (prev.HistogramSize() != histogramSize) {
        YDB_LOG_CRIT("CalculateCountersDiff: histogram counter count mismatch",
            {"prevHistogramSize", prev.HistogramSize()},
            {"currentHistogramSize", histogramSize});
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
            YDB_LOG_CRIT("CalculateCountersDiff: histogram bucket count mismatch",
                {"histogramIndex", i},
                {"prevBucketCount", prevH.BucketsSize()},
                {"currentBucketCount", bucketCount});
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

void ResetSimpleCounters(NKikimrSysView::TDbCounters* dst) {
    auto simpleSize = dst->SimpleSize();
    auto* to = dst->MutableSimple();
    for (size_t i = 0; i < simpleSize; ++i) {
        (*to)[i] = 0;
    }
}

void ResetMaxCounters(NKikimrSysView::TDbCounters* dst) {
    ResetSimpleCounters(dst);
    auto cumulativeSize = dst->CumulativeSize();
    auto* to = dst->MutableCumulative();
    for (size_t i = 0; i < cumulativeSize; ++i) {
        (*to)[i] = 0;
    }
}

} // NSysView
} // NKikimr
