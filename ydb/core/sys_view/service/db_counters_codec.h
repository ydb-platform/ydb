#pragma once

#include <ydb/core/protos/sys_view.pb.h>

#include <algorithm>

namespace NKikimr {
namespace NSysView {

template <bool IsMax>
struct TAggregateCumulative {
    static void Apply(NKikimrSysView::TDbCounters* dst, const NKikimrSysView::TDbCounters& src) {
        auto cumulativeSize = src.GetCumulativeCount();
        auto histogramSize = src.HistogramSize();

        if (dst->CumulativeSize() < cumulativeSize) {
            dst->MutableCumulative()->Resize(cumulativeSize, 0);
        }
        if (dst->HistogramSize() < histogramSize) {
            auto missing = histogramSize - dst->HistogramSize();
            for (; missing > 0; --missing) {
                dst->AddHistogram();
            }
        }

        const auto& from = src.GetCumulative();
        auto* to = dst->MutableCumulative();
        auto doubleDiffSize = from.size() / 2 * 2;
        for (int i = 0; i < doubleDiffSize; ) {
            auto index = from[i++];
            auto value = from[i++];
            if (index >= cumulativeSize) {
                continue;
            }
            if constexpr (!IsMax) {
                (*to)[index] += value;
            } else {
                (*to)[index] = std::max(value, (*to)[index]);
            }
        }
        for (size_t i = 0; i < histogramSize; ++i) {
            const auto& histogram = src.GetHistogram(i);
            const auto& from = histogram.GetBuckets();
            auto* to = dst->MutableHistogram(i)->MutableBuckets();
            auto bucketCount = histogram.GetBucketsCount();
            if (to->size() < (int)bucketCount) {
                to->Resize(bucketCount, 0);
            }
            auto doubleDiffSize = from.size();
            for (int b = 0; b < doubleDiffSize; ) {
                auto index = from[b++];
                auto value = from[b++];
                if (index >= bucketCount) {
                    continue;
                }
                if constexpr (!IsMax) {
                    (*to)[index] += value;
                } else {
                    (*to)[index] = std::max(value, (*to)[index]);
                }
            }
        }
    }
};

template <bool IsMax>
struct TAggregateSimple {
    static void Apply(NKikimrSysView::TDbCounters* dst, const NKikimrSysView::TDbCounters& src) {
        auto simpleSize = src.SimpleSize();
        if (dst->SimpleSize() < simpleSize) {
            dst->MutableSimple()->Resize(simpleSize, 0);
        }
        const auto& from = src.GetSimple();
        auto* to = dst->MutableSimple();
        for (size_t i = 0; i < simpleSize; ++i) {
            if constexpr (!IsMax) {
                (*to)[i] += from[i];
            } else {
                (*to)[i] = std::max(from[i], (*to)[i]);
            }
        }
    }
};

void CopyCounters(NKikimrSysView::TDbCounters* diff,
    const NKikimrSysView::TDbCounters& current);

void CalculateCountersDiff(NKikimrSysView::TDbCounters* diff,
    const NKikimrSysView::TDbCounters& current,
    NKikimrSysView::TDbCounters& prev);

void ResetSimpleCounters(NKikimrSysView::TDbCounters* dst);
void ResetMaxCounters(NKikimrSysView::TDbCounters* dst);

// Clear output and encode an absolute snapshot when prev is absent, or a delta
// otherwise. Unsigned subtraction and addition reconstruct decreases modulo
// 2^64, including histogram buckets that shrink or become empty.
void CalculateCountersDiff(NKikimrSysView::TDbCounters* diff,
    const NKikimrSysView::TDbCounters& current,
    NKikimrSysView::TDbCounters* prev = nullptr);

// Executor/App use deltas; their MAX counterparts are always absolute.
void CalculateCountersDiff(NKikimrSysView::TDbTabletCounters* diff,
    const NKikimrSysView::TDbTabletCounters& current,
    NKikimrSysView::TDbTabletCounters* prev = nullptr);

} // NSysView
} // NKikimr
