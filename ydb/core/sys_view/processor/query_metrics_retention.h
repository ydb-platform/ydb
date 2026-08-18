#pragma once

#include <util/generic/map.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

namespace NKikimr::NSysView {

struct TQueryMetricsRetentionPlan {
    TVector<ui64> BucketsToEvict;
    ui64 EvictBeforeHourEnd = 0;
    ui64 RetainedBytes = 0;
};

inline TQueryMetricsRetentionPlan PlanQueryMetricsRetention(
    const TMap<ui64, ui64>& bucketBytes,
    ui64 activeHourEnd,
    ui64 byteLimit)
{
    TQueryMetricsRetentionPlan result;
    for (const auto& [_, bytes] : bucketBytes) {
        result.RetainedBytes += bytes;
    }

    for (const auto& [hourEnd, bytes] : bucketBytes) {
        if (result.RetainedBytes <= byteLimit) {
            break;
        }
        if (hourEnd == activeHourEnd) {
            continue;
        }
        result.BucketsToEvict.push_back(hourEnd);
        result.RetainedBytes -= bytes;
    }

    if (!result.BucketsToEvict.empty()) {
        auto firstRetained = bucketBytes.upper_bound(result.BucketsToEvict.back());
        result.EvictBeforeHourEnd = firstRetained != bucketBytes.end()
            ? firstRetained->first
            : result.BucketsToEvict.back() + 1;
    }

    return result;
}

} // namespace NKikimr::NSysView
