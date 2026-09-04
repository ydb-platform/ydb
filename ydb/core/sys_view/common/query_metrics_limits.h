#pragma once

#include <util/system/types.h>

namespace NKikimr::NSysView::NQueryMetricsLimits {

// Query metrics resource limits are intentionally compile-time constants.
// Keep the primary tuning knobs in this block so that changing the amount of
// collected data does not require updating independent limits across the node
// service, processor and public result caches.
inline constexpr size_t CandidateCount = 1024;
inline constexpr size_t CollectedCount = 256;

inline constexpr ui64 OneHourHistoryByteLimit = 256ull << 20;
inline constexpr size_t OneHourCleanupBatchSize = 512;

// Derived limits. Candidate caches stay wider than the expensive path that
// fetches full metrics and retains public rows with query texts.
inline constexpr size_t NodeCandidateCount = CandidateCount;
inline constexpr size_t ProcessorCandidateCount = CandidateCount;
inline constexpr size_t MetricsFetchCount = CollectedCount;
inline constexpr size_t OneMinuteResultCount = CollectedCount;
inline constexpr size_t OneHourResultCount = CollectedCount;

static_assert(CandidateCount > 0);
static_assert(CollectedCount > 0);
static_assert(CollectedCount <= CandidateCount);
static_assert(OneHourHistoryByteLimit > 0);
static_assert(OneHourCleanupBatchSize > 0);

} // namespace NKikimr::NSysView::NQueryMetricsLimits
