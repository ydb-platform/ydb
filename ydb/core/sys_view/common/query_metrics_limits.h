#pragma once

#include <util/system/types.h>

namespace NKikimr::NSysView::NQueryMetricsLimits {

// Query metrics resource limits are intentionally compile-time constants.
// Collect more candidates than the public top so that recurring queries below
// the minute result cutoff can still accumulate into the hour top.
inline constexpr size_t CandidateCount = 1024;
inline constexpr size_t CollectedCount = 1024;
inline constexpr size_t ResultCount = 256;

inline constexpr ui64 OneHourHistoryByteLimit = 256ull << 20;
inline constexpr size_t OneHourCleanupBatchSize = 512;

// The accumulator retains every collected contribution. Public minute and hour
// results have a separate limit, including the query texts kept in history.
inline constexpr size_t NodeCandidateCount = CandidateCount;
inline constexpr size_t ProcessorCandidateCount = CandidateCount;
inline constexpr size_t MetricsFetchCount = CollectedCount;
inline constexpr size_t OneMinuteResultCount = ResultCount;
inline constexpr size_t OneHourResultCount = ResultCount;

static_assert(CandidateCount > 0);
static_assert(CollectedCount > 0);
static_assert(CollectedCount <= CandidateCount);
static_assert(ResultCount > 0);
static_assert(ResultCount <= CollectedCount);
static_assert(OneHourHistoryByteLimit > 0);
static_assert(OneHourCleanupBatchSize > 0);

} // namespace NKikimr::NSysView::NQueryMetricsLimits
