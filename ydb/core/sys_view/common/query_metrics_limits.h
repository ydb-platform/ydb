#pragma once

#include <util/system/types.h>

namespace NKikimr::NSysView::NQueryMetricsLimits {

// Per-node candidates -> processor candidates -> fetched metrics -> public tops.
inline constexpr size_t NodeCandidateCount = 1024;
inline constexpr size_t ProcessorCandidateCount = 1024;
inline constexpr size_t MetricsFetchCount = 1024;
inline constexpr size_t OneMinuteResultCount = 256;
inline constexpr size_t OneHourResultCount = 256;


static_assert(NodeCandidateCount > 0);
static_assert(ProcessorCandidateCount > 0);
static_assert(MetricsFetchCount > 0);
static_assert(MetricsFetchCount <= ProcessorCandidateCount);
static_assert(OneMinuteResultCount > 0);
static_assert(OneMinuteResultCount <= MetricsFetchCount);
static_assert(OneHourResultCount > 0);
static_assert(OneHourResultCount <= MetricsFetchCount);

} // namespace NKikimr::NSysView::NQueryMetricsLimits
