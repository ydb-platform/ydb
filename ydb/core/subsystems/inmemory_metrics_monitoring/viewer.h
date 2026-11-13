#pragma once

#include <ydb/library/actors/metrics/line_read.h>

namespace NKikimr::NInMemoryMetricsMonitoring {

inline constexpr size_t MaxHistoryPoints = 64 * 1024;
inline constexpr size_t MaxHistoryFields = 128;

TString RenderPage();
TString RenderOverviewPage();
TString SerializeSnapshot(const NActors::TInMemorySnapshot& snapshot,
                          const NActors::TInMemoryMetricsStats& stats,
                          const NActors::TInMemoryMetricsConfig& config,
                          TInstant now, TDuration period, bool includeHistory);

} // namespace NKikimr::NInMemoryMetricsMonitoring
