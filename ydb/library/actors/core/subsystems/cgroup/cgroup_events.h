#pragma once

#include "cgroup_oom.h"
#include "cgroup_v1.h"
#include "cgroup_v2.h"

#include <ydb/library/actors/core/events.h>

#include <utility>

namespace NActors {

    enum class ECGroupEvent : ui32 {
        V1Stats = EventSpaceBegin(TEvents::ES_CGROUP),
        V2Stats,
        MemoryStats,
        OomAlert,
        OomTrend,
        End,
    };

    static_assert(static_cast<ui32>(ECGroupEvent::End) < EventSpaceEnd(TEvents::ES_CGROUP));

    struct TEvCGroupV1Stats
        : TEventLocal<TEvCGroupV1Stats, static_cast<ui32>(ECGroupEvent::V1Stats)>
    {
        TCGroupV1StatsPtr Stats;

        explicit TEvCGroupV1Stats(TCGroupV1StatsPtr stats)
            : Stats(std::move(stats))
        {
        }
    };

    struct TEvCGroupV2Stats
        : TEventLocal<TEvCGroupV2Stats, static_cast<ui32>(ECGroupEvent::V2Stats)>
    {
        TCGroupV2StatsPtr Stats;

        explicit TEvCGroupV2Stats(TCGroupV2StatsPtr stats)
            : Stats(std::move(stats))
        {
        }
    };

    struct TEvCGroupMemoryStats
        : TEventLocal<TEvCGroupMemoryStats, static_cast<ui32>(ECGroupEvent::MemoryStats)>
    {
        TCGroupMemoryStatsPtr Stats;

        explicit TEvCGroupMemoryStats(TCGroupMemoryStatsPtr stats)
            : Stats(std::move(stats))
        {
        }
    };

    struct TEvCGroupOomAlert
        : TEventLocal<TEvCGroupOomAlert, static_cast<ui32>(ECGroupEvent::OomAlert)>
    {
        TCGroupOomAlert Alert;

        explicit TEvCGroupOomAlert(TCGroupOomAlert alert)
            : Alert(std::move(alert))
        {
        }
    };

    struct TEvCGroupOomTrend
        : TEventLocal<TEvCGroupOomTrend, static_cast<ui32>(ECGroupEvent::OomTrend)>
    {
        // ReadTrend responses are always Active. Subscriptions additionally
        // receive Stopped when their forecast condition ceases to hold.
        ECGroupOomTrendState State;

        // Empty when the calculation completed without finding a trend. A
        // Stopped subscription notification may still contain the current
        // trend when it exists but no longer meets the subscription threshold.
        std::optional<TCGroupOomTrend> Trend;

        explicit TEvCGroupOomTrend(
                std::optional<TCGroupOomTrend> trend,
                ECGroupOomTrendState state = ECGroupOomTrendState::Active)
            : State(state)
            , Trend(std::move(trend))
        {
        }

        explicit TEvCGroupOomTrend(TCGroupOomTrend trend)
            : State(ECGroupOomTrendState::Active)
            , Trend(std::move(trend))
        {
        }
    };

} // namespace NActors
