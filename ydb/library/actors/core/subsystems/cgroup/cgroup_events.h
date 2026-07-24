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

} // namespace NActors
