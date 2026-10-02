#pragma once

#include "ddisk.h"
#include "tablet_stats.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event_local.h>

namespace NKikimr::NDDisk {

struct TTabletIoRate {
    double Iops = 0;
    double BytesPerSecond = 0;
};

inline TTabletIoRate CalculateTabletIoRate(const TTabletIoCounters& previous,
        const TTabletIoCounters& current, TDuration elapsed) {
    if (!elapsed || current.Requests < previous.Requests || current.Bytes < previous.Bytes) {
        return {};
    }
    const double seconds = elapsed.MicroSeconds() / 1e6;
    return {(current.Requests - previous.Requests) / seconds, (current.Bytes - previous.Bytes) / seconds};
}

struct TTabletStats {
    ui64 TabletId = 0;
    ui64 DataMappedChunks = 0;
    std::array<TTabletIoRate, 3> Rates; // ETabletOperation order; logical DDisk I/O only.
    TInstant SampledAt;
    TDuration Interval;
};

struct TEvTabletStatsBatch : NActors::TEventLocal<TEvTabletStatsBatch, TEv::EvTabletStatsBatch> {
    std::vector<TTabletStatsSample> Samples;
    TInstant SampledAt;
};

struct TEvTabletStatsAck : NActors::TEventLocal<TEvTabletStatsAck, TEv::EvTabletStatsAck> {};

// Send to DDisk or directly to its statistics actor. Results are bounded and
// ordered by ID. Samples are asynchronous: SampledAt exposes backlog freshness.
struct TEvGetTabletStats : NActors::TEventLocal<TEvGetTabletStats, TEv::EvGetTabletStats> {
    std::optional<ui64> TabletId;
    std::optional<ui64> AfterTabletId;
    ui32 Limit = TTabletStatsTracker::MaxBatch;
};

struct TEvTabletStats : NActors::TEventLocal<TEvTabletStats, TEv::EvTabletStats> {
    bool Available = true;
    std::vector<TTabletStats> Tablets;
    std::optional<ui64> NextTabletId;
};

NActors::IActor* CreateTabletStatsActor(NActors::TActorId owner);

} // namespace NKikimr::NDDisk
