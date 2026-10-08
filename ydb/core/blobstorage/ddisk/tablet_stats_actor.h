#pragma once

#include "ddisk.h"
#include "monitoring_snapshot.h"
#include "tablet_stats.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event_local.h>

namespace NKikimr::NDDisk {

struct TTabletIoRate {
    float Iops = 0;
    float BytesPerSecond = 0;
};

inline TTabletIoRate CalculateTabletIoRate(const TTabletIoCounters& previous,
        const TTabletIoCounters& current, TDuration elapsed) {
    if (!elapsed || current.Requests < previous.Requests || current.Bytes < previous.Bytes) {
        return {};
    }
    const double seconds = elapsed.MicroSeconds() / 1e6;
    return {static_cast<float>((current.Requests - previous.Requests) / seconds),
        static_cast<float>((current.Bytes - previous.Bytes) / seconds)};
}

struct TTabletStats {
    ui64 TabletId = 0;
    ui64 DataMappedChunks = 0;
    std::array<TTabletIoRate, TabletOperationCount> Rates; // ETabletOperation order; logical DDisk I/O only.
    TInstant SampledAt;
    TDuration Interval;
};

struct TEvTabletStatsBatch : NActors::TEventLocal<TEvTabletStatsBatch, TEv::EvTabletStatsBatch> {
    std::vector<TTabletStatsSample> Samples;
    TInstant SampledAt;
    std::optional<TMonotonic> NextDeadline;
};

struct TEvCollectTabletStats : NActors::TEventLocal<TEvCollectTabletStats, TEv::EvCollectTabletStats> {};
struct TEvTabletStatsChanged : NActors::TEventLocal<TEvTabletStatsChanged, TEv::EvTabletStatsChanged> {};

// Send to DDisk. Results are bounded and
// ordered by ID. Samples are asynchronous: SampledAt exposes backlog freshness.
struct TEvGetTabletStats : NActors::TEventLocal<TEvGetTabletStats, TEv::EvGetTabletStats> {
    std::optional<ui64> TabletId;
    std::optional<ui64> AfterTabletId;
    ui32 Limit = TTabletStatsLimits::MaxBatch;
    TString RankBy; // Empty: ID pagination; otherwise iops, throughput or chunks.
};

struct TEvTabletStats : NActors::TEventLocal<TEvTabletStats, TEv::EvTabletStats> {
    bool Available = true;
    ui64 TotalChunks = 0;
    double TotalIops = 0;
    double TotalBytesPerSecond = 0;
    std::vector<TTabletStats> Tablets;
    std::optional<ui64> NextTabletId;
};

struct TEvGetTabletStatsSnapshot : NActors::TEventLocal<TEvGetTabletStatsSnapshot, TEv::EvGetTabletStatsSnapshot> {
    TTabletStatsSnapshotQuery Query;
};

struct TEvTabletStatsSnapshot : NActors::TEventLocal<TEvTabletStatsSnapshot, TEv::EvTabletStatsSnapshot> {
    TTabletStatsSnapshot Info;
};

NActors::IActor* CreateTabletStatsActor(NActors::TActorId owner);

} // namespace NKikimr::NDDisk
