#pragma once

#include "metric_rates.h"
#include <util/generic/string.h>
#include <array>
#include <map>
#include <optional>
#include <vector>

namespace NKikimr::NDDisk {

struct TPersistentBufferSnapshotQuery {
    static constexpr ui32 MaxRows = 100;
    bool SummaryOnly = false;
    std::optional<ui64> TabletId;
    std::optional<ui32> DirectBlockGroupIndex;
    std::optional<ui64> AfterTabletId;
};

struct TTabletStatsSnapshotQuery {
    static constexpr ui32 MaxRows = 100;
    TString StatsOther;
    std::optional<ui64> SearchTabletId;
    std::optional<ui64> StatsSelectedTabletId;
    // Every page uses current rankings. With unchanged input, a full traversal
    // returns each tablet once. Changes between pages may cause omissions or
    // duplicates when tablets enter or leave the first-page prominent set.
    std::optional<ui64> AfterTabletId;
};

struct TDDiskMonMemorySample {
    TInstant Timestamp;
    ui64 Bytes = 0;
};

struct TDDiskMonMemory {
    TString Error;
    ui32 LineId = 0;
    ui64 Limit = 0;
    std::vector<TDDiskMonMemorySample> Samples;
};

struct TDDiskSpaceMonInfo {
    TInstant CollectedAt;
    ui64 TotalChunks = 0;
    ui64 UsedChunks = 0;
    ui64 FreeChunks = 0;
};

struct TPersistentBufferMonInfo {
    TDDiskMonMemory Memory;
    struct TTablet {
        ui64 TabletId = 0;
        ui64 LiveBytes = 0;
        ui64 Records = 0;
        ui64 Registrations = 0;
    };
    struct TRegistration {
        ui32 DirectBlockGroupIndex = 0;
        bool Registered = false;
        TString RemovalStage;
        std::optional<TInstant> RemovalDeadline;
        std::optional<ui32> BarrierGeneration;
        std::optional<ui64> BarrierLsn;
        ui64 Records = 0;
        ui64 LiveBytes = 0;
    };

    TInstant CollectedAt;
    TString State;
    TString BrokenReason;
    TInstant StartedAt;
    ui64 AllocatedChunks = 0;
    ui64 ChunkSize = 0;
    ui64 MaxChunks = 0;
    ui64 FreeSectors = 0;
    ui64 SectorSize = 0;
    ui64 LiveBytes = 0;
    ui64 CacheBytes = 0;
    ui64 CacheLimit = 0;
    ui64 PendingEvents = 0;
    ui64 DiskOperations = 0;
    ui64 RouterInFlight = 0;
    ui64 RestoringChunks = 0;
    ui64 RestoreReadsInFlight = 0;
    ui64 RegistrationCount = 0;
    std::optional<double> NormalizedOccupancy;
    std::optional<TDDiskSpaceMonInfo> PDiskSpace;
    bool IoStalled = false;
    bool OwnDrainComplete = false;
    bool MoreTablets = false;
    std::vector<TTablet> Tablets;
    std::vector<TRegistration> Registrations;
};

struct TDDiskMonTabletStats {
    ui64 TabletId = 0;
    ui64 Chunks = 0;
    std::array<TDDiskMonRate, 3> Rates;
    TInstant SampledAt;
    TDuration Interval;
};

struct TTabletStatsSnapshot {
    std::vector<TDDiskMonTabletStats> TabletStats;
    std::vector<TDDiskMonTabletStats> StatsShares;
    std::map<ui64, ui32> ParticipantSlots;
    std::optional<ui64> StatsNextTabletId;
    ui64 StatsFilteredTablets = 0;
    bool StatsAvailable = false;
    ui64 StatsTablets = 0;
    ui64 StatsChunks = 0;
    double StatsIops = 0;
    double StatsBytesPerSecond = 0;
};

} // namespace NKikimr::NDDisk
