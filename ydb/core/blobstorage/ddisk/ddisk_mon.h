#pragma once

#include "monitoring_snapshot.h"

#include <library/cpp/time_provider/monotonic.h>
#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/generic/strbuf.h>

#include <array>
#include <map>
#include <optional>
#include <vector>

namespace NKikimr::NDDisk {

struct TDDiskMonQuery {
    static constexpr ui32 MaxRows = 100;
    static constexpr ui32 MaxTabletSessions = 256;
    TString Tab = "overview";
    TString StatsSort = "throughput";
    TString StatsOther;
    std::optional<ui64> SearchTabletId;
    std::optional<ui64> StatsSelectedTabletId;
    ui32 RefreshRate = 0;
    std::optional<ui64> TabletId;
    std::optional<ui32> DirectBlockGroupIndex;
    std::optional<ui64> AfterTabletId;
    std::optional<ui64> AfterVChunk;
    std::optional<ui64> VChunk;
};

struct TDDiskMonField {
    TString Name;
    TString Value;
};

struct TDDiskMonOperation {
    TString Name;
    ui64 Requests = 0;
    ui64 InFlight = 0;
    ui64 ReplyOk = 0;
    ui64 ReplyErr = 0;
    ui64 Bytes = 0;
    ui64 BytesInFlight = 0;
    std::optional<TDDiskMonRate> Rate;
};

struct TDDiskMonConnection {
    ui64 TabletId = 0;
    ui32 DirectBlockGroupIndex = 0;
    ui32 Generation = 0;
    ui64 Sequence = 0;
    ui32 NodeId = 0;
    TString InterconnectSession;
};

struct TDDiskMonTablet {
    ui64 TabletId = 0;
    ui64 DataChunks = 0;
    ui64 UnmappedChunks = 0;
    ui64 PendingAllocation = 0;
    ui64 PendingIntegrity = 0;
};

struct TDDiskMonChunk {
    ui64 VChunk = 0;
    ui32 PhysicalChunk = 0;
    std::optional<ui32> IntegrityChunk;
    std::optional<ui32> IntegritySlot;
    ui32 InFlight = 0;
    ui64 PendingAllocation = 0;
    ui64 PendingIntegrity = 0;
};

struct TDDiskMonSync {
    ui64 Id = 0;
    ui64 TabletId = 0;
    ui32 DirectBlockGroupIndex = 0;
    ui64 VChunk = 0;
    ui64 SourceRanges = 0;
    ui64 PendingSourceRanges = 0;
    TString Error;
};

struct TDDiskMonInfo : TTabletStatsSnapshot {
    std::map<ui64, ui32> StatsColorSlots;
    TDDiskMonMemory Memory;
    // Data, checksums, PB allocation and empty reserve, in bytes.
    std::array<TDDiskMonMemory, 4> SpaceHistory;
    std::optional<double> RateWindowSeconds;
    ui32 OperationLineId = 0;
    TString OperationHistoryError;
    std::vector<TDDiskMonRateSample> RateHistory;
    TInstant CollectedAt;
    TInstant StartedAt;
    TString Id;
    TString ActorId;
    TString State;
    TString BrokenReason;
    TString Backend;
    TString Pool;
    ui32 NodeId = 0;
    ui32 PDiskId = 0;
    ui32 SlotId = 0;
    ui64 ChunkSize = 0;
    ui64 DataChunks = 0;
    ui64 IntegrityChunks = 0;
    ui64 ReservedChunks = 0;
    ui64 AllocationsInFlight = 0;
    ui64 FormattingChunks = 0;
    ui64 PendingRelease = 0;
    ui64 TabletsWithChunks = 0;
    ui64 ConnectionCount = 0;
    ui64 PendingQueries = 0;
    ui64 PendingAllocation = 0;
    ui64 PendingIntegrity = 0;
    ui64 RouterInFlight = 0;
    ui64 SharedIoInFlight = 0;
    bool IoStalled = false;
    bool MoreTablets = false;
    bool MoreChunks = false;
    bool MoreConnections = false;
    bool MoreSyncs = false;
    std::vector<TDDiskMonTablet> Tablets;
    std::vector<TDDiskMonConnection> Connections;
    std::vector<TDDiskMonChunk> Chunks;
    std::vector<TDDiskMonSync> Syncs;
    std::vector<TDDiskMonOperation> Operations;
    std::vector<TDDiskMonOperation> DirectIo;
    std::vector<TDDiskMonField> Identity;
    std::vector<TDDiskMonField> Recovery;
    std::vector<TDDiskMonField> Integrity;
    std::vector<TDDiskMonField> Lifecycle;
};

TString RenderDDiskMonPage(const TDDiskMonInfo& info, const TPersistentBufferMonInfo* pb,
    const TDDiskMonQuery& query, TStringBuf pbError);

} // namespace NKikimr::NDDisk
