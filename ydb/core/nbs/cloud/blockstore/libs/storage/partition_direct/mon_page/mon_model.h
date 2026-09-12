#pragma once

<<<<<<< HEAD
=======
#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range/pbuffer_key.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/memory/arena_allocator.h>
#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/vchunk_stats.h>
>>>>>>> 78a20188184 (Field map set optimization (#52565))
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host_stat.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host_state.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/oracle.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

#include <optional>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

enum class EMonPage
{
<<<<<<< HEAD
    Overview,
    Dbg,
    LocalDb,
=======
    Overview,         // Tablet summary.
    Dbg,              // Runtime state of direct block groups.
    Chaos,            // Transport failure controls.
    LocalDb,          // Persisted tablet state.
    VChunk,           // State of one vchunk.
    VChunkCounters,   // Vchunk operation counters.
    Latency,          // Per-node and per-slot latency.
    Memory,           // Memory usage by direct block group.
>>>>>>> 78a20188184 (Field map set optimization (#52565))
};

struct TTabletInfo
{
    ui64 TabletId = 0;
    ui32 Generation = 0;
    TString DiskId;
    TString State;   // "INIT" / "WORK"
};

struct TArenaMemoryUsage
{
    TVector<TArenaAllocatorStats> Slots;
};

struct TFastPathServiceInfo
{
    ui64 LsnCounter = 0;
    size_t TotalVChunks = 0;
    size_t DbgCount = 0;

    TArenaMemoryUsage ArenaMemoryUsage;
};

struct THostSnapshot
{
    THostIndex Index = InvalidHostIndex;
    EHostState State = EHostState::Online;
    EHostHealth Health = EHostHealth::Online;
    TInflightByOperation InflightByOperation{};
    THostStat::TErrorsInfo Errors;
    ui64 PBufferUsedSize = 0;
};

struct TDbgSnapshot
{
    size_t Index = 0;
    size_t VChunkCount = 0;
    TVector<THostSnapshot> Hosts;
<<<<<<< HEAD
=======
    TVector<TConnectionSnapshot> Connections;
    TVChunkConfigs VChunkConfigs;
    size_t AllocatedMemorySize = 0;
    size_t UsedMemorySize = 0;
    // OracleConfig.TimePredictionHistorySize for this DBG (0 => disabled).
    size_t LatencyHistoryCapacity = 0;
};

struct TVChunkSnapshot
{
    TVChunkConfig VChunkConfig;
    std::optional<TPBufferKey> SafeBarrier;
    TString DirtyMapDump;
};

// One vchunk's stats as collected on its DBG executor.
struct TVChunkStatsSnapshot
{
    ui32 VChunkIndex = 0;
    size_t DbgIndex = 0;
    TVChunkStats Stats;
};

// One DBG's aggregated vchunk stats.
struct TVChunkDbgStats
{
    size_t DbgIndex = 0;
    TVChunkStats Stats;
};

// Result of a disk-wide or per-DBG vchunk stats gather.
struct TVChunkStatsGatherResult
{
    size_t DbgIndex = 0;
    TVChunkStats Total;
    TVector<TVChunkDbgStats> PerDbg;
    TVector<TVChunkStatsSnapshot> PerVChunk;
>>>>>>> 78a20188184 (Field map set optimization (#52565))
};

// Persisted tablet state (local DB). Protos are pre-dumped to text; an absent
// value means the row was never persisted.
struct TLocalDbContents
{
    std::optional<TString> VolumeConfig;
    std::optional<TString> DirectBlockGroupsConnections;
    std::optional<TString> AddHostInProgress;
    // Persisted per-vchunk overrides.
    TVector<TVChunkConfig> VChunkConfigs;
};

struct TMonPageData
{
    EMonPage Page = EMonPage::Overview;
    TTabletInfo TabletInfo;
    // When set, the page shows only the header/menu plus this message.
    std::optional<TString> RuntimeError;
    std::optional<TFastPathServiceInfo> FastPathServiceInfo;
    // DBG tab: all DBGs (list) or the selected one (detail).
    TVector<TDbgSnapshot> Dbgs;
    // DBG detail index (absent => list view).
    std::optional<ui32> SelectedDbg;
    // Local DB tab.
    std::optional<TLocalDbContents> LocalDb;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
