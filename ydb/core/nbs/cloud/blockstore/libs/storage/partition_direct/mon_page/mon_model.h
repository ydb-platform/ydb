#pragma once

<<<<<<< HEAD
=======
#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range/pbuffer_key.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/memory/arena_allocator.h>
#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/vchunk_stats.h>
<<<<<<< HEAD
>>>>>>> 78a20188184 (Field map set optimization (#52565))
=======
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/mon_model.h>
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
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
<<<<<<< HEAD
=======
    ui32 BlockSize = 0;
    ui64 BlockCount = 0;
<<<<<<< HEAD
    ui64 VChunkBlockCount = 0;
    ui64 RegionBlockCount = 0;
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
=======
    ui64 VChunkSize = 0;
    ui32 VolumeDirectBlockGroupCount = 0;
>>>>>>> f32af13259a (Refactor partition direct disk geometry calculations (#53169))
    TString DiskId;
    TString State;   // "INIT" / "WORK"
};

struct TArenaMemoryUsage
{
    TArenaAllocatorStats Slots;
};

struct TFastPathServiceInfo
{
    ui64 LsnCounter = 0;
<<<<<<< HEAD
    size_t TotalVChunks = 0;
    size_t DbgCount = 0;
=======
    // Minimum safe barrier across all DBGs from the last finished cleanup
    // round; 0 until the first round finishes.
    ui64 LastSafeBarrier = 0;
>>>>>>> f32af13259a (Refactor partition direct disk geometry calculations (#53169))

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
    TArenaPoolStats MemoryStats;
    TArenaAllocatorStats DetailedMemoryStats;
    TDirtyMapStats DirtyMapStats;
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
