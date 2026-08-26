#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/common/pbuffer_key.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host_stat.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host_state.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/mon_model.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>

#include <ydb/core/mind/bscontroller/types.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

#include <optional>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

enum class EMonPage
{
    Overview,
    Dbg,
    LocalDb,
    VChunk,
    Latency,
};

enum class ELatencyPercentile
{
    P50,
    P90,
    P99,
    Max,
};

struct TTabletInfo
{
    ui64 TabletId = 0;
    ui32 Generation = 0;
    TString DiskId;
    TString State;   // "INIT" / "WORK"
};

struct TFastPathServiceInfo
{
    ui64 LsnCounter = 0;
    // Minimum safe barrier across all DBGs from the last finished cleanup
    // round; 0 until the first round finishes.
    ui64 LastSafeBarrier = 0;
    size_t TotalVChunks = 0;
    size_t DbgCount = 0;
};

struct TConnectionSnapshot
{
    THostIndex HostIndex = InvalidHostIndex;
    NKikimr::NBsController::TDDiskId DDiskId;
    std::optional<NKikimr::NBsController::TDDiskId> PBufferId;
    TString DDiskSession;
    bool PBufferConnected = false;
};

struct TDbgSnapshot
{
    size_t Index = 0;
    size_t VChunkCount = 0;
    TVector<THostSnapshot> Hosts;
    TVector<TConnectionSnapshot> Connections;
    TVChunkConfigs VChunkConfigs;
    // OracleConfig.TimePredictionHistorySize for this DBG (0 => disabled).
    size_t LatencyHistoryCapacity = 0;
};

struct TVChunkSnapshot
{
    TVChunkConfig VChunkConfig;
    std::optional<TPBufferKey> SafeBarrier;
    TString DirtyMapDump;
};

// Persisted tablet state (local DB). Protos are pre-dumped to text; an absent
// value means the row was never persisted.
struct TLocalDbContents
{
    std::optional<TString> VolumeConfig;
    std::optional<TString> DirectBlockGroupsConnections;
    std::optional<TString> AddHostInProgress;
    // Persisted per-vchunk overrides.
    TVChunkConfigs VChunkConfigs;
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
    // VChunk tab: the requested index (absent => only the input form) and the
    // snapshot (absent => no such vchunk).
    std::optional<ui32> SelectedVChunk;
    std::optional<TVChunkSnapshot> VChunk;
    // Latency tab: which percentile colors the heatmap / slot grid, and
    // which operation filters the slot grid (absent => worst across ops).
    ELatencyPercentile SelectedPercentile = ELatencyPercentile::P99;
    std::optional<EOperation> SelectedLatencyOperation;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
