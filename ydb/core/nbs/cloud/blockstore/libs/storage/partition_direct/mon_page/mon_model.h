#pragma once

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
    Overview,
    Dbg,
    LocalDb,
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
    size_t TotalVChunks = 0;
    size_t DbgCount = 0;
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

struct TConnectionSnapshot
{
    THostIndex HostIndex = InvalidHostIndex;
    TString DDiskId;
    TString DDiskPageUrl;
    TString PBufferId;
    TString PBufferPageUrl;
    TString DDiskSession;
    bool PBufferConnected = false;
};

struct TDbgSnapshot
{
    size_t Index = 0;
    size_t VChunkCount = 0;
    TVector<THostSnapshot> Hosts;
    TVector<TConnectionSnapshot> Connections;
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
