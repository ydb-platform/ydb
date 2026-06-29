#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host_stat.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host_state.h>

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

// Mirror of model EHostHealth, to avoid the Oracle header dependency.
enum class EHostHealthView
{
    Online,
    Sufferer,
    TemporaryOffline,
    Offline,

    // Must remain the last entry. Used to size per-health containers.
    Count_,
};

inline constexpr size_t EHostHealthViewCount =
    static_cast<size_t>(EHostHealthView::Count_);

struct THostSnapshot
{
    THostIndex Index = InvalidHostIndex;
    EHostState State = EHostState::Online;
    EHostHealthView Health = EHostHealthView::Online;
    TInflightByOperation InflightByOperation{};
    THostStat::TErrorsInfo Errors;
    ui64 PBufferUsedSize = 0;
};

struct TDbgSnapshot
{
    size_t Index = 0;
    size_t VChunkCount = 0;
    TVector<THostSnapshot> Hosts;
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
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
