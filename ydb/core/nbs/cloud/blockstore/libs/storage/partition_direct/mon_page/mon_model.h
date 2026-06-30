#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host_stat.h>   // EOperation, OperationCount, THostStat::TErrorsInfo
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host_state.h>   // EHostState

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

#include <array>
#include <optional>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

// Which menu tab is selected.
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

// Tablet-wide values for the Overview tab.
struct TFastPathServiceInfo
{
    ui64 LsnCounter = 0;
    size_t TotalVChunks = 0;
    size_t DbgCount = 0;
};

// Mirror of EHostHealth (model/oracle.h), kept here so the view model does not
// pull the Oracle header; the snapshot builder maps one to the other.
enum class EHostHealthView
{
    Online,
    Sufferer,
    TemporaryOffline,
    Offline,
};

struct THostSnapshot
{
    THostIndex Index = InvalidHostIndex;
    EHostState State = EHostState::Online;
    EHostHealthView Health = EHostHealthView::Online;
    std::array<size_t, OperationCount> InflightByOp{};
    THostStat::TErrorsInfo Errors;
    ui64 PBufferUsedSize = 0;
};

// One Direct Block Group: per-host view for the DBG tab (list rolls these up,
// detail shows the full host table).
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
    // Set when the running service is unavailable (BOOT/INIT or gather error);
    // the page then shows just the header/menu plus this banner.
    std::optional<TString> RuntimeError;
    // Overview tab.
    std::optional<TFastPathServiceInfo> FastPathServiceInfo;
    // DBG tab: every DBG for the list view, or just the selected one for
    // detail.
    TVector<TDbgSnapshot> Dbgs;
    // DBG detail: the index from "?page=dbg&dbg=N" (absent => list view).
    std::optional<ui32> SelectedDbg;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
