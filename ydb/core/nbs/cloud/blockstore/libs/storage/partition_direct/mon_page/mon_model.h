#pragma once

#include <util/generic/string.h>
#include <util/system/types.h>

#include <optional>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

enum class EMonPage
{
    Overview,
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

struct TMonPageData
{
    EMonPage Page = EMonPage::Overview;
    TTabletInfo TabletInfo;
    // Absent until TFastPathService is up (BOOT/INIT); the page then shows just
    // the header plus an "initializing" banner.
    std::optional<TFastPathServiceInfo> FastPathServiceInfo;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
