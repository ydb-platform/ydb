#pragma once
#include "defs.h"
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_defs.h>

#include <util/generic/map.h>

namespace NKikimr {
namespace NPDisk {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TKeeper params
//
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct TOwnerInfo {
    i64 ChunksOwned;
    TVDiskID VDiskId;
};

struct TKeeperParams {
    // Total number of chunks of the disk
    i64 TotalChunks = 0;

    // Split main chunk pool between this many owners (or 0 for 'split as you go' mode)
    i64 ExpectedOwnerCount = 0;

    // Number of chunks used for format record and system log
    i64 SysLogSize = 0;

    // Number of chunks actually used by the common log at the moment
    i64 CommonLogSize = 0;

    i64 MaxCommonLogChunks = 200;

    // Should be true for disks that have one or more static group
    bool HasStaticGroups = false;

    // Initially owned chunk count for each owner, must be present for all currently present owners
    TMap<TOwner, TOwnerInfo> OwnersInfo;

    NKikimrBlobStorage::TPDiskSpaceColor::E SpaceColorBorder = NKikimrBlobStorage::TPDiskSpaceColor::GREEN;

    // Small disk
    bool SeparateCommonLog = true;
};

} // NPDisk
} // NKikimr

