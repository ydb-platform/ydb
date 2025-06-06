#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_mongroups.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/blobstorage_hulldefs.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // CreateHullSyncFullHandler
    // VDisk Skeleton Handler for TEvVSyncFull event
    // MUST work on the same mailbox as Skeleton
    ////////////////////////////////////////////////////////////////////////////

    class TDb;
    class THull;
    IActor *CreateHullSyncFullHandler(const TIntrusivePtr<TDb> &db,
                                      const TIntrusivePtr<THullCtx> &hullCtx,
                                      const TVDiskID &selfVDiskId,
                                      const TActorId &parentId,
                                      const std::shared_ptr<THull> &hull,
                                      const std::shared_ptr<NMonGroup::TVDiskIFaceGroup> &ifaceMonGroup,
                                      const std::shared_ptr<NMonGroup::TFullSyncGroup>& fullSyncGroup,
                                      ui64 dbBirthLsn);

} // NKikimr
