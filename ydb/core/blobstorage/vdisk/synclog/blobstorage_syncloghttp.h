#pragma once

#include "defs.h"
#include "blobstorage_synclogneighbors.h"
#include <ydb/library/actors/core/mon.h>

namespace NKikimr {

    class TVDiskContext;
    class TBlobStorageGroupInfo;

    namespace NSyncLog {

        ////////////////////////////////////////////////////////////////////////////
        // Create actor that handles Http Info request to SyncLog
        ////////////////////////////////////////////////////////////////////////////
        IActor* CreateGetHttpInfoActor(const TIntrusivePtr<TVDiskContext> &vctx,
                                       const TIntrusivePtr<TBlobStorageGroupInfo> &ginfo,
                                       NMon::TEvHttpInfo::TPtr &ev,
                                       const TActorId &notifyId,
                                       const TActorId &keeperId,
                                       TSyncLogNeighborsPtr neighborsPtr);

    } // NSyncLog
} // NKikimr
