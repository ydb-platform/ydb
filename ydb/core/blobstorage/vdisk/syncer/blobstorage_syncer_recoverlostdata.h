#pragma once

#include "defs.h"
#include "blobstorage_syncer_defs.h"
#include <ydb/core/blobstorage/vdisk/common/blobstorage_vdisk_guids.h>
#include <ydb/core/base/blobstorage.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TEvSyncerLostDataRecovered
    ////////////////////////////////////////////////////////////////////////////
    struct TEvSyncerLostDataRecovered :
        public TEventLocal<TEvSyncerLostDataRecovered,
                           TEvBlobStorage::EvSyncerLostDataRecovered>
    {
        NSyncer::TLocalSyncerState LocalSyncerState;

        TEvSyncerLostDataRecovered(const NSyncer::TLocalSyncerState &lss)
            : LocalSyncerState(lss)
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // CreateSyncerRecoverLostDataActor
    ////////////////////////////////////////////////////////////////////////////
    class TSyncerContext;
    class TBlobStorageGroupInfo;
    IActor *CreateSyncerRecoverLostDataActor(const TIntrusivePtr<TSyncerContext> &sc,
                                             const TIntrusivePtr<TBlobStorageGroupInfo> &info,
                                             const TActorId &committerId,
                                             const TActorId &notifyId,
                                             TVDiskEternalGuid guid);

} // NKikimr
