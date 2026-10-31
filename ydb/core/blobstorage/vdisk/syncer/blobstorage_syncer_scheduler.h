#pragma once

#include "defs.h"

#include <ydb/core/base/blobstorage.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TEvStartupDataSyncDone
    ////////////////////////////////////////////////////////////////////////////
    struct TEvStartupDataSyncDone
        : public TEventLocal<TEvStartupDataSyncDone, TEvBlobStorage::EvStartupDataSyncDone>
    {};

    struct TEvReinforcementSyncDone
        : public TEventLocal<TEvReinforcementSyncDone, TEvBlobStorage::EvReinforcementSyncDone>
    {
        const ui32 GroupGeneration;

        explicit TEvReinforcementSyncDone(ui32 groupGeneration);
    };

    ////////////////////////////////////////////////////////////////////////////
    // SYNCER ACTOR CREATOR
    ////////////////////////////////////////////////////////////////////////////
    class TSyncerContext;
    class TBlobStorageGroupInfo;
    struct TSyncerData;
    IActor* CreateSyncerSchedulerActor(const TIntrusivePtr<TSyncerContext> &sc,
                                       const TIntrusivePtr<TBlobStorageGroupInfo> &info,
                                       const TIntrusivePtr<TSyncerData> &syncerData,
                                       const TActorId &committerId,
                                       const TActorId &notifyId);

} // NKikimr
