#pragma once

#include "defs.h"

#include <ydb/core/base/blobstorage.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TEvStartupCatchupDone
    ////////////////////////////////////////////////////////////////////////////
    struct TEvStartupCatchupDone
        : public TEventLocal<TEvStartupCatchupDone, TEvBlobStorage::EvStartupCatchupDone>
    {};

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
