#pragma once

#include "defs.h"
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/vdisk/ingress/blobstorage_ingress.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // THandoffSyncLogProxy interface
    // The goal of the proxy is to commit hull changes (i.e. compaction results)
    // only after all changes to recovery log returned success
    // THandoffSyncLogProxy lifecycle description:
    // 1. It starts and waits for TEvHandoffSyncLogDel
    // 2. On TEvHandoffSyncLogDel(finished=false) message the proxy writes
    //    to recovery log and then (after success) writes to SyncLog
    // 3. On TEvHandoffSyncLogDel(finished=true) message the proxy confirms
    //    that all previous writes were successfully committed to disk (i.e.
    //    recovery log). Then it sends TEvHandoffSyncLogFinished message
    //    to an actor which id has been passed as a proxy parameter and dies.
    ////////////////////////////////////////////////////////////////////////////
    struct TEvHandoffSyncLogDel : public TEventLocal<TEvHandoffSyncLogDel, TEvBlobStorage::EvHandoffSyncLogDel> {
        const bool Finished;
        const TLogoBlobID Id;
        const TIngress Ingress;

        TEvHandoffSyncLogDel(const TLogoBlobID &id, const TIngress &ingress)
            : Finished(false)
            , Id(id)
            , Ingress(ingress)
        {}

        TEvHandoffSyncLogDel()
            : Finished(true)
            , Id()
            , Ingress()
        {}
    };

    struct TEvHandoffSyncLogFinished
        : public TEventLocal<TEvHandoffSyncLogFinished, TEvBlobStorage::EvHandoffSyncLogFinished>
    {
        const bool FromProxy = false;
        TEvHandoffSyncLogFinished(bool fromProxy)
            : FromProxy(fromProxy)
        {}
    };

    IActor* CreateHandoffSyncLogProxy(const TActorId &skeletonId, const TActorId &notifyId);

} // NKikimr

