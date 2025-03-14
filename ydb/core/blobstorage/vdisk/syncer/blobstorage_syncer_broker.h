#pragma once

#include "defs.h"
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/base/blobstorage_vdiskid.h>

namespace NKikimr {

    struct TEvQuerySyncToken : public TEventLocal<TEvQuerySyncToken, TEvBlobStorage::EvQuerySyncToken> {
        TActorId VDiskActorId;

        explicit TEvQuerySyncToken(const TActorId& id)
            : VDiskActorId(id)
        {}
    };

    struct TEvSyncToken : public TEventLocal<TEvSyncToken, TEvBlobStorage::EvSyncToken>
    {};

    struct TEvReleaseSyncToken : public TEventLocal<TEvReleaseSyncToken, TEvBlobStorage::EvReleaseSyncToken> {
        TActorId VDiskActorId;

        explicit TEvReleaseSyncToken(const TActorId& id)
            : VDiskActorId(id)
        {}
    };

    extern IActor *CreateSyncBrokerActor();

} // NKikimr
