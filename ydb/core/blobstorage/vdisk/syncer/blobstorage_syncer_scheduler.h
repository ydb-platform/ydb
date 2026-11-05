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

    struct TEvSyncerMetadataSync
        : public TEventLocal<TEvSyncerMetadataSync, TEvBlobStorage::EvSyncerMetadataSync> {
        const ui32 Generation;

        explicit TEvSyncerMetadataSync(ui32 generation)
            : Generation(generation)
        {}
    };

    struct TEvSyncerMetadataSyncDone
        : public TEventLocal<TEvSyncerMetadataSyncDone, TEvBlobStorage::EvSyncerMetadataSyncDone> {
        const ui32 Generation;

        explicit TEvSyncerMetadataSyncDone(ui32 generation)
            : Generation(generation)
        {}
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
