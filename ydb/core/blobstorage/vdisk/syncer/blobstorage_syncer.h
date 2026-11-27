#pragma once

#include "defs.h"
#include <ydb/core/base/blobstorage.h>
#include "syncer_context.h"

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // SYNCER ACTOR CREATOR
    // The actor is responsible for periodic syncing of current vdisk with others.
    // It's an active entity.
    ////////////////////////////////////////////////////////////////////////////
    class TBlobStorageGroupInfo;
    struct TSyncerData;

    struct TEvGenerationMetadataReplicated
        : TEventLocal<TEvGenerationMetadataReplicated, TEvBlobStorage::EvGenerationMetadataReplicated>
    {
        ui32 Generation;

        explicit TEvGenerationMetadataReplicated(ui32 generation)
            : Generation(generation)
        {}
    };
    IActor* CreateSyncerActor(const TIntrusivePtr<TSyncerContext> &sc,
                              const TIntrusivePtr<TBlobStorageGroupInfo> &info,
                              const TIntrusivePtr<TSyncerData> &syncerData);

} // NKikimr
