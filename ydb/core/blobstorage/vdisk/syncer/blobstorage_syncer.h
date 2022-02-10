#pragma once

#include "defs.h"
#include "syncer_context.h"

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // SYNCER ACTOR CREATOR
    // The actor is responsible for periodic syncing of current vdisk with others.
    // It's an active entity.
    ////////////////////////////////////////////////////////////////////////////
    class TBlobStorageGroupInfo;
    struct TSyncerData;
    IActor* CreateSyncerActor(const TIntrusivePtr<TSyncerContext> &sc,
                              const TIntrusivePtr<TBlobStorageGroupInfo> &info,
                              const TIntrusivePtr<TSyncerData> &syncerData);

} // NKikimr
