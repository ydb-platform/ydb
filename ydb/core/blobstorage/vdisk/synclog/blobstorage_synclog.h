#pragma once

#include "defs.h"
#include "blobstorage_synclog_public_events.h"

namespace NKikimr {

    // forward declarations
    class TBlobStorageGroupInfo;
    namespace NSyncLog {
        struct TSyncLogRepaired;
    }

    ////////////////////////////////////////////////////////////////////////////
    // SYNC LOG ACTOR CREATOR
    // It creates an actor that represent SyncLog. Other VDisks (peers) syncs
    // with current vdisk by applying its sync log.
    ////////////////////////////////////////////////////////////////////////////
    IActor* CreateSyncLogActor(
            const TIntrusivePtr<NSyncLog::TSyncLogCtx> &slCtx,
            const TIntrusivePtr<TBlobStorageGroupInfo> &ginfo,
            const TVDiskID &selfVDiskId,
            // state we got after local recovery
            std::unique_ptr<NSyncLog::TSyncLogRepaired> repaired);

} // NKikimr
