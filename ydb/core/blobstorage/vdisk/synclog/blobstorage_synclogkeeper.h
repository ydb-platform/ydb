#pragma once

#include "defs.h"

namespace NKikimr {

    struct TSyncLogRepaired;

    namespace NSyncLog {

        class TSyncLogCtx;

        ////////////////////////////////////////////////////////////////////////////
        // CreateSyncLogKeeperActor
        // Actor responsible for managing SyncLog
        ////////////////////////////////////////////////////////////////////////////
        // syncLogId is the SyncLog actor that forwards TEvSyncLogPut to the keeper; the committer
        // reports through it (see TSyncLogKeeperActor::SyncLogId)
        IActor* CreateSyncLogKeeperActor(
                TIntrusivePtr<TSyncLogCtx> slCtx,
                std::unique_ptr<TSyncLogRepaired> repaired,
                const TActorId &syncLogId);

    } // NSyncLog
} // NKikimr
