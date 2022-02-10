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
        IActor* CreateSyncLogKeeperActor(
                TIntrusivePtr<TSyncLogCtx> slCtx,
                std::unique_ptr<TSyncLogRepaired> repaired);

    } // NSyncLog
} // NKikimr
