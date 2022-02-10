#pragma once

#include "defs.h"

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // CreateHullLogCutterNotifier
    // It periodically notifies LogCutter about Hull first lsn to keep
    ////////////////////////////////////////////////////////////////////////////
    class TVDiskContext;
    using TVDiskContextPtr = TIntrusivePtr<TVDiskContext>;
    struct THullDs;

    IActor* CreateHullLogCutterNotifier(
            const TVDiskContextPtr &vctx,
            const TActorId &logCutterId,
            TIntrusivePtr<THullDs> hullDs);

} // NKikimr
