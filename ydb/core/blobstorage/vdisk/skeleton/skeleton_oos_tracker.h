#pragma once
#include "defs.h"

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // CreateDskSpaceTracker -- tracks (refreshes) actual state of 'out of disk space'
    ////////////////////////////////////////////////////////////////////////////
    class TVDiskContext;
    class TPDiskCtx;
    IActor* CreateDskSpaceTracker(
                TIntrusivePtr<TVDiskContext> vctx,
                std::shared_ptr<TPDiskCtx> pdiskCtx,
                TDuration dskTrackerInterval);

} // NKikimr
