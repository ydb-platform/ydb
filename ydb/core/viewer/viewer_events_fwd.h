#pragma once

#include "viewer.h"

namespace NKikimr::NViewer {

class TViewerRequestEvent;
class TViewerResponseEvent;
class TUpdateSharedCacheTabletRequestEvent;
class TUpdateSharedCacheTabletResponseEvent;

struct TEvViewer {
    enum EEv {
        // requests
        EvViewerRequest = EventSpaceBegin(TKikimrEvents::ES_VIEWER),
        EvViewerResponse,
        EvUpdateSharedCacheTabletRequest,
        EvUpdateSharedCacheTabletResponse,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_VIEWER), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_VIEWER)");

    using TEvViewerRequest = TViewerRequestEvent;
    using TEvViewerResponse = TViewerResponseEvent;
    using TEvUpdateSharedCacheTabletRequest = TUpdateSharedCacheTabletRequestEvent;
    using TEvUpdateSharedCacheTabletResponse = TUpdateSharedCacheTabletResponseEvent;
};

}
