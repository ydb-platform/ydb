#pragma once

#include "defs.h"
#include "cms_state.h"

namespace NKikimr::NCms {

struct TEvSentinel {
    enum EEv {
        EvUpdateConfig = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
        EvConfigUpdated,

        EvUpdateState,
        EvStateUpdated,

        EvTimeout,
        EvBSCPipeDisconnected,

        EvEnd,
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE)");

    struct TEvUpdateConfig: public TEventLocal<TEvUpdateConfig, EvUpdateConfig> {};
    struct TEvConfigUpdated: public TEventLocal<TEvConfigUpdated, EvConfigUpdated> {};

    struct TEvUpdateState: public TEventLocal<TEvUpdateState, EvUpdateState> {};
    struct TEvStateUpdated: public TEventLocal<TEvStateUpdated, EvStateUpdated> {};

    struct TEvTimeout: public TEventLocal<TEvTimeout, EvTimeout> {};
    struct TEvBSCPipeDisconnected: public TEventLocal<TEvBSCPipeDisconnected, EvBSCPipeDisconnected> {};

}; // TEvSentinel

IActor* CreateSentinel(TCmsStatePtr state);

} // namespace NKikimr::NCms
