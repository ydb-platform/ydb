#pragma once

#include "defs.h"
#include "cms_state.h"
#include "pdiskid.h"

namespace NKikimr {
namespace NCms {

struct TEvSentinel {
    enum EEv {
        EvUpdateConfig = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
        EvConfigUpdated,

        EvUpdateState,
        EvStateUpdated,

        EvStatusChanged,

        EvRetry,
        EvTimeout,

        EvBSCPipeDisconnected,

        EvEnd,
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE)");

    struct TEvUpdateConfig: public TEventLocal<TEvUpdateConfig, EvUpdateConfig> {};
    struct TEvConfigUpdated: public TEventLocal<TEvConfigUpdated, EvConfigUpdated> {};

    struct TEvUpdateState: public TEventLocal<TEvUpdateState, EvUpdateState> {};
    struct TEvStateUpdated: public TEventLocal<TEvStateUpdated, EvStateUpdated> {};

    struct TEvRetry: public TEventLocal<TEvRetry, EvRetry> {};
    struct TEvTimeout: public TEventLocal<TEvTimeout, EvTimeout> {};

    struct TEvStatusChanged: public TEventLocal<TEvStatusChanged, EvStatusChanged> {
        TPDiskID Id;
        bool Success;

        explicit TEvStatusChanged(const TPDiskID& id, bool success)
            : Id(id)
            , Success(success)
        {
        }
    };

    struct TEvBSCPipeDisconnected: public TEventLocal<TEvBSCPipeDisconnected, EvBSCPipeDisconnected> {};
}; // TEvSentinel

IActor* CreateSentinel(TCmsStatePtr state);

} // NCms
} // NKikimr
