#pragma once

#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/core/event_local.h>
#include "hive.h"
#include "tablet_info.h"
#include "node_info.h"

namespace NKikimr {
namespace NHive {

using namespace NActors;

struct TEvPrivate {
    enum EEv {
        EvKickTablet = EventSpaceBegin(TEvents::ES_PRIVATE),
        EvProcessBootQueue,
        EvProcessDisconnectNode,
        EvPostponeProcessBootQueue,
        EvBootTablets,
        EvCheckTabletNodeAlive,
        EvProcessTabletBalancer,
        EvUnlockTabletReconnectTimeout,
        EvProcessPendingOperations,
        EvRestartComplete,
        EvBalancerOut,
        EvProcessIncomingEvent,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

    struct TEvKickTablet : TEventLocal<TEvKickTablet, EvKickTablet> {
        std::pair<TTabletId, TFollowerId> TabletId;

        TEvKickTablet(const TTabletInfo& tablet)
            : TabletId(tablet.GetFullTabletId())
        {}
    };

    struct TEvProcessBootQueue : TEventLocal<TEvProcessBootQueue, EvProcessBootQueue> {};

    struct TEvPostponeProcessBootQueue : TEventLocal<TEvPostponeProcessBootQueue, EvPostponeProcessBootQueue> {};

    struct TEvProcessDisconnectNode : TEventLocal<TEvProcessDisconnectNode, EvProcessDisconnectNode> {
        ui32 NodeId;
        TActorId Local;
        TInstant StartTime;
        TMap<ui64,TVector<std::pair<TTabletId, TFollowerId>>> Tablets;
    };

    struct TEvBootTablets : TEventLocal<TEvBootTablets, EvBootTablets> {};

    struct TEvCheckTabletNodeAlive : TEventLocal<TEvCheckTabletNodeAlive, EvCheckTabletNodeAlive> {
        ui64 TabletId;
    };

    struct TEvProcessTabletBalancer : TEventLocal<TEvProcessTabletBalancer, EvProcessTabletBalancer> {};

    struct TEvUnlockTabletReconnectTimeout : TEventLocal<TEvUnlockTabletReconnectTimeout, EvUnlockTabletReconnectTimeout> {
        ui64 TabletId;
        ui64 SeqNo;

        TEvUnlockTabletReconnectTimeout() = default;

        explicit TEvUnlockTabletReconnectTimeout(ui64 tabletId, ui64 seqNo)
            : TabletId(tabletId)
            , SeqNo(seqNo)
        {}
    };

    struct TEvRestartComplete : TEventLocal<TEvRestartComplete, EvRestartComplete> {
        TFullTabletId TabletId;
        TStringBuf Status;

        TEvRestartComplete(TFullTabletId tabletId, TStringBuf status)
            : TabletId(tabletId)
            , Status(status)
        {}
    };

    struct TEvProcessPendingOperations : TEventLocal<TEvProcessPendingOperations, EvProcessPendingOperations> {};

    struct TEvBalancerOut : TEventLocal<TEvBalancerOut, EvBalancerOut> {};

    struct TEvProcessIncomingEvent : TEventLocal<TEvProcessIncomingEvent, EvProcessIncomingEvent> {};
};

} // NHive
} // NKikimr
