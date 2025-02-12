#pragma once

#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include "hive.h"
#include "tablet_info.h"
#include "node_info.h"
#include "leader_tablet_info.h"

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
        EvRefreshStorageInfo,
        EvLogTabletMoves,
        EvStartStorageBalancer,
        EvRestartCancelled,
        EvProcessStorageBalancer,
        EvStorageBalancerOut,
        EvDeleteNode,
        EvCanMoveTablets,
        EvUpdateDataCenterFollowers,
        EvGenerateTestData,
        EvRefreshScaleRecommendation,
        EvUpdateFollowers,
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
        NKikimrHive::ELockLostReason Reason;

        TEvUnlockTabletReconnectTimeout() = default;

        explicit TEvUnlockTabletReconnectTimeout(const TLeaderTabletInfo& tablet, NKikimrHive::ELockLostReason reason)
            : TabletId(tablet.Id)
            , SeqNo(tablet.PendingUnlockSeqNo)
            , Reason(reason)
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

    struct TEvRefreshStorageInfo : TEventLocal<TEvRefreshStorageInfo, EvRefreshStorageInfo> {};

    struct TEvLogTabletMoves : TEventLocal<TEvLogTabletMoves, EvLogTabletMoves> {};

    struct TEvStartStorageBalancer : TEventLocal<TEvStartStorageBalancer, EvStartStorageBalancer> {
        TStorageBalancerSettings Settings;

        TEvStartStorageBalancer(TStorageBalancerSettings settings) : Settings(settings) {}
    };

    struct TEvRestartCancelled : TEventLocal<TEvRestartCancelled, EvRestartCancelled> {
        TFullTabletId TabletId;

        TEvRestartCancelled(TFullTabletId tabletId) : TabletId(tabletId) {}
    };

    struct TEvProcessStorageBalancer : TEventLocal<TEvProcessStorageBalancer, EvProcessStorageBalancer> {};

    struct TEvStorageBalancerOut : TEventLocal<TEvStorageBalancerOut, EvStorageBalancerOut> {};

    struct TEvDeleteNode : TEventLocal<TEvDeleteNode, EvDeleteNode> {
        TNodeId NodeId;

        TEvDeleteNode(TNodeId nodeId) : NodeId(nodeId) {}
    };

    struct TEvCanMoveTablets : TEventLocal<TEvCanMoveTablets, EvCanMoveTablets> {};

    struct TEvUpdateDataCenterFollowers : TEventLocal<TEvUpdateDataCenterFollowers, EvUpdateDataCenterFollowers> {
        TDataCenterId DataCenter;

        TEvUpdateDataCenterFollowers(TDataCenterId dataCenter) : DataCenter(dataCenter) {};
    };

    struct TEvGenerateTestData : TEventLocal<TEvGenerateTestData, EvGenerateTestData> {};
  
    struct TEvRefreshScaleRecommendation : TEventLocal<TEvRefreshScaleRecommendation, EvRefreshScaleRecommendation> {};

    struct TEvUpdateFollowers : TEventLocal<TEvUpdateFollowers, EvUpdateFollowers> {
    };
};

} // NHive
} // NKikimr
