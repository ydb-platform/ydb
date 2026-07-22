#pragma once

#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/tx/columnshard/flow_control_manager/flow_control_manager_counters.h>
#include <ydb/core/tx/columnshard/flow_control_manager/flow_control_manager_events.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/log.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

namespace NKikimr::NColumnShard::NFlowControl {

enum class EAdmitDecision {
    Allow,
    RejectNow,
    // Enqueue // phase 2 wait-queue
};

class TFlowControlManager: public NActors::TActor<TFlowControlManager> {
    static constexpr TDuration LocationRecheckPeriod = TDuration::Seconds(5);

    TCSFlowControlManagerCounters Counters;

    // nodeId -> last overload generation (present => hot)
    THashMap<ui32, ui64> HotNodes;
    // tabletId -> nodeId
    THashMap<ui64, ui32> TabletToNode;
    THashMap<ui64, TInstant> LastLocationRecheck;
    THashSet<ui64> LocationRecheckInFlight;

    // clang-format off
    STRICT_STFUNC(StateMain,
                  HFunc(NFlowControl::TEvLongTxWrite, Handle)
                  HFunc(NFlowControl::TEvNodeOverloadStatus, Handle)
                  HFunc(NFlowControl::TEvTabletLocationUpdated, Handle)
                  HFunc(NFlowControl::TEvTabletLocationInvalidated, Handle)
                  HFunc(TEvTabletResolver::TEvForwardResult, Handle)
    )
    // clang-format on

    void Handle(const NFlowControl::TEvLongTxWrite::TPtr& ev, const TActorContext& ctx);
    void Handle(const NFlowControl::TEvNodeOverloadStatus::TPtr& ev, const TActorContext& ctx);
    void Handle(const NFlowControl::TEvTabletLocationUpdated::TPtr& ev, const TActorContext& ctx);
    void Handle(const NFlowControl::TEvTabletLocationInvalidated::TPtr& ev, const TActorContext& ctx);
    void Handle(const TEvTabletResolver::TEvForwardResult::TPtr& ev, const TActorContext& ctx);

    EAdmitDecision TryAdmit(const TVector<ui64>& tabletIds) const;
    void MaybeStartLocationRechecks(const TVector<ui64>& tabletIds);
    static bool TryCollectTargetTablets(const TLongTxWrite& tx, TVector<ui64>* tabletIds);
    void ReplyOverloaded(const TActorContext& ctx, TLongTxWrite& tx, const TString& message) const;

public:
    TFlowControlManager(TIntrusivePtr<::NMonitoring::TDynamicCounters> countersGroup);
};

}   // namespace NKikimr::NColumnShard::NFlowControl
