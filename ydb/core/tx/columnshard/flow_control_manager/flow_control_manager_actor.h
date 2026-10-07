#pragma once

#include "delayed_reject_queue.h"
#include "drain_rate_controller.h"
#include "flow_control_manager_counters.h"
#include "flow_control_manager_events.h"
#include "node_state_map.h"
#include "wait_queue.h"

#include <ydb/core/base/tablet_resolver.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/log.h>

#include <util/datetime/base.h>
#include <util/generic/vector.h>

namespace NKikimr::NColumnShard::NFlowControl {

// Node-local admission control for LongTx writes into column shards.
//
// The actor itself only routes events and owns the scheduler; everything it decides is delegated:
//  * TNodeStateMap        — where tablets live and which nodes are overloaded;
//  * TWaitQueue           — the FIFO of waiting requests and the no-jump per-node counts;
//  * TDelayedRejectQueue  — requests that will be failed with OVERLOADED near their deadline;
//  * TDrainRateController — the closed-loop rate math (token buckets, CUBIC, anchor, cohorts).
//
// The split exists because the rate control loop is the part that needs real testing, and it is
// far cheaper to test it against explicit timestamps than through a simulated actor system.
class TFlowControlManager: public NActors::TActor<TFlowControlManager> {
    // A tablet gated by a hot node may simply have moved; re-resolve its location, but not more
    // often than this per tablet.
    static constexpr TDuration NodeRecheckPeriod = TDuration::Seconds(5);
    // Tick period used to integrate the hot decay: while a node is hot nothing is drainable, so
    // the normal pacing wakeup never fires.
    static constexpr ui64 HotDecayTickMs = 200;

    TCSFlowControlManagerCounters Counters;
    TNodeStateMap NodeState;
    TWaitQueue WaitQueue;
    // Capacity is read live from TFlowControlManagerServiceOperator::GetMaxDelayedRejectQueueSize().
    TDelayedRejectQueue DelayedRejects;
    // Declared after Counters: it holds a reference to them.
    TDrainRateController Drain;
    bool DrainWakeupScheduled = false;

    // clang-format off
    STRICT_STFUNC(StateMain,
                  HFunc(NFlowControl::TEvTryAdmit, Handle)
                  HFunc(NFlowControl::TEvCancelWait, Handle)
                  HFunc(NFlowControl::TEvDrainWaiter, Handle)
                  HFunc(NFlowControl::TEvContinueDrain, Handle)
                  HFunc(NFlowControl::TEvNodeOverloadStatus, Handle)
                  HFunc(NFlowControl::TEvTabletLocationUpdated, Handle)
                  HFunc(NFlowControl::TEvTabletLocationInvalidated, Handle)
                  HFunc(TEvTabletResolver::TEvForwardResult, Handle)
                  HFunc(NFlowControl::TEvFireDelayedReject, Handle)
                  HFunc(NFlowControl::TEvWriteOutcome, Handle)
    )
    // clang-format on

    void Handle(const NFlowControl::TEvTryAdmit::TPtr& ev, const TActorContext& ctx);
    void Handle(const NFlowControl::TEvCancelWait::TPtr& ev, const TActorContext& ctx);
    void Handle(const NFlowControl::TEvDrainWaiter::TPtr& ev, const TActorContext& ctx);
    void Handle(const NFlowControl::TEvContinueDrain::TPtr& ev, const TActorContext& ctx);
    void Handle(const NFlowControl::TEvNodeOverloadStatus::TPtr& ev, const TActorContext& ctx);
    void Handle(const NFlowControl::TEvTabletLocationUpdated::TPtr& ev, const TActorContext& ctx);
    void Handle(const NFlowControl::TEvTabletLocationInvalidated::TPtr& ev, const TActorContext& ctx);
    void Handle(const TEvTabletResolver::TEvForwardResult::TPtr& ev, const TActorContext& ctx);
    void Handle(const NFlowControl::TEvFireDelayedReject::TPtr& ev, const TActorContext& ctx);
    void Handle(const NFlowControl::TEvWriteOutcome::TPtr& ev, const TActorContext& ctx);

    // Snapshot of everything the rate controller needs to know about the actor's state.
    TDrainState MakeDrainState(TInstant now) const;
    // BatchSize of the FIFO head that can actually drain (0 if none): the liveness floor for every
    // bytes-bucket cap.
    double FrontWaiterBatchSize(TInstant now) const;

    void PublishMapSizes() const;
    void MaybeStartNodeRechecks(const TVector<ui64>& tabletIds);
    // Reserve tokens for and schedule every waiter that may go now, then arm the next wakeup.
    void ScheduleDrainEligible(const TActorContext& ctx);
    // The single choke point for waiter removal (cancel, drain, client deadline).
    void EraseWaiter(ui64 waiterId);
    void RefundDrainToken(TWaiter& waiter);

public:
    TFlowControlManager(TIntrusivePtr<::NMonitoring::TDynamicCounters> countersGroup);
};

}   // namespace NKikimr::NColumnShard::NFlowControl
