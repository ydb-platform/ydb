#pragma once

#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/tx/columnshard/flow_control_manager/flow_control_manager_counters.h>
#include <ydb/core/tx/columnshard/flow_control_manager/flow_control_manager_events.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/log.h>

#include <util/datetime/base.h>
#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>

namespace NKikimr::NColumnShard::NFlowControl {

class TFlowControlManager: public NActors::TActor<TFlowControlManager> {
    static constexpr TDuration LocationRecheckPeriod = TDuration::Seconds(5);

    struct TWaiter {
        ui64 WaiterId = 0;
        TActorId Helper;
        TVector<ui64> TabletIds;
        TVector<ui32> DestinationNodes;   // distinct known nodes at enqueue (for WaiterCountByNode)
        TInstant WaitDeadline;
        TInstant EnqueuedAt;
        bool DrainScheduled = false;
        bool TokenReserved = false;
    };

    // Delayed-reject entry: holds only minimal data needed to send OVERLOADED after a delay.
    // Arrow batch is dropped immediately to save memory.
    struct TDelayedReject {
        ui64 RejectId = 0;
        TActorId ReplyTo;
        std::shared_ptr<NYql::TIssues> Issues;
        TInstant RejectAt;
    };

    TCSFlowControlManagerCounters Counters;

    // nodeId -> last overload generation (present => hot)
    THashMap<ui32, ui64> HotNodes;
    // tabletId -> nodeId
    THashMap<ui64, ui32> TabletToNode;
    THashMap<ui64, TInstant> LastLocationRecheck;
    THashSet<ui64> LocationRecheckInFlight;

    THashMap<ui64, TWaiter> Waiters;
    TDeque<ui64> WaitQueueOrder;
    ui64 NextWaiterId = 1;

    // Per-destination waiter counts (no-jump admit). Key = nodeId.
    THashMap<ui32, ui64> WaiterCountByNode;

    // Delayed-reject queue: minimal metadata only, no Arrow batch.
    // Capacity is read live from TFlowControlManagerServiceOperator::GetMaxDelayedRejectQueueSize().
    THashMap<ui64, TDelayedReject> DelayedRejects;
    TDeque<ui64> DelayedRejectOrder;
    ui64 NextRejectId = 1;

    // Drain token bucket + AIMD (FCM-local).
    //
    // Rate control is fully closed-loop on observed per-request write outcomes
    // (TEvWriteOutcome from TShardWriter) and contains no wall-clock timers:
    //  * growth: release a "cohort" of ceil(RefillRateR) waiters, then grow by AimdAdd
    //    only once that many outcomes came back with zero overloads;
    //  * cut: proportional to the observed overload fraction of a cohort.
    // LastRefillAt is the only remaining TInstant and is pure token-bucket bookkeeping
    // (tokens accrue as RefillRateR * dt), not a control-loop timer.
    double Tokens = 0.0;
    double RefillRateR = 10.0;
    double Burst = 20.0;
    double RMin = 10.0;
    double RMax = 500.0;
    double AimdAdd = 5.0;
    double AimdBeta = 0.5;
    TInstant LastRefillAt;
    bool DrainWakeupScheduled = false;

    // Outcome-counted cohort. Opened when the first waiter of a new round is released,
    // closed when Target outcomes have arrived. Growth needs no clock: it is decided
    // purely by counting outcomes, which are positive events (each released write
    // reports back), unlike the absence of a node-level overload signal.
    bool CohortOpen = false;
    ui64 CohortTarget = 0;
    ui64 CohortReleased = 0;
    ui64 CohortOkCount = 0;
    ui64 CohortOverloadCount = 0;

    // clang-format off
    STRICT_STFUNC(StateMain,
                  HFunc(NFlowControl::TEvLongTxWrite, Handle)
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

    void Handle(const NFlowControl::TEvLongTxWrite::TPtr& ev, const TActorContext& ctx);
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

    bool IsAdmitAllowed(const TVector<ui64>& tabletIds) const;
    bool HasWaitersOnDestinations(const TVector<ui64>& tabletIds) const;
    TVector<ui32> CollectDestinationNodes(const TVector<ui64>& tabletIds) const;
    void IncWaiterCounts(const TVector<ui32>& nodes);
    void DecWaiterCounts(const TVector<ui32>& nodes);
    void MaybeStartLocationRechecks(const TVector<ui64>& tabletIds);
    void PublishMapSizes() const;
    void PublishDrainGauges() const;
    void RefillTokens(TInstant now);
    // Re-read the static AIMD bounds (RMin/RMax/AimdAdd/AimdBeta) from live config and
    // clamp RefillRateR into them. Called each drain cycle so that FlowControl config
    // applied AFTER the actor was constructed takes effect — mirroring how the wait-queue
    // knobs are already read live. Without this the bounds were frozen at construction
    // (to process defaults if config was not yet merged), e.g. RMax stuck at 500.
    void SyncDrainBounds();
    void RecomputeBurst();
    // Open a cohort (if none) and account one released waiter.
    void NoteCohortRelease();
    // Account one arrived outcome and close/apply the cohort when it is complete.
    void NoteCohortOutcome(bool overloaded);
    // Applies additive increase (clean cohort) or a cut proportional to the observed
    // overload fraction, then resets cohort state.
    void CloseCohort();
    void CutRateByOverloadFraction(double overloadFraction);
    void ScheduleDrainEligible(const TActorContext& ctx);
    void EraseWaiter(ui64 waiterId);
    void RefundDrainToken(TWaiter& waiter);

public:
    TFlowControlManager(TIntrusivePtr<::NMonitoring::TDynamicCounters> countersGroup);
};

}   // namespace NKikimr::NColumnShard::NFlowControl
