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

#include <limits>

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
        ui64 BatchSize = 0;   // deserialized batch bytes; charged against the bytes-rate bucket
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
    //    **percent** of the current rate (traffic-related), only once that many outcomes
    //    came back with zero overloads;
    //  * cut: proportional to the observed overload fraction of a cohort.
    double Tokens = 0.0;
    double RefillRateR = 10.0;
    double RMin = 0.0;   // 0 => unset => EffectiveRMin() clamps to a tiny floor (no config nail)
    double RMax = 0.0;   // 0 => unset => EffectiveRMax() is +inf (AIMD self-regulates upward)
    double AimdAdd = 5.0;   // percent of RefillRateR per clean cohort
    double AimdBeta = 0.5;
    TInstant LastRefillAt;
    bool DrainWakeupScheduled = false;

    // Bytes-rate token bucket (mirrors the count bucket): limits bytes/sec out of the
    // wait queue. A waiter is released only when BOTH buckets have enough tokens, so small
    // batches are gated by the count bucket and large batches by the bytes bucket.
    double TokensBytes = 0.0;
    double RefillRateBytesR = 10'000'000.0;   // bytes/sec
    double RMinBytes = 0.0;   // 0 => unset
    double RMaxBytes = 0.0;   // 0 => unset
    double AimdAddBytes = 5.0;   // percent of RefillRateBytesR per clean cohort
    double AimdBetaBytes = 0.5;
    TInstant LastRefillBytesAt;

    // Observe-then-limit: while the wait queue is empty every admit takes the fast path,
    // so the observed throughput is the rate the system currently sustains without pushing
    // back. We EWMA it and, the moment the queue first fills, seed the drain rates from it
    // (× a safety factor) instead of a config "nail". ObservedOverload records whether any
    // overload was seen during the current empty-queue window (then we seed more cautiously).
    double ObservedRateCount = 0.0;   // EWMA requests/sec
    double ObservedRateBytes = 0.0;   // EWMA bytes/sec
    TInstant LastObserveAt;
    bool ObservedOverload = false;
    bool WasQueueEmpty = true;
    static constexpr double ObserveTauSec = 5.0;
    static constexpr double ObserveSafetyFactor = 0.8;
    static constexpr double ObserveOverloadFactor = 0.5;

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

    // Unset (0) bounds mean "no limit": tiny floor to avoid a zero-rate stall, +inf ceiling.
    double EffectiveRMin() const {
        return RMin > 0.0 ? RMin : 0.001;
    }

    double EffectiveRMax() const {
        return RMax > 0.0 ? RMax : std::numeric_limits<double>::infinity();
    }

    double EffectiveRMinBytes() const {
        return RMinBytes > 0.0 ? RMinBytes : 1.0;
    }

    double EffectiveRMaxBytes() const {
        return RMaxBytes > 0.0 ? RMaxBytes : std::numeric_limits<double>::infinity();
    }

    // Soft cap for the bytes bucket: one second of traffic, but never below the FIFO head's
    // BatchSize — otherwise a single request larger than RefillRateBytesR permanently stalls
    // the wait queue (tokens can never accumulate past ceil(rate)).
    double BytesSoftCap() const;
    // Observe-then-limit helpers.
    void UpdateObservedThroughput(TInstant now, ui64 batchSize);
    void InitializeRatesFromObservation(ui64 firstBatchSize = 0);
    void MaybeMarkQueueEmpty();
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
