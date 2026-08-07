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

    // Drain token buckets + CUBIC recovery (FCM-local).
    //
    // Rate control is closed-loop on:
    //  * per-request write outcomes (TEvWriteOutcome from TShardWriter);
    //  * node overload from OverloadManager (compaction / in-flight) entering HotNodes —
    //    otherwise compaction-only overload only gates admits and CUBIC keeps probing.
    //  * cut: proportional to cohort overload fraction, or a full AimdBeta when HotNodes
    //    goes from empty → non-empty (Wmax pinned to post-cut — no CUBIC climb back);
    //  * decay: while HotNodes stays non-empty the rate keeps decaying (AimdBeta per
    //    HotDecayTauSec), so sustained compaction pressure keeps pushing the rate down
    //    instead of parking it at the single edge cut;
    //  * growth: CUBIC W(t) / probe on a clean cohort, but at most one step per
    //    GrowthPeriodSec and only after HotCooldownSec without hot nodes or overloaded
    //    outcomes. Outcomes arrive per *shard* write, so cohort completion alone would
    //    let growth fire at the shard fan-out rate;
    //  * ceiling: AnchorFactor × the throughput FCM actually admits — the rate can never
    //    run far above reality (no absolute config nail; the anchor is measured).
    double Tokens = 0.0;
    double RefillRateR = 10.0;
    double RMin = 0.0;   // 0 => unset => EffectiveRMin() clamps to a tiny floor (no config nail)
    double RMax = 0.0;   // 0 => unset => EffectiveRMax() is +inf
    double AimdBeta = 0.5;
    double CubicRecoveryTargetSec = 10.0;
    double CubicProbePercent = 5.0;
    TInstant LastRefillAt;
    bool DrainWakeupScheduled = false;

    // Bytes-rate token bucket (mirrors the count bucket): limits bytes/sec out of the
    // wait queue. A waiter is released only when BOTH buckets have enough tokens, so small
    // batches are gated by the count bucket and large batches by the bytes bucket.
    double TokensBytes = 0.0;
    double RefillRateBytesR = 10'000'000.0;   // bytes/sec
    double RMinBytes = 0.0;   // 0 => unset
    double RMaxBytes = 0.0;   // 0 => unset
    double AimdBetaBytes = 0.5;
    TInstant LastRefillBytesAt;

    // CUBIC epoch (shared wall-clock origin for both buckets after a meaningful cut).
    double WmaxCount = 0.0;
    double WmaxBytes = 0.0;
    double CubicCCount = 0.0;
    double CubicCBytes = 0.0;
    TInstant CubicEpochStart = TInstant::Zero();
    static constexpr double MeaningfulCutRatio = 1.05;   // prev/new >= this ⇒ reset epoch

    // Observe-then-limit: while the wait queue is empty every admit takes the fast path,
    // so the observed throughput is the rate the system currently sustains without pushing
    // back. We EWMA it and, the moment the queue first fills, seed the drain rates from it
    // (× a safety factor) instead of a config "nail". ObservedOverload records whether any
    // overload was seen during the current empty-queue window (then we seed more cautiously).
    // The seed is spacing-based, so it is capped by the anchor and may only raise the rates
    // in a quiet window — otherwise it would undo a decay the hot pressure just applied.
    double ObservedRateCount = 0.0;   // EWMA requests/sec
    double ObservedRateBytes = 0.0;   // EWMA bytes/sec
    TInstant LastObserveAt;
    bool ObservedOverload = false;
    bool WasQueueEmpty = true;
    static constexpr double ObserveTauSec = 5.0;
    static constexpr double ObserveSafetyFactor = 0.8;
    static constexpr double ObserveOverloadFactor = 0.5;

    // Outcome-counted cohort. Opened when the first waiter of a new round is released,
    // closed when Target outcomes have arrived. Outcomes are positive events (each
    // released write reports back), unlike the absence of a node-level overload signal,
    // so a clean cohort is what makes growth *permissible* — but not what times it: the
    // events are per shard write, so the growth clock lives in CanGrowNow().
    bool CohortOpen = false;
    ui64 CohortTarget = 0;
    ui64 CohortReleased = 0;
    ui64 CohortOkCount = 0;
    ui64 CohortOverloadCount = 0;

    // Served-throughput anchor. Counts everything FCM admits (fast path + drains) over
    // closed windows and EWMAs the per-window rate — never 1/dt spacing, which turns a
    // burst of same-millisecond admits into a huge instantaneous rate.
    //
    // Only "busy" time counts: the wait queue non-empty (there is demand FCM is metering)
    // and no hot node (admits are permitted). Throughput measured while nobody is asking,
    // or while everything is gated, says nothing about the rate we can sustain — using it
    // as a ceiling would throttle the next burst down to the previous idle level.
    double ServedRateCount = 0.0;   // EWMA requests/sec served under demand
    double ServedRateBytes = 0.0;   // EWMA bytes/sec served under demand
    double ServedAccumCount = 0.0;
    double ServedAccumBytes = 0.0;
    double ServedBusySec = 0.0;
    TInstant ServedWindowStart = TInstant::Zero();
    TInstant LastBusySampleAt = TInstant::Zero();
    static constexpr double ServedWindowSec = 1.0;
    static constexpr double ServedTauSec = 5.0;
    // A window has to be mostly busy before its rate is a usable capacity sample.
    static constexpr double ServedBusyMinFraction = 0.5;
    // Headroom over measured throughput. >1 so the bucket can still absorb bursts and
    // discover more capacity, but small enough that the rate cannot detach from reality.
    static constexpr double AnchorFactor = 2.0;
    // Per growth step, how much of the excess above the anchor is given back.
    static constexpr double AnchorGiveBackFactor = 0.9;

    // Hot-state feedback for decay / cooldown. Zero = "never happened".
    TInstant LastHotAt = TInstant::Zero();
    TInstant LastHotDecayAt = TInstant::Zero();
    TInstant LastGrowthAt = TInstant::Zero();
    TInstant LastAnchorAt = TInstant::Zero();
    TInstant LastOverloadOutcomeAt = TInstant::Zero();
    // Tick period used to integrate decay while hot (no waiter is drainable then, so the
    // normal pacing wakeup does not fire).
    static constexpr ui64 HotDecayTickMs = 200;
    // Fraction of one second of budget tokens may keep across a hot → cool edge.
    static constexpr double ReadyDumpFraction = 0.25;

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
    // Re-read the static CUBIC/bounds knobs (RMin/RMax/AimdBeta/KTarget/Probe) from live
    // config and clamp RefillRateR into them. Called each drain cycle so that FlowControl
    // config applied AFTER the actor was constructed takes effect — mirroring how the
    // wait-queue knobs are already read live.
    void SyncDrainBounds();

    // Unset (0) bounds mean "no limit": a usable floor so cuts cannot freeze the wait queue
    // (0.001 req/s or 1 B/s needs minutes-to-days to release one batch), +inf ceiling.
    double EffectiveRMin() const {
        return RMin > 0.0 ? RMin : 1.0;
    }

    double EffectiveRMax() const {
        return RMax > 0.0 ? RMax : std::numeric_limits<double>::infinity();
    }

    double EffectiveRMinBytes() const {
        return RMinBytes > 0.0 ? RMinBytes : 1'000'000.0;
    }

    double EffectiveRMaxBytes() const {
        return RMaxBytes > 0.0 ? RMaxBytes : std::numeric_limits<double>::infinity();
    }

    // Soft cap for the bytes bucket: one second of traffic, but never below the FIFO head's
    // BatchSize — otherwise a single request larger than RefillRateBytesR permanently stalls
    // the wait queue (tokens can never accumulate past ceil(rate)).
    double BytesSoftCap() const;
    // BatchSize of the FIFO head that can actually drain (0 if none): the liveness floor
    // for every bytes-bucket cap.
    double FrontWaiterBatchSize() const;

    // Served-throughput anchor helpers.
    void NoteAdmitted(TInstant now, ui64 batchSize);
    void AccrueBusyTime(TInstant now);
    void CloseServedWindow(TInstant now);
    // +inf until the first window closes, so a cold FCM is not clamped to its floor.
    double AnchorMaxCount() const;
    double AnchorMaxBytes() const;

    // Rate ceiling from measured throughput; also pulls an already-inflated rate down.
    void MaybeApplyAnchor(TInstant now);

    // Hot-state control.
    void ApplyHotDecay(TInstant now);
    // No hot node and no overloaded outcome for HotCooldownSec: the only state in which
    // anything is allowed to raise the drain rates.
    bool IsQuietSinceHot(TInstant now) const;
    bool CanGrowNow(TInstant now) const;
    // Tokens accrued while admits were gated must not all be spent in the instant the
    // node reports READY — that is what re-overloads compaction immediately.
    void ClampTokensAfterReady();
    double GrowthPeriodSec() const;
    double HotCooldownSec() const;
    double HotDecayTauSec() const;

    // Observe-then-limit helpers.
    void UpdateObservedThroughput(TInstant now, ui64 batchSize);
    void InitializeRatesFromObservation(ui64 firstBatchSize = 0);
    void MaybeMarkQueueEmpty();
    // Open a cohort (if none) and account one released waiter.
    void NoteCohortRelease();
    // Account one arrived outcome and close/apply the cohort when it is complete.
    void NoteCohortOutcome(bool overloaded);
    // Applies CUBIC growth (clean cohort) or a cut proportional to the observed
    // overload fraction, then resets cohort state.
    void CloseCohort();
    void CutRateByOverloadFraction(double overloadFraction);
    // After a rate cut, soft-cap tokens immediately so a READY edge cannot dump a
    // pre-cut cohort's worth of reserved budget.
    void ClampTokensToSoftCap();
    // CUBIC helpers.
    static double CubicW(double c, double wmax, double tSec, double kTarget);
    void EnsureCubicProbeEpoch(TInstant now);
    void StartCubicEpoch(TInstant now, double prevCount, double newCount, double prevBytes, double newBytes);
    void ScheduleDrainEligible(const TActorContext& ctx);
    void EraseWaiter(ui64 waiterId);
    void RefundDrainToken(TWaiter& waiter);

public:
    TFlowControlManager(TIntrusivePtr<::NMonitoring::TDynamicCounters> countersGroup);
};

}   // namespace NKikimr::NColumnShard::NFlowControl
