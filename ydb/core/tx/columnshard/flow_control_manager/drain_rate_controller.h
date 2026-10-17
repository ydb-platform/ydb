#pragma once

#include "flow_control_manager_counters.h"
#include "flow_control_manager_service.h"
#include "rate_bucket.h"

#include <util/datetime/base.h>

namespace NKikimr::NColumnShard::NFlowControl {

// Everything the rate math needs to know about the actor's world, sampled at the call site.
// Passing it explicitly (rather than letting the controller reach into the actor) is what makes
// the whole rate-control loop testable without an actor system or a scheduler.
struct TDrainState {
    TInstant Now;
    // Any node currently reporting overload: admits are gated, so measured throughput says
    // nothing about capacity and the rate must keep decaying.
    bool AnyHotNode = false;
    // No demand: nothing to meter, so busy time does not accrue and the observation window for
    // the next empty -> non-empty seed is open.
    bool QueueEmpty = true;
    // BatchSize of the FIFO head that could drain now (0 if none). Liveness floor for every
    // bytes-bucket cap: a request larger than one second of budget must still be able to drain.
    double FrontWaiterBatchSize = 0.0;
};

// The closed-loop drain rate controller: two token buckets (admits/sec and bytes/sec) plus the
// signals that move them. A waiter is released only when both buckets can pay, so small batches
// are gated on count and large batches on bytes.
//
// Inputs, all of them measured rather than configured:
//  * per-request write outcomes (TEvWriteOutcome from TShardWriter), grouped into cohorts;
//  * node overload from OverloadManager entering/leaving the hot set — compaction overload is
//    reported even when writes succeed, so outcomes alone would never cut;
//  * the throughput FCM actually admits, which caps how far the rate may run above reality.
//
// Responses:
//  * cut: proportional to the cohort's overload fraction, or a full AimdBeta on the empty ->
//    non-empty hot edge (with Wmax pinned to the post-cut rate, so CUBIC does not climb back);
//  * decay: while the hot set stays non-empty the rate keeps decaying (AimdBeta per
//    HotDecayTauSec), so sustained compaction pressure keeps pushing down instead of parking at
//    the single edge cut;
//  * growth: CUBIC W(t) then a probe above Wmax on a clean cohort, at most one step per
//    GrowthPeriodSec and only after HotCooldownSec of quiet. Outcomes arrive per *shard* write,
//    so cohort completion alone would let growth fire at the shard fan-out rate.
class TDrainRateController {
public:
    using TDrainRateParams = TFlowControlManagerServiceOperator::TDrainRateParams;

    explicit TDrainRateController(const TCSFlowControlManagerCounters& counters)
        : Counters(counters)
    {
    }

    // Seed both buckets from config. The bounds are only a seed: SyncBounds() re-reads them every
    // drain cycle, so FlowControl config merged after construction still takes effect.
    void Seed(const TDrainRateParams& params);
    void SyncBounds(const TDrainRateParams& params);
    void PublishCounters() const;

    double GetRateCount() const {
        return Count.GetRate();
    }

    double GetRateBytes() const {
        return Bytes.GetRate();
    }

    double GetTokensBytes() const {
        return Bytes.GetTokens();
    }

    // Both rates sit on their floors: there is nothing left to decay, so the hot-decay tick can
    // stop instead of running forever.
    bool IsAtRateFloor() const;

    // Advance every clock the rate math owns (busy time, served window, hot decay, anchor, token
    // refill) up to state.Now. Must run before waiter eligibility is evaluated.
    void PrepareDrainCycle(const TDrainState& state, const TDrainRateParams& params);
    // Charge one admit and its bytes against both buckets, all-or-nothing.
    bool TryReserve(ui64 batchSize);
    void Refund(ui64 batchSize);

    // Fast-path admit (empty queue): folds the arrival spacing into the observation EWMA that
    // seeds the rates when the queue first fills.
    void NoteFastPathAdmit(const TDrainState& state, ui64 batchSize);
    // Any admit, fast path or drained: feeds the served-throughput anchor.
    void NoteAdmitted(const TDrainState& state, ui64 batchSize);
    // A waiter left the queue through the drain path: accounts it against the open cohort.
    void NoteWaiterReleased();

    // True while the queue has stayed empty, i.e. the observation window is still collecting.
    bool IsObservationWindowOpen() const {
        return WasQueueEmpty;
    }

    // Overload seen while the queue was empty: the observed throughput was already too high, so
    // the next seed must be more cautious.
    void NoteOverloadObserved() {
        ObservedOverload = true;
    }

    // Empty -> non-empty edge: the incoming rate has outrun what the fast path sustained, so seed
    // the drain rates from the observation instead of a config nail.
    void NoteQueueBecameNonEmpty(const TDrainState& state, ui64 firstBatchSize);
    // The last waiter left: reopen the observation window. Drain rates are NOT reset — AIMD keeps
    // its learned value, observation only re-seeds the *starting* rate at the next edge.
    void NoteQueueEmpty();

    void NoteWriteOutcome(const TDrainState& state, const TDrainRateParams& params, EWriteOutcome outcome);
    // Empty -> non-empty hot edge: drop the in-flight cohort and apply a full cut.
    void NoteFirstHotNode(const TDrainState& state, const TDrainRateParams& params);
    void NoteHotNode(TInstant now);
    // Hot -> cool edge: waiters piled up while admits were gated and tokens kept accruing.
    // Releasing all of it at once is what drives the next compaction overload.
    void NoteAllNodesReady(const TDrainState& state);

private:
    double GrowthPeriodSec() const;
    double HotCooldownSec() const;
    double HotDecayTauSec() const;

    // +inf until the first window closes, so a cold controller is not clamped to its floor.
    double AnchorMaxCount() const;
    double AnchorMaxBytes() const;

    void AccrueBusyTime(const TDrainState& state);
    void CloseServedWindow(TInstant now);
    void ApplyHotDecay(const TDrainState& state);
    void MaybeApplyAnchor(const TDrainState& state);
    void RefillTokens(const TDrainState& state);

    // No hot node and no overloaded outcome for HotCooldownSec: the only state in which anything
    // is allowed to raise the drain rates.
    bool IsQuietSinceHot(const TDrainState& state) const;
    bool CanGrowNow(const TDrainState& state) const;

    void NoteCohortOutcome(const TDrainState& state, EWriteOutcome outcome);
    void ResetCohort();
    void CloseCohort(const TDrainState& state);
    void CutRateByOverloadFraction(const TDrainState& state, double overloadFraction);
    void ClampTokensToSoftCap(const TDrainState& state);
    void EnsureCubicProbeEpoch(TInstant now);

private:
    const TCSFlowControlManagerCounters& Counters;

    // Admits/sec. Floor of 1 when DrainRateMin is 0 (UT-only).
    TRateBucket Count{ 1.0 };
    // Bytes/sec. Floor of 1 MB/s when DrainRateMinBytes is 0 (UT-only).
    TRateBucket Bytes{ 1'000'000.0 };

    // Shared CUBIC knobs and the epoch origin both buckets recover along.
    double AimdBeta = 0.0;
    double AimdBetaBytes = 0.0;
    double CubicRecoveryTargetSec = 0.0;
    double CubicProbePercent = 0.0;
    TInstant CubicEpochStart = TInstant::Zero();
    static constexpr double MeaningfulCutRatio = 1.05;   // prev/new >= this => reset the epoch

    // Observe-then-limit. While the wait queue is empty every admit takes the fast path, so the
    // observed throughput is what the system sustains without FCM pushing back. The estimate is
    // spacing-based (1/dt), so bursty arrivals read high: it is capped by the anchor and may only
    // raise the rates in a quiet window, otherwise it would undo a decay hot pressure just applied.
    double ObservedRateCount = 0.0;   // EWMA requests/sec
    double ObservedRateBytes = 0.0;   // EWMA bytes/sec
    TInstant LastObserveAt;
    bool ObservedOverload = false;
    bool WasQueueEmpty = true;
    static constexpr double ObserveTauSec = 5.0;
    static constexpr double ObserveSafetyFactor = 0.8;
    static constexpr double ObserveOverloadFactor = 0.5;

    // Outcome-counted cohort. Opened when the first waiter of a new round is released, closed once
    // Target outcomes have arrived. A clean cohort makes growth *permissible* but does not time it
    // — the events are per shard write, so the growth clock lives in CanGrowNow().
    bool CohortOpen = false;
    ui64 CohortTarget = 0;
    ui64 CohortOkCount = 0;
    ui64 CohortOverloadCount = 0;

    // Served-throughput anchor. Counts everything FCM admits over closed windows and EWMAs the
    // per-window rate — never 1/dt spacing, which would turn a burst of same-millisecond admits
    // into a huge instantaneous rate.
    //
    // Only "busy" time counts: queue non-empty (there is demand being metered) and no hot node
    // (admits are permitted). Throughput measured while nobody is asking, or while everything is
    // gated, says nothing about the sustainable rate — using it as a ceiling would throttle the
    // next burst down to the previous idle level.
    double ServedRateCount = 0.0;
    double ServedRateBytes = 0.0;
    double ServedAccumCount = 0.0;
    double ServedAccumBytes = 0.0;
    double ServedBusySec = 0.0;
    TInstant ServedWindowStart = TInstant::Zero();
    TInstant LastBusySampleAt = TInstant::Zero();
    static constexpr double ServedWindowSec = 1.0;
    static constexpr double ServedTauSec = 5.0;
    // A window has to be mostly busy before its rate is a usable capacity sample.
    static constexpr double ServedBusyMinFraction = 0.5;
    // Headroom over measured throughput. >1 so the bucket can still absorb bursts and discover
    // more capacity, but small enough that the rate cannot detach from reality.
    static constexpr double AnchorFactor = 2.0;
    // Per growth step, how much of the excess above the anchor is given back.
    static constexpr double AnchorGiveBackFactor = 0.9;

    // Hot-state feedback for decay / cooldown. Zero = "never happened".
    TInstant LastHotAt = TInstant::Zero();
    TInstant LastHotDecayAt = TInstant::Zero();
    TInstant LastGrowthAt = TInstant::Zero();
    TInstant LastAnchorAt = TInstant::Zero();
    TInstant LastOverloadOutcomeAt = TInstant::Zero();

public:
    // Fraction of one second of budget tokens may keep across a hot -> cool edge.
    static constexpr double ReadyDumpFraction = 0.25;
};

}   // namespace NKikimr::NColumnShard::NFlowControl
