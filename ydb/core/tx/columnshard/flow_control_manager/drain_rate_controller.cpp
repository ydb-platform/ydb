#include "drain_rate_controller.h"

#include <util/generic/utility.h>

#include <cmath>
#include <limits>

namespace NKikimr::NColumnShard::NFlowControl {

void TDrainRateController::Seed(const TDrainRateParams& params) {
    // Both buckets start in the probe phase at the seed rate: there is no cut to recover from
    // yet, and tokens start at the soft one-cohort cap so the first waiter drains immediately
    // instead of paying a full 1/rate of latency after every idle period.
    Count.Seed(params.RMin, params.RMax, params.RStart);
    Bytes.Seed(params.RMinBytes, params.RMaxBytes, params.RStartBytes);

    AimdBeta = params.AimdBeta;
    AimdBetaBytes = params.AimdBetaBytes;
    CubicRecoveryTargetSec = params.CubicRecoveryTargetSec;
    CubicProbePercent = params.CubicProbePercent;
    CubicEpochStart = TInstant::Zero();

    WasQueueEmpty = true;
    ObservedOverload = false;
    LastObserveAt = TInstant::Zero();
    ObservedRateCount = 0.0;
    ObservedRateBytes = 0.0;
}

void TDrainRateController::SyncBounds(const TDrainRateParams& params) {
    AimdBeta = params.AimdBeta;
    AimdBetaBytes = params.AimdBetaBytes;
    CubicRecoveryTargetSec = params.CubicRecoveryTargetSec;
    CubicProbePercent = params.CubicProbePercent;
    Count.SetBounds(params.RMin, params.RMax);
    Bytes.SetBounds(params.RMinBytes, params.RMaxBytes);
}

void TDrainRateController::PublishCounters() const {
    // Clamp before the ui64 cast: a negative long long from llround would wrap to a huge gauge
    // and look like infinite capacity. Rates/tokens should stay non-negative on the happy path;
    // this is belt-and-braces for floating-point underflows at the publish site only.
    const auto asUi64 = [](double value) {
        return static_cast<ui64>(std::llround(Max(0.0, value)));
    };
    Counters.SetDrainRefillRate(asUi64(Count.GetRate()));
    Counters.SetDrainTokens(asUi64(Count.GetTokens()));
    Counters.SetDrainRefillRateBytes(asUi64(Bytes.GetRate()));
    Counters.SetDrainTokensBytes(asUi64(Bytes.GetTokens()));
    Counters.SetObservedRateCount(asUi64(ObservedRateCount));
    Counters.SetObservedRateBytes(asUi64(ObservedRateBytes));
    Counters.SetServedRateCount(asUi64(ServedRateCount));
    Counters.SetServedRateBytes(asUi64(ServedRateBytes));
}

bool TDrainRateController::IsAtRateFloor() const {
    return Count.GetRate() <= Count.EffectiveMin() && Bytes.GetRate() <= Bytes.EffectiveMin();
}

double TDrainRateController::GrowthPeriodSec() const {
    // Growth is clocked by wall time, not by outcome arrivals: one TEvWriteOutcome is emitted per
    // *shard* write, so a single client request feeding N shards closes a cohort N times faster
    // than the loop assumes.
    return Max(0.1, CubicRecoveryTargetSec / 10.0);
}

double TDrainRateController::HotCooldownSec() const {
    // Quiet period the system must sustain (no hot node, no overloaded outcome) before growth is
    // allowed again — plain hysteresis around the READY flap.
    return Max(0.2, CubicRecoveryTargetSec / 5.0);
}

double TDrainRateController::HotDecayTauSec() const {
    return Max(0.1, CubicRecoveryTargetSec / 10.0);
}

double TDrainRateController::AnchorMaxCount() const {
    if (ServedRateCount <= 0.0) {
        return std::numeric_limits<double>::infinity();
    }
    return Max(Count.EffectiveMin(), AnchorFactor * ServedRateCount);
}

double TDrainRateController::AnchorMaxBytes() const {
    if (ServedRateBytes <= 0.0) {
        return std::numeric_limits<double>::infinity();
    }
    return Max(Bytes.EffectiveMin(), AnchorFactor * ServedRateBytes);
}

void TDrainRateController::PrepareDrainCycle(const TDrainState& state, const TDrainRateParams& params) {
    AccrueBusyTime(state);
    CloseServedWindow(state.Now);
    ApplyHotDecay(state);
    MaybeApplyAnchor(state);
    // Pick up FlowControl config merged after construction (e.g. dynamic config) and clamp the
    // live rate into the current bounds before refilling tokens.
    SyncBounds(params);
    RefillTokens(state);
}

void TDrainRateController::RefillTokens(const TDrainState& state) {
    // Count bucket: the soft cap is one cohort's worth of admits. It only matters when all
    // eligible waiters were blocked by a hot node and then become ready at once, and it prevents
    // releasing more than a cohort in that single instant.
    Count.RefillTokens(state.Now, 0.0);
    // Bytes bucket: one second of bytes, raised to the FIFO head's BatchSize so a single large
    // request can never permanently deadlock the queue.
    Bytes.RefillTokens(state.Now, state.FrontWaiterBatchSize);
}

bool TDrainRateController::TryReserve(ui64 batchSize) {
    // All-or-nothing: a waiter is released only when BOTH buckets can pay, which is what makes
    // small batches gate on count and large batches gate on bytes. BatchSize == 0 (unknown)
    // charges nothing to the bytes bucket.
    const double bytes = static_cast<double>(batchSize);
    if (!Count.CanPay(1.0) || !Bytes.CanPay(bytes)) {
        return false;
    }
    Count.Pay(1.0);
    Bytes.Pay(bytes);
    return true;
}

void TDrainRateController::Refund(ui64 batchSize) {
    Count.Refund(1.0);
    Bytes.Refund(static_cast<double>(batchSize));
}

void TDrainRateController::NoteFastPathAdmit(const TDrainState& state, ui64 batchSize) {
    // Called only on the fast path (empty queue): the throughput we see here is what the system
    // currently sustains without FCM pushing back. EWMA it so a later empty -> non-empty
    // transition can seed the drain rates from reality instead of a config nail.
    if (LastObserveAt == TInstant::Zero()) {
        LastObserveAt = state.Now;
        return;
    }
    const double dt = (state.Now - LastObserveAt).SecondsFloat();
    if (dt <= 0.001) {
        return;   // ignore sub-millisecond spacing (burst noise), avoids huge 1/dt spikes
    }
    // Clock-invariant EWMA: the weight of a sample depends on how long it has been since the
    // previous one, so irregular arrival spacing does not change the effective time constant.
    const double alpha = 1.0 - std::exp(-dt / ObserveTauSec);
    const double instantCountRate = 1.0 / dt;
    const double instantBytesRate = static_cast<double>(batchSize) / dt;
    ObservedRateCount = alpha * instantCountRate + (1.0 - alpha) * ObservedRateCount;
    ObservedRateBytes = alpha * instantBytesRate + (1.0 - alpha) * ObservedRateBytes;
    LastObserveAt = state.Now;
}

void TDrainRateController::NoteAdmitted(const TDrainState& state, ui64 batchSize) {
    AccrueBusyTime(state);
    if (ServedWindowStart == TInstant::Zero()) {
        ServedWindowStart = state.Now;
    }
    ServedAccumCount += 1.0;
    ServedAccumBytes += static_cast<double>(batchSize);
    CloseServedWindow(state.Now);
}

void TDrainRateController::AccrueBusyTime(const TDrainState& state) {
    const TInstant last = LastBusySampleAt;
    LastBusySampleAt = state.Now;
    if (last == TInstant::Zero() || state.Now <= last) {
        return;
    }
    // Demand present and admits permitted: whatever we deliver in this interval is the rate FCM
    // itself is imposing, which is what the anchor must track.
    if (!state.QueueEmpty && !state.AnyHotNode) {
        ServedBusySec += (state.Now - last).SecondsFloat();
    }
}

void TDrainRateController::CloseServedWindow(TInstant now) {
    if (ServedWindowStart == TInstant::Zero()) {
        ServedWindowStart = now;
        return;
    }
    const double dt = (now - ServedWindowStart).SecondsFloat();
    if (dt < ServedWindowSec) {
        return;
    }
    const double busy = Min(dt, ServedBusySec);
    const double count = ServedAccumCount;
    const double bytes = ServedAccumBytes;
    ServedAccumCount = 0.0;
    ServedAccumBytes = 0.0;
    ServedBusySec = 0.0;
    ServedWindowStart = now;
    if (busy < dt * ServedBusyMinFraction || busy <= 0.0) {
        // Mostly idle or mostly gated: no capacity information, keep the previous estimate.
        return;
    }
    // Clock-invariant EWMA again: windows close on admit arrivals, so dt varies. Weighting by
    // std::exp(-dt/tau) keeps the smoothing horizon at ServedTauSec of wall time regardless.
    const double alpha = 1.0 - std::exp(-dt / ServedTauSec);
    ServedRateCount = alpha * (count / busy) + (1.0 - alpha) * ServedRateCount;
    ServedRateBytes = alpha * (bytes / busy) + (1.0 - alpha) * ServedRateBytes;
}

void TDrainRateController::ApplyHotDecay(const TDrainState& state) {
    if (!state.AnyHotNode) {
        LastHotDecayAt = TInstant::Zero();
        return;
    }
    LastHotAt = state.Now;
    if (LastHotDecayAt == TInstant::Zero()) {
        LastHotDecayAt = state.Now;
        return;
    }
    const double dt = (state.Now - LastHotDecayAt).SecondsFloat();
    if (dt <= 0.0) {
        return;
    }
    LastHotDecayAt = state.Now;
    // The empty -> non-empty hot edge cuts once; if the node stays hot that single cut was
    // evidently not enough, so keep applying it continuously (AimdBeta per tau) until the pressure
    // clears or the configured floor is reached.
    const double beta = Min(0.999, Max(0.01, AimdBeta));
    const double factor = std::pow(beta, dt / HotDecayTauSec());
    const double prev = Count.ScaleRate(factor);
    const double prevBytes = Bytes.ScaleRate(factor);
    if (Count.GetRate() < prev || Bytes.GetRate() < prevBytes) {
        // Keep Wmax at the decayed level: the pre-cut peak is known-bad under this pressure.
        Count.PinWmaxToRate();
        Bytes.PinWmaxToRate();
        Counters.OnDrainRateDecay();
        ClampTokensToSoftCap(state);
        PublishCounters();
    }
}

void TDrainRateController::MaybeApplyAnchor(const TDrainState& state) {
    // Pull a runaway rate back toward measured throughput. This is deliberately not tied to cohort
    // completion: the cohort target is ceil(rate), so the more inflated the rate is, the longer a
    // cohort takes to close — exactly when the correction is most needed. One step per
    // GrowthPeriodSec, on its own clock.
    if (LastAnchorAt != TInstant::Zero() && (state.Now - LastAnchorAt).SecondsFloat() < GrowthPeriodSec()) {
        return;
    }
    LastAnchorAt = state.Now;
    const double capCount = AnchorMaxCount();
    const double capBytes = AnchorMaxBytes();
    bool gaveBack = false;
    if (Count.GetRate() > capCount) {
        Count.SetRate(Max(capCount, Count.GetRate() * AnchorGiveBackFactor));
        gaveBack = true;
    }
    if (Bytes.GetRate() > capBytes) {
        Bytes.SetRate(Max(capBytes, Bytes.GetRate() * AnchorGiveBackFactor));
        gaveBack = true;
    }
    if (gaveBack) {
        Count.PinWmaxToRate();
        Bytes.PinWmaxToRate();
        Counters.OnDrainAnchorGiveBack();
        ClampTokensToSoftCap(state);
        PublishCounters();
    }
}

bool TDrainRateController::IsQuietSinceHot(const TDrainState& state) const {
    if (state.AnyHotNode) {
        return false;
    }
    if (LastHotAt != TInstant::Zero() && (state.Now - LastHotAt).SecondsFloat() < HotCooldownSec()) {
        return false;
    }
    if (LastOverloadOutcomeAt != TInstant::Zero() && (state.Now - LastOverloadOutcomeAt).SecondsFloat() < HotCooldownSec()) {
        return false;
    }
    return true;
}

bool TDrainRateController::CanGrowNow(const TDrainState& state) const {
    if (!IsQuietSinceHot(state)) {
        return false;
    }
    if (LastGrowthAt != TInstant::Zero() && (state.Now - LastGrowthAt).SecondsFloat() < GrowthPeriodSec()) {
        return false;
    }
    return true;
}

void TDrainRateController::NoteQueueBecameNonEmpty(const TDrainState& state, ui64 firstBatchSize) {
    if (!WasQueueEmpty) {
        return;
    }
    WasQueueEmpty = false;

    // The queue just went non-empty: the incoming rate has exceeded what we sustained on the fast
    // path. Seed the drain rates from the observed throughput (x safety factor), or more
    // cautiously if we saw any overload while the queue was still empty. With no observation yet
    // (cold start) keep the current seeded rate.
    //
    // The fast-path EWMA is spacing-based (1/dt), so bursty arrivals read high: cap the seed with
    // the anchor, which is measured over whole windows while the queue was actually busy. Cold
    // start has no anchor yet and keeps the raw observation.
    //
    // Seeding may only raise the rate in a quiet window: right after a hot episode the decayed
    // rate is the current verdict, and re-seeding from fast-path traffic would hand the pressure
    // right back.
    const double factor = ObservedOverload ? ObserveOverloadFactor : ObserveSafetyFactor;
    const bool mayRaise = IsQuietSinceHot(state);
    if (ObservedRateCount > 0.0) {
        const double cap = Min(Count.EffectiveMax(), AnchorMaxCount());
        const double seed = Min(cap, Max(Count.EffectiveMin(), ObservedRateCount * factor));
        if (mayRaise || seed < Count.GetRate()) {
            Count.SetRate(seed);
        }
    }
    if (ObservedRateBytes > 0.0) {
        const double capBytes = Min(Bytes.EffectiveMax(), AnchorMaxBytes());
        const double seedBytes = Min(capBytes, Max(Bytes.EffectiveMin(), ObservedRateBytes * factor));
        if (mayRaise || seedBytes < Bytes.GetRate()) {
            Bytes.SetRate(seedBytes);
        }
    }
    // Seed to the soft one-cohort cap so the first waiter can drain immediately, then pace. The
    // bytes seed is raised to firstBatchSize so a single large first waiter is not stuck waiting
    // for tokens that the soft cap would otherwise refuse to accumulate.
    Count.ResetTokens(Count.SoftCap());
    Bytes.ResetTokens(Bytes.SoftCap(static_cast<double>(firstBatchSize)));
    Count.ResetRefillClock(state.Now);
    Bytes.ResetRefillClock(state.Now);
    // A real observation seed replaces the recovery curve. With no EWMA yet (cold empty ->
    // non-empty with no fast-path samples), keep any in-flight CUBIC epoch so a brief empty queue
    // between drain rounds does not throw away Wmax / KTarget progress.
    if (ObservedRateCount > 0.0 || ObservedRateBytes > 0.0) {
        Count.ResetWmaxToRate();
        Bytes.ResetWmaxToRate();
        CubicEpochStart = TInstant::Zero();
    }
    ObservedOverload = false;
    Counters.OnObservationTransition();
    PublishCounters();
}

void TDrainRateController::NoteQueueEmpty() {
    if (WasQueueEmpty) {
        return;
    }
    WasQueueEmpty = true;
    // Reopen the observation window. Do NOT reset the drain rates — AIMD keeps its learned value,
    // observation only re-seeds the *starting* rate at the next empty -> non-empty edge.
    LastObserveAt = TInstant::Zero();
    ObservedRateCount = 0.0;
    ObservedRateBytes = 0.0;
    ObservedOverload = false;
}

void TDrainRateController::NoteWaiterReleased() {
    // A cohort targets one full rate-worth of releases: exactly the "send `rate` requests, then
    // judge the result" round.
    if (!CohortOpen) {
        CohortOpen = true;
        CohortTarget = Max<ui64>(1, static_cast<ui64>(std::ceil(Count.GetRate())));
        CohortOkCount = 0;
        CohortOverloadCount = 0;
    }
}

void TDrainRateController::ResetCohort() {
    CohortOpen = false;
    CohortTarget = 0;
    CohortOkCount = 0;
    CohortOverloadCount = 0;
}

void TDrainRateController::NoteWriteOutcome(const TDrainState& state, const TDrainRateParams& params, EWriteOutcome outcome) {
    // If the outcome arrives while the queue is empty (fast-path traffic), remember that the
    // observed throughput was already causing overload, so the next empty -> non-empty transition
    // seeds the rates more cautiously.
    if (WasQueueEmpty && outcome == EWriteOutcome::Overloaded) {
        ObservedOverload = true;
    }
    Counters.OnWriteOutcome(outcome);
    // Outcomes drive growth/cuts together with hot-node edges. Re-read bounds here, otherwise the
    // RMax/RMin/CUBIC knobs stay at their construction-time seed.
    SyncBounds(params);
    NoteCohortOutcome(state, outcome);
}

void TDrainRateController::NoteCohortOutcome(const TDrainState& state, EWriteOutcome outcome) {
    if (outcome == EWriteOutcome::Unknown) {
        // A write that never heard back is evidence of nothing, so it neither cuts nor counts
        // toward the cohort. The cohort simply stays short of its target, which is the right
        // response: growth stops until real answers come back, without inventing a cut from what
        // may be a network fault.
        return;
    }
    const bool overloaded = outcome == EWriteOutcome::Overloaded;
    if (overloaded) {
        // Opens the same cooldown a hot node does: growth must wait for a quiet window.
        LastOverloadOutcomeAt = state.Now;
    }
    if (!CohortOpen) {
        // Outcome of a write that was not part of an open cohort (e.g. a fast-path admit with no
        // queueing, or an in-flight write that finished between cohorts). Overload still matters
        // as a cut signal, but a *single* stray overload must not apply the full AimdBeta: treat
        // it as 1/notionalCohort of a dirty round so severity matches in-cohort proportional cuts.
        if (overloaded) {
            const double notional = Max(1.0, std::ceil(Count.GetRate()));
            CutRateByOverloadFraction(state, 1.0 / notional);
        }
        return;
    }
    if (overloaded) {
        ++CohortOverloadCount;
    } else {
        ++CohortOkCount;
    }
    if (CohortOkCount + CohortOverloadCount >= CohortTarget) {
        CloseCohort(state);
    }
}

void TDrainRateController::EnsureCubicProbeEpoch(TInstant now) {
    // No recovery curve yet: treat the current rates as Wmax and start in the probe region
    // (t >= K) so clean cohorts add ProbePercent * Wmax without a convex climb from zero.
    if (CubicEpochStart != TInstant::Zero()) {
        return;
    }
    Count.EnterProbePhase();
    Bytes.EnterProbePhase();
    CubicEpochStart = now - TDuration::Seconds(Max(0.001, CubicRecoveryTargetSec));
}

void TDrainRateController::CloseCohort(const TDrainState& state) {
    const ui64 total = CohortOkCount + CohortOverloadCount;
    const ui64 overloads = CohortOverloadCount;
    ResetCohort();

    if (!total) {
        return;
    }

    if (overloads) {
        Counters.OnDrainCohortAborted();
        CutRateByOverloadFraction(state, static_cast<double>(overloads) / static_cast<double>(total));
        return;
    }

    // Clean round: CUBIC recovery toward Wmax, then a fractional probe above it. A clean cohort is
    // necessary but not sufficient — growth is additionally clocked (one step per GrowthPeriodSec)
    // and gated on a quiet window, because outcomes arrive per shard write and would otherwise
    // fire growth at the fan-out rate.
    if (!CanGrowNow(state)) {
        Counters.OnDrainGrowthBlocked();
        PublishCounters();
        return;
    }
    LastGrowthAt = state.Now;

    // Never grow past what the system actually takes from us (MaybeApplyAnchor pulls the rate back
    // down when it is already above).
    const double capCount = Min(Count.EffectiveMax(), AnchorMaxCount());
    const double capBytes = Min(Bytes.EffectiveMax(), AnchorMaxBytes());

    EnsureCubicProbeEpoch(state.Now);
    const double k = Max(0.001, CubicRecoveryTargetSec);
    const double t = Max(0.0, (state.Now - CubicEpochStart).SecondsFloat());

    bool grew = Count.Grow(t, k, CubicProbePercent, capCount);
    grew |= Bytes.Grow(t, k, CubicProbePercent, capBytes);
    if (grew) {
        Counters.OnDrainRateGrow();
    }
    PublishCounters();
}

void TDrainRateController::CutRateByOverloadFraction(const TDrainState& state, double overloadFraction) {
    // Proportional multiplicative decrease: a single overloaded write out of many need not halve
    // the rate, while an all-overloaded round applies the full AimdBeta. A meaningful drop resets
    // the CUBIC epoch (Wmax = pre-cut rates); tiny out-of-cohort nicks do not.
    const double fraction = Min(1.0, Max(0.0, overloadFraction));
    if (fraction <= 0.0) {
        return;
    }
    const double prev = Count.ScaleRate(1.0 - fraction * (1.0 - AimdBeta));
    const double prevBytes = Bytes.ScaleRate(1.0 - fraction * (1.0 - AimdBetaBytes));

    const bool meaningfulCount = prev > 0.0 && (prev / Max(Count.GetRate(), Count.EffectiveMin())) >= MeaningfulCutRatio;
    const bool meaningfulBytes = prevBytes > 0.0 && (prevBytes / Max(Bytes.GetRate(), Bytes.EffectiveMin())) >= MeaningfulCutRatio;
    if (meaningfulCount || meaningfulBytes) {
        const double k = Max(0.001, CubicRecoveryTargetSec);
        Count.StartCubicEpoch(prev, k);
        Bytes.StartCubicEpoch(prevBytes, k);
        CubicEpochStart = state.Now;
    }

    if (Count.GetRate() < prev || Bytes.GetRate() < prevBytes) {
        Counters.OnDrainRateCut();
    }
    ClampTokensToSoftCap(state);
    PublishCounters();
}

void TDrainRateController::ClampTokensToSoftCap(const TDrainState& state) {
    // After a rate cut, soft-cap tokens immediately so a READY edge cannot dump a pre-cut cohort's
    // worth of reserved budget.
    Count.CapTokens(Count.SoftCap());
    Bytes.CapTokens(Bytes.SoftCap(state.FrontWaiterBatchSize));
}

void TDrainRateController::NoteFirstHotNode(const TDrainState& state, const TDrainRateParams& params) {
    SyncBounds(params);
    // Drop an in-flight cohort: its clean OK outcomes are not a valid sample of the post-cut rate
    // under compaction pressure.
    ResetCohort();
    const double prev = Count.GetRate();
    const double prevBytes = Bytes.GetRate();
    CutRateByOverloadFraction(state, 1.0);
    // Compaction hot is not a discovered link limit — do not CUBIC-recover to the pre-cut peak
    // (with beta ~ 0.8 that undoes the cut within KTarget and then probes above it, recreating the
    // sawtooth). Pin Wmax at the post-cut rate so the next cool window only probes from here. Skip
    // when the floor absorbed the cut (rate unchanged) so a write-outcome CUBIC epoch is preserved.
    if (Count.GetRate() < prev || Bytes.GetRate() < prevBytes) {
        Count.ResetWmaxToRate();
        Bytes.ResetWmaxToRate();
        CubicEpochStart = state.Now - TDuration::Seconds(Max(0.001, CubicRecoveryTargetSec));
    }
}

void TDrainRateController::NoteHotNode(TInstant now) {
    LastHotAt = now;
    if (WasQueueEmpty) {
        ObservedOverload = true;
    }
}

void TDrainRateController::NoteAllNodesReady(const TDrainState& state) {
    // The cooldown is anchored to the recovery edge rather than to the last overload signal on
    // purpose: what has to stay quiet before growth resumes is the period *after* the burst that the
    // clamp below releases, not the overload that preceded it. In practice the two are the same
    // instant anyway, since ApplyHotDecay refreshes LastHotAt on every drain cycle while the node is
    // hot. Only a genuine hot -> cool edge gets here, see TNodeStateMap::MarkReady.
    LastHotAt = state.Now;
    // Asymmetric on purpose: the count bucket keeps at least one whole admit so the queue can
    // always move, while the bytes bucket keeps at least the head's batch so one large request is
    // not starved. Neither may keep the full budget accrued while admits were gated.
    Count.CapTokens(Max(1.0, std::ceil(Count.GetRate() * ReadyDumpFraction)));
    Bytes.CapTokens(Max(state.FrontWaiterBatchSize, Bytes.GetRate() * ReadyDumpFraction));
    // Restart the refill clock: the time the node spent hot must not be credited back as tokens
    // right after the clamp, or the drain loop immediately undoes it.
    Count.ResetRefillClock(state.Now);
    Bytes.ResetRefillClock(state.Now);
    PublishCounters();
}

}   // namespace NKikimr::NColumnShard::NFlowControl
