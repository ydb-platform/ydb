#pragma once

#include "flow_control_manager_types.h"

#include <ydb/library/signals/owner.h>

#include <util/datetime/base.h>

namespace NKikimr::NColumnShard::NFlowControl {

class TCSFlowControlManagerCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;

    NMonitoring::TDynamicCounters::TCounterPtr RequestsInFlight;
    NMonitoring::TDynamicCounters::TCounterPtr WaitingAdmitInFlight;
    NMonitoring::TDynamicCounters::TCounterPtr HotNodesCount;
    NMonitoring::TDynamicCounters::TCounterPtr TabletToNodeCount;
    NMonitoring::TDynamicCounters::TCounterPtr WaitQueueCount;
    NMonitoring::TDynamicCounters::TCounterPtr DelayedRejectQueueCount;

    NMonitoring::TDynamicCounters::TCounterPtr DrainRefillRate;
    NMonitoring::TDynamicCounters::TCounterPtr DrainTokens;
    NMonitoring::TDynamicCounters::TCounterPtr DrainRefillRateBytes;
    NMonitoring::TDynamicCounters::TCounterPtr DrainTokensBytes;
    NMonitoring::TDynamicCounters::TCounterPtr ObservedRateCount;
    NMonitoring::TDynamicCounters::TCounterPtr ObservedRateBytes;
    NMonitoring::TDynamicCounters::TCounterPtr ServedRateCount;
    NMonitoring::TDynamicCounters::TCounterPtr ServedRateBytes;
    NMonitoring::TDynamicCounters::TCounterPtr ObservationTransitionCount;
    NMonitoring::TDynamicCounters::TCounterPtr DrainAllowedCount;
    NMonitoring::TDynamicCounters::TCounterPtr DrainRateCutCount;
    NMonitoring::TDynamicCounters::TCounterPtr DrainRateGrowCount;
    NMonitoring::TDynamicCounters::TCounterPtr DrainRateDecayCount;
    NMonitoring::TDynamicCounters::TCounterPtr DrainGrowthBlockedCount;
    NMonitoring::TDynamicCounters::TCounterPtr DrainAnchorGiveBackCount;
    NMonitoring::TDynamicCounters::TCounterPtr DrainCohortAbortedCount;
    NMonitoring::TDynamicCounters::TCounterPtr DrainOutcomeOkCount;
    NMonitoring::TDynamicCounters::TCounterPtr DrainOutcomeOverloadedCount;
    NMonitoring::TDynamicCounters::TCounterPtr DrainOutcomeUnknownCount;

    NMonitoring::TDynamicCounters::TCounterPtr RequestsCount;
    NMonitoring::TDynamicCounters::TCounterPtr AdmitAllowedCount;
    NMonitoring::TDynamicCounters::TCounterPtr AdmitRejectedCount;
    NMonitoring::TDynamicCounters::TCounterPtr AdmitSkippedNoSplitCount;
    NMonitoring::TDynamicCounters::TCounterPtr AdmitSkippedUnavailableCount;
    NMonitoring::TDynamicCounters::TCounterPtr WaitQueueEnqueuedCount;
    NMonitoring::TDynamicCounters::TCounterPtr WaitQueueDrainedCount;
    NMonitoring::TDynamicCounters::TCounterPtr WaitQueueRejectedDeadlineCount;
    NMonitoring::TDynamicCounters::TCounterPtr WaitQueueTimedOutCount;
    NMonitoring::TDynamicCounters::TCounterPtr WaitQueueCancelledCount;
    NMonitoring::TDynamicCounters::TCounterPtr WaitQueueRejectedFullCount;
    NMonitoring::TDynamicCounters::TCounterPtr DelayedRejectEnqueuedCount;
    NMonitoring::TDynamicCounters::TCounterPtr DelayedRejectFiredCount;
    NMonitoring::TDynamicCounters::TCounterPtr DelayedRejectQueueFullCount;
    NMonitoring::TDynamicCounters::TCounterPtr NodeRecheckCount;
    NMonitoring::TDynamicCounters::TCounterPtr StatusOverloadCount;
    NMonitoring::TDynamicCounters::TCounterPtr StatusReadyCount;

    NMonitoring::TDynamicCounters::TCounterPtr SplitDurationUs;
    NMonitoring::THistogramPtr SplitDurationMsHistogram;
    NMonitoring::THistogramPtr WaitAdmitDurationMsHistogram;
    NMonitoring::THistogramPtr WaitQueueWaitDurationMsHistogram;

public:
    TCSFlowControlManagerCounters(TIntrusivePtr<::NMonitoring::TDynamicCounters> countersGroup)
        : TBase("CSFlowControlManager", countersGroup)
        , RequestsInFlight(TBase::GetValue("FlowControl/Requests/InFlight"))
        , WaitingAdmitInFlight(TBase::GetValue("FlowControl/Admit/Waiting/InFlight"))
        , HotNodesCount(TBase::GetValue("FlowControl/HotNodes/Count"))
        , TabletToNodeCount(TBase::GetValue("FlowControl/TabletToNode/Count"))
        , WaitQueueCount(TBase::GetValue("FlowControl/WaitQueue/Count"))
        , DelayedRejectQueueCount(TBase::GetValue("FlowControl/DelayedRejectQueue/Count"))
        , DrainRefillRate(TBase::GetValue("FlowControl/Drain/RefillRate"))
        , DrainTokens(TBase::GetValue("FlowControl/Drain/Tokens"))
        , DrainRefillRateBytes(TBase::GetValue("FlowControl/Drain/RefillRateBytes"))
        , DrainTokensBytes(TBase::GetValue("FlowControl/Drain/TokensBytes"))
        , ObservedRateCount(TBase::GetValue("FlowControl/Observe/RateCount"))
        , ObservedRateBytes(TBase::GetValue("FlowControl/Observe/RateBytes"))
        , ServedRateCount(TBase::GetValue("FlowControl/Drain/ServedRateCount"))
        , ServedRateBytes(TBase::GetValue("FlowControl/Drain/ServedRateBytes"))
        , ObservationTransitionCount(TBase::GetDeriviative("FlowControl/Observe/Transition/Count"))
        , DrainAllowedCount(TBase::GetDeriviative("FlowControl/Drain/Allowed/Count"))
        , DrainRateCutCount(TBase::GetDeriviative("FlowControl/Drain/RateCut/Count"))
        , DrainRateGrowCount(TBase::GetDeriviative("FlowControl/Drain/RateGrow/Count"))
        , DrainRateDecayCount(TBase::GetDeriviative("FlowControl/Drain/RateDecay/Count"))
        , DrainGrowthBlockedCount(TBase::GetDeriviative("FlowControl/Drain/GrowthBlocked/Count"))
        , DrainAnchorGiveBackCount(TBase::GetDeriviative("FlowControl/Drain/AnchorGiveBack/Count"))
        , DrainCohortAbortedCount(TBase::GetDeriviative("FlowControl/Drain/CohortAborted/Count"))
        , DrainOutcomeOkCount(TBase::GetDeriviative("FlowControl/Drain/Outcome/Ok/Count"))
        , DrainOutcomeOverloadedCount(TBase::GetDeriviative("FlowControl/Drain/Outcome/Overloaded/Count"))
        , DrainOutcomeUnknownCount(TBase::GetDeriviative("FlowControl/Drain/Outcome/Unknown/Count"))
        , RequestsCount(TBase::GetDeriviative("FlowControl/Requests/Count"))
        , AdmitAllowedCount(TBase::GetDeriviative("FlowControl/Admit/Allowed/Count"))
        , AdmitRejectedCount(TBase::GetDeriviative("FlowControl/Admit/Rejected/Count"))
        , AdmitSkippedNoSplitCount(TBase::GetDeriviative("FlowControl/Admit/SkippedNoSplit/Count"))
        , AdmitSkippedUnavailableCount(TBase::GetDeriviative("FlowControl/Admit/SkippedUnavailable/Count"))
        , WaitQueueEnqueuedCount(TBase::GetDeriviative("FlowControl/WaitQueue/Enqueued/Count"))
        , WaitQueueDrainedCount(TBase::GetDeriviative("FlowControl/WaitQueue/Drained/Count"))
        , WaitQueueRejectedDeadlineCount(TBase::GetDeriviative("FlowControl/WaitQueue/RejectedDeadline/Count"))
        , WaitQueueTimedOutCount(TBase::GetDeriviative("FlowControl/WaitQueue/TimedOut/Count"))
        , WaitQueueCancelledCount(TBase::GetDeriviative("FlowControl/WaitQueue/Cancelled/Count"))
        , WaitQueueRejectedFullCount(TBase::GetDeriviative("FlowControl/WaitQueue/RejectedFull/Count"))
        , DelayedRejectEnqueuedCount(TBase::GetDeriviative("FlowControl/DelayedRejectQueue/Enqueued/Count"))
        , DelayedRejectFiredCount(TBase::GetDeriviative("FlowControl/DelayedRejectQueue/Fired/Count"))
        , DelayedRejectQueueFullCount(TBase::GetDeriviative("FlowControl/DelayedRejectQueue/Full/Count"))
        , NodeRecheckCount(TBase::GetDeriviative("FlowControl/NodeRecheck/Count"))
        , StatusOverloadCount(TBase::GetDeriviative("FlowControl/Status/Overloaded/Count"))
        , StatusReadyCount(TBase::GetDeriviative("FlowControl/Status/Ready/Count"))
        , SplitDurationUs(TBase::GetDeriviative("FlowControl/Split/Duration/Us"))
        , SplitDurationMsHistogram(TBase::GetHistogram("FlowControl/Split/DurationMs/Histogram", NMonitoring::ExponentialHistogram(20, 2, 1)))
        , WaitAdmitDurationMsHistogram(
              TBase::GetHistogram("FlowControl/Admit/WaitDurationMs/Histogram", NMonitoring::ExponentialHistogram(18, 2, 1)))
        , WaitQueueWaitDurationMsHistogram(
              TBase::GetHistogram("FlowControl/WaitQueue/WaitDurationMs/Histogram", NMonitoring::ExponentialHistogram(18, 2, 1)))
    {
    }

    void OnRequestStart() const {
        RequestsCount->Inc();
        RequestsInFlight->Inc();
    }

    void OnRequestFinish() const {
        RequestsInFlight->Dec();
    }

    void OnWaitingAdmitStart() const {
        WaitingAdmitInFlight->Inc();
    }

    void OnWaitingAdmitFinish(const TDuration wait) const {
        WaitingAdmitInFlight->Dec();
        WaitAdmitDurationMsHistogram->Collect(wait.MilliSeconds());
    }

    void OnSplitFinished(const TDuration duration) const {
        SplitDurationUs->Add(duration.MicroSeconds());
        SplitDurationMsHistogram->Collect(duration.MilliSeconds());
    }

    void OnAdmitAllowed() const {
        AdmitAllowedCount->Inc();
    }

    void OnAdmitRejected() const {
        AdmitRejectedCount->Inc();
    }

    void OnAdmitSkippedNoSplit() const {
        AdmitSkippedNoSplitCount->Inc();
    }

    // The admit request could not be delivered, or was never answered: the write proceeds
    // unthrottled. Non-zero here means flow control is silently not running on this node.
    void OnAdmitSkippedUnavailable() const {
        AdmitSkippedUnavailableCount->Inc();
    }

    // WaitQueue/Count and DelayedRejectQueue/Count are gauges owned solely by
    // SetWaitQueueCount / SetDelayedRejectQueueCount, which PublishMapSizes calls on every
    // queue mutation. The On* events must never also Inc/Dec them: a drain does both, which
    // used to drift the wait-queue gauge down by one per drain.
    void OnWaitQueueEnqueue() const {
        WaitQueueEnqueuedCount->Inc();
    }

    void OnWaitQueueDrain(const TDuration waited) const {
        WaitQueueDrainedCount->Inc();
        WaitQueueWaitDurationMsHistogram->Collect(waited.MilliSeconds());
    }

    void OnWaitQueueRejectDeadlineAtAdmit() const {
        WaitQueueRejectedDeadlineCount->Inc();
    }

    void OnWaitQueueRejectFull() const {
        WaitQueueRejectedFullCount->Inc();
    }

    // A queued waiter hit its WaitDeadline while still waiting: distinct from a
    // client-initiated cancel.
    void OnWaitQueueTimedOut() const {
        WaitQueueTimedOutCount->Inc();
    }

    // A queued waiter was cancelled by the client (not a deadline timeout).
    void OnWaitQueueCancelled() const {
        WaitQueueCancelledCount->Inc();
    }

    void SetWaitQueueCount(ui64 count) const {
        WaitQueueCount->Set(count);
    }

    void SetDrainRefillRate(ui64 rate) const {
        DrainRefillRate->Set(rate);
    }

    void SetDrainTokens(ui64 tokens) const {
        DrainTokens->Set(tokens);
    }

    void SetDrainRefillRateBytes(ui64 rate) const {
        DrainRefillRateBytes->Set(rate);
    }

    void SetDrainTokensBytes(ui64 tokens) const {
        DrainTokensBytes->Set(tokens);
    }

    void SetObservedRateCount(ui64 rate) const {
        ObservedRateCount->Set(rate);
    }

    void SetObservedRateBytes(ui64 rate) const {
        ObservedRateBytes->Set(rate);
    }

    // Throughput FCM actually admits (fast path + drains), measured over closed windows.
    // The drain rates are anchored to it, so RefillRate* far above these means the
    // buckets are slack and the rate is meaningless.
    void SetServedRateCount(ui64 rate) const {
        ServedRateCount->Set(rate);
    }

    void SetServedRateBytes(ui64 rate) const {
        ServedRateBytes->Set(rate);
    }

    // A wait queue went from empty to non-empty and the drain rates were seeded from the
    // observed fast-path throughput.
    void OnObservationTransition() const {
        ObservationTransitionCount->Inc();
    }

    void OnDrainAllowed() const {
        DrainAllowedCount->Inc();
    }

    void OnDrainRateCut() const {
        DrainRateCutCount->Inc();
    }

    void OnDrainRateGrow() const {
        DrainRateGrowCount->Inc();
    }

    // Continuous multiplicative decay applied while at least one node is hot.
    void OnDrainRateDecay() const {
        DrainRateDecayCount->Inc();
    }

    // A growth opportunity was suppressed (hot node, hot cooldown, recent overloaded
    // outcome, or the per-period growth budget was already spent).
    void OnDrainGrowthBlocked() const {
        DrainGrowthBlockedCount->Inc();
    }

    // The rate was above the served-throughput anchor and was pulled back toward it.
    void OnDrainAnchorGiveBack() const {
        DrainAnchorGiveBackCount->Inc();
    }

    // A cohort completed but contained at least one overloaded write, so no growth.
    void OnDrainCohortAborted() const {
        DrainCohortAbortedCount->Inc();
    }

    // Per-request write outcome reported by TShardWriter. A rising Unknown rate means writes are
    // ending without an answer, which stalls cohort completion and therefore growth.
    void OnWriteOutcome(EWriteOutcome outcome) const {
        switch (outcome) {
            case EWriteOutcome::Ok:
                DrainOutcomeOkCount->Inc();
                break;
            case EWriteOutcome::Overloaded:
                DrainOutcomeOverloadedCount->Inc();
                break;
            case EWriteOutcome::Unknown:
                DrainOutcomeUnknownCount->Inc();
                break;
        }
    }

    void OnNodeRecheck() const {
        NodeRecheckCount->Inc();
    }

    void OnStatusOverloaded() const {
        StatusOverloadCount->Inc();
    }

    void OnStatusReady() const {
        StatusReadyCount->Inc();
    }

    void SetHotNodesCount(ui64 count) const {
        HotNodesCount->Set(count);
    }

    void SetTabletToNodeCount(ui64 count) const {
        TabletToNodeCount->Set(count);
    }

    void OnDelayedRejectEnqueue() const {
        DelayedRejectEnqueuedCount->Inc();
    }

    void OnDelayedRejectFired() const {
        DelayedRejectFiredCount->Inc();
    }

    void OnDelayedRejectQueueFull() const {
        DelayedRejectQueueFullCount->Inc();
    }

    void SetDelayedRejectQueueCount(ui64 count) const {
        DelayedRejectQueueCount->Set(count);
    }
};

}   // namespace NKikimr::NColumnShard::NFlowControl
