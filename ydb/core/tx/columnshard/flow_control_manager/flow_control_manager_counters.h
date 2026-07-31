#pragma once

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
    NMonitoring::TDynamicCounters::TCounterPtr DrainAllowedCount;
    NMonitoring::TDynamicCounters::TCounterPtr DrainRateCutCount;
    NMonitoring::TDynamicCounters::TCounterPtr DrainRateGrowCount;

    NMonitoring::TDynamicCounters::TCounterPtr RequestsCount;
    NMonitoring::TDynamicCounters::TCounterPtr AdmitAllowedCount;
    NMonitoring::TDynamicCounters::TCounterPtr AdmitRejectedCount;
    NMonitoring::TDynamicCounters::TCounterPtr AdmitSkippedNoSplitCount;
    NMonitoring::TDynamicCounters::TCounterPtr WaitQueueEnqueuedCount;
    NMonitoring::TDynamicCounters::TCounterPtr WaitQueueDrainedCount;
    NMonitoring::TDynamicCounters::TCounterPtr WaitQueueRejectedDeadlineCount;
    NMonitoring::TDynamicCounters::TCounterPtr WaitQueueTimedOutCount;
    NMonitoring::TDynamicCounters::TCounterPtr WaitQueueCancelledCount;
    NMonitoring::TDynamicCounters::TCounterPtr WaitQueueRejectedFullCount;
    NMonitoring::TDynamicCounters::TCounterPtr DelayedRejectEnqueuedCount;
    NMonitoring::TDynamicCounters::TCounterPtr DelayedRejectFiredCount;
    NMonitoring::TDynamicCounters::TCounterPtr DelayedRejectQueueFullCount;
    NMonitoring::TDynamicCounters::TCounterPtr LocationRecheckCount;
    NMonitoring::TDynamicCounters::TCounterPtr StatusOverloadCount;
    NMonitoring::TDynamicCounters::TCounterPtr StatusReadyCount;

    NMonitoring::TDynamicCounters::TCounterPtr SplitDurationUs;
    NMonitoring::TDynamicCounters::TCounterPtr AdmitDurationUs;
    NMonitoring::THistogramPtr SplitDurationMsHistogram;
    NMonitoring::THistogramPtr AdmitDurationMsHistogram;
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
        , DrainAllowedCount(TBase::GetDeriviative("FlowControl/Drain/Allowed/Count"))
        , DrainRateCutCount(TBase::GetDeriviative("FlowControl/Drain/RateCut/Count"))
        , DrainRateGrowCount(TBase::GetDeriviative("FlowControl/Drain/RateGrow/Count"))
        , RequestsCount(TBase::GetDeriviative("FlowControl/Requests/Count"))
        , AdmitAllowedCount(TBase::GetDeriviative("FlowControl/Admit/Allowed/Count"))
        , AdmitRejectedCount(TBase::GetDeriviative("FlowControl/Admit/Rejected/Count"))
        , AdmitSkippedNoSplitCount(TBase::GetDeriviative("FlowControl/Admit/SkippedNoSplit/Count"))
        , WaitQueueEnqueuedCount(TBase::GetDeriviative("FlowControl/WaitQueue/Enqueued/Count"))
        , WaitQueueDrainedCount(TBase::GetDeriviative("FlowControl/WaitQueue/Drained/Count"))
        , WaitQueueRejectedDeadlineCount(TBase::GetDeriviative("FlowControl/WaitQueue/RejectedDeadline/Count"))
        , WaitQueueTimedOutCount(TBase::GetDeriviative("FlowControl/WaitQueue/TimedOut/Count"))
        , WaitQueueCancelledCount(TBase::GetDeriviative("FlowControl/WaitQueue/Cancelled/Count"))
        , WaitQueueRejectedFullCount(TBase::GetDeriviative("FlowControl/WaitQueue/RejectedFull/Count"))
        , DelayedRejectEnqueuedCount(TBase::GetDeriviative("FlowControl/DelayedRejectQueue/Enqueued/Count"))
        , DelayedRejectFiredCount(TBase::GetDeriviative("FlowControl/DelayedRejectQueue/Fired/Count"))
        , DelayedRejectQueueFullCount(TBase::GetDeriviative("FlowControl/DelayedRejectQueue/Full/Count"))
        , LocationRecheckCount(TBase::GetDeriviative("FlowControl/LocationRecheck/Count"))
        , StatusOverloadCount(TBase::GetDeriviative("FlowControl/Status/Overloaded/Count"))
        , StatusReadyCount(TBase::GetDeriviative("FlowControl/Status/Ready/Count"))
        , SplitDurationUs(TBase::GetDeriviative("FlowControl/Split/Duration/Us"))
        , AdmitDurationUs(TBase::GetDeriviative("FlowControl/Admit/Duration/Us"))
        , SplitDurationMsHistogram(TBase::GetHistogram("FlowControl/Split/DurationMs/Histogram", NMonitoring::ExponentialHistogram(20, 2, 1)))
        , AdmitDurationMsHistogram(TBase::GetHistogram("FlowControl/Admit/DurationMs/Histogram", NMonitoring::ExponentialHistogram(18, 2, 1)))
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

    void OnAdmitAllowed(const TDuration duration) const {
        AdmitAllowedCount->Inc();
        AdmitDurationUs->Add(duration.MicroSeconds());
        AdmitDurationMsHistogram->Collect(duration.MilliSeconds());
    }

    void OnAdmitRejected(const TDuration duration) const {
        AdmitRejectedCount->Inc();
        AdmitDurationUs->Add(duration.MicroSeconds());
        AdmitDurationMsHistogram->Collect(duration.MilliSeconds());
    }

    void OnAdmitSkippedNoSplit() const {
        AdmitSkippedNoSplitCount->Inc();
    }

    void OnWaitQueueEnqueue() const {
        WaitQueueEnqueuedCount->Inc();
        WaitQueueCount->Inc();
    }

    void OnWaitQueueDrain(const TDuration waited) const {
        WaitQueueDrainedCount->Inc();
        WaitQueueCount->Dec();
        WaitQueueWaitDurationMsHistogram->Collect(waited.MilliSeconds());
    }

    void OnWaitQueueRejectDeadlineAtAdmit() const {
        WaitQueueRejectedDeadlineCount->Inc();
    }

    void OnWaitQueueRejectDeadline(const TDuration waited) const {
        WaitQueueRejectedDeadlineCount->Inc();
        WaitQueueCount->Dec();
        WaitQueueWaitDurationMsHistogram->Collect(waited.MilliSeconds());
    }

    void OnWaitQueueRejectFull() const {
        WaitQueueRejectedFullCount->Inc();
    }

    // A queued waiter hit its WaitDeadline while still waiting: distinct from a
    // client-initiated cancel. Also drops the WaitQueue/Count gauge.
    void OnWaitQueueTimedOut() const {
        WaitQueueTimedOutCount->Inc();
        WaitQueueCount->Dec();
    }

    // A queued waiter was cancelled by the client (not a deadline timeout).
    // Also drops the WaitQueue/Count gauge.
    void OnWaitQueueCancelled() const {
        WaitQueueCancelledCount->Inc();
        WaitQueueCount->Dec();
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

    void OnDrainAllowed() const {
        DrainAllowedCount->Inc();
    }

    void OnDrainRateCut() const {
        DrainRateCutCount->Inc();
    }

    void OnDrainRateGrow() const {
        DrainRateGrowCount->Inc();
    }

    void OnLocationRecheck() const {
        LocationRecheckCount->Inc();
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
        DelayedRejectQueueCount->Inc();
    }

    void OnDelayedRejectFired() const {
        DelayedRejectFiredCount->Inc();
        DelayedRejectQueueCount->Dec();
    }

    void OnDelayedRejectQueueFull() const {
        DelayedRejectQueueFullCount->Inc();
    }

    void SetDelayedRejectQueueCount(ui64 count) const {
        DelayedRejectQueueCount->Set(count);
    }
};

}   // namespace NKikimr::NColumnShard::NFlowControl
