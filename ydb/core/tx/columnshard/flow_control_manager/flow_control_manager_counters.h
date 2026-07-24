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

    NMonitoring::TDynamicCounters::TCounterPtr RequestsCount;
    NMonitoring::TDynamicCounters::TCounterPtr AdmitAllowedCount;
    NMonitoring::TDynamicCounters::TCounterPtr AdmitRejectedCount;
    NMonitoring::TDynamicCounters::TCounterPtr AdmitSkippedNoSplitCount;
    NMonitoring::TDynamicCounters::TCounterPtr LocationRecheckCount;
    NMonitoring::TDynamicCounters::TCounterPtr StatusOverloadCount;
    NMonitoring::TDynamicCounters::TCounterPtr StatusReadyCount;

    NMonitoring::TDynamicCounters::TCounterPtr SplitDurationUs;
    NMonitoring::TDynamicCounters::TCounterPtr AdmitDurationUs;
    NMonitoring::THistogramPtr SplitDurationMsHistogram;
    NMonitoring::THistogramPtr AdmitDurationMsHistogram;
    NMonitoring::THistogramPtr WaitAdmitDurationMsHistogram;

public:
    TCSFlowControlManagerCounters(TIntrusivePtr<::NMonitoring::TDynamicCounters> countersGroup)
        : TBase("CSFlowControlManager", countersGroup)
        , RequestsInFlight(TBase::GetValue("FlowControl/Requests/InFlight"))
        , WaitingAdmitInFlight(TBase::GetValue("FlowControl/Admit/Waiting/InFlight"))
        , HotNodesCount(TBase::GetValue("FlowControl/HotNodes/Count"))
        , TabletToNodeCount(TBase::GetValue("FlowControl/TabletToNode/Count"))
        , RequestsCount(TBase::GetDeriviative("FlowControl/Requests/Count"))
        , AdmitAllowedCount(TBase::GetDeriviative("FlowControl/Admit/Allowed/Count"))
        , AdmitRejectedCount(TBase::GetDeriviative("FlowControl/Admit/Rejected/Count"))
        , AdmitSkippedNoSplitCount(TBase::GetDeriviative("FlowControl/Admit/SkippedNoSplit/Count"))
        , LocationRecheckCount(TBase::GetDeriviative("FlowControl/LocationRecheck/Count"))
        , StatusOverloadCount(TBase::GetDeriviative("FlowControl/Status/Overloaded/Count"))
        , StatusReadyCount(TBase::GetDeriviative("FlowControl/Status/Ready/Count"))
        , SplitDurationUs(TBase::GetDeriviative("FlowControl/Split/Duration/Us"))
        , AdmitDurationUs(TBase::GetDeriviative("FlowControl/Admit/Duration/Us"))
        , SplitDurationMsHistogram(TBase::GetHistogram("FlowControl/Split/DurationMs/Histogram", NMonitoring::ExponentialHistogram(20, 2, 1)))
        , AdmitDurationMsHistogram(TBase::GetHistogram("FlowControl/Admit/DurationMs/Histogram", NMonitoring::ExponentialHistogram(18, 2, 1)))
        , WaitAdmitDurationMsHistogram(
              TBase::GetHistogram("FlowControl/Admit/WaitDurationMs/Histogram", NMonitoring::ExponentialHistogram(18, 2, 1)))
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
};

}   // namespace NKikimr::NColumnShard::NFlowControl
