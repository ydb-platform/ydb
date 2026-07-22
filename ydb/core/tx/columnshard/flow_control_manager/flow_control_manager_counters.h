#pragma once

#include <ydb/library/signals/owner.h>

namespace NKikimr::NColumnShard::NFlowControl {

class TCSFlowControlManagerCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    NMonitoring::TDynamicCounters::TCounterPtr QueueSize;
    NMonitoring::TDynamicCounters::TCounterPtr RequestsCount;

public:
    TCSFlowControlManagerCounters(TIntrusivePtr<::NMonitoring::TDynamicCounters> countersGroup)
        : TBase("CSFlowControlManager", countersGroup)
        , QueueSize(TBase::GetValue("FlowControl/Queue/Size"))
        , RequestsCount(TBase::GetDeriviative("FlowControl/Requests/Count"))
    {
    }

    void SetQueueSize(ui64 queueSize) const {
        QueueSize->Set(queueSize);
    }

    void OnNewRequest() const {
        RequestsCount->Inc();
    }
};

}   // namespace NKikimr::NColumnShard::NFlowControl
