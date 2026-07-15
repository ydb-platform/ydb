#pragma once

#include <ydb/library/signals/owner.h>

namespace NKikimr::NColumnShard::NFlowControl {

class TCSFlowControlManagerCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    NMonitoring::TDynamicCounters::TCounterPtr QueueSize;

public:
    TCSFlowControlManagerCounters(TIntrusivePtr<::NMonitoring::TDynamicCounters> countersGroup)
        : TBase("CSFlowControlManager", countersGroup)
        , QueueSize(TBase::GetValue("FlowControl/Queue/Size"))
    {
    }

    void SetQueueSize(ui64 queueSize) const {
        QueueSize->Set(queueSize);
    }
};

}   // namespace NKikimr::NColumnShard::NFlowControl
