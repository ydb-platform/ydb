#include "counters.h"

namespace NKikimr::NPrioritiesQueue {

 TCounters::TCounters(const TString& queueName, TIntrusivePtr<::NMonitoring::TDynamicCounters> baseSignals)
    : TBase("Priorities/" + queueName, baseSignals)
    , UsedCount(TBase::GetValue("UsedCount"))
    , Using(TBase::GetDeriviative("Using"))
    , Ask(TBase::GetDeriviative("Ask"))
    , AskMax(TBase::GetDeriviative("AskMax"))
    , Free(TBase::GetDeriviative("Free"))
    , FreeNoClient(TBase::GetDeriviative("FreeNoClient"))
    , Register(TBase::GetDeriviative("Register"))
    , Unregister(TBase::GetDeriviative("Unregister"))
    , QueueSize(TBase::GetValue("QueueSize"))
    , Clients(TBase::GetValue("Clients"))
    , Limit(TBase::GetValue("Limit")) {
}

}
