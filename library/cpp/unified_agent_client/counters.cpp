#include "counters.h"

using namespace NMonitoring;

namespace NUnifiedAgent {
    TClientCounters::TClientCounters(const NMonitoring::TDynamicCounterPtr& counters)
        : TDynamicCountersWrapper(counters)
        , ActiveSessionsCount(GetCounter("ActiveSessionsCount", false))
        , ClientLogDroppedBytes(GetCounter("ClientLogDroppedBytes", true))
    {
    }

    TIntrusivePtr<TClientSessionCounters> TClientCounters::GetDefaultSessionCounters() {
        auto group = Unwrap()->GetSubgroup("session", "default");
        return MakeIntrusive<TClientSessionCounters>(group);
    }

    TClientSessionCounters::TClientSessionCounters(const NMonitoring::TDynamicCounterPtr& counters)
        : TDynamicCountersWrapper(counters)
        , ReceivedMessages(GetCounter("ReceivedMessages", true))
        , ReceivedBytes(GetCounter("ReceivedBytes", true))
        , AcknowledgedMessages(GetCounter("AcknowledgedMessages", true))
        , AcknowledgedBytes(GetCounter("AcknowledgedBytes", true))
        , InflightMessages(GetCounter("InflightMessages", false))
        , InflightBytes(GetCounter("InflightBytes", false))
        , GrpcWriteBatchRequests(GetCounter("GrpcWriteBatchRequests", true))
        , GrpcInflightMessages(GetCounter("GrpcInflightMessages", false))
        , GrpcInflightBytes(GetCounter("GrpcInflightBytes", false))
        , GrpcCalls(GetCounter("GrpcCalls", true))
        , GrpcCallsInitialized(GetCounter("GrpcCallsInitialized", true))
        , DroppedMessages(GetCounter("DroppedMessages", true))
        , DroppedBytes(GetCounter("DroppedBytes", true))
        , ErrorsCount(GetCounter("ErrorsCount", true))
    {
    }
}
