#pragma once
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <ydb/core/tx/columnshard/counters/common/owner.h>

namespace NKikimr::NOlap::NBlobOperations {

class TConsumerCounters;

class TWriteCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    NMonitoring::TDynamicCounters::TCounterPtr RequestsCount;
    NMonitoring::TDynamicCounters::TCounterPtr RequestBytes;

    NMonitoring::TDynamicCounters::TCounterPtr RepliesCount;
    NMonitoring::TDynamicCounters::TCounterPtr ReplyBytes;
    NMonitoring::THistogramPtr ReplyDuration;

    NMonitoring::TDynamicCounters::TCounterPtr FailsCount;
    NMonitoring::TDynamicCounters::TCounterPtr FailBytes;
    NMonitoring::THistogramPtr FailDuration;
public:
    TWriteCounters(const TConsumerCounters& owner);

    void OnRequest(const ui64 bytes) const {
        RequestsCount->Add(1);
        RequestBytes->Add(bytes);
    }

    void OnReply(const ui64 bytes, const TDuration d) const {
        RepliesCount->Add(1);
        ReplyBytes->Add(bytes);
        ReplyDuration->Collect(d.MilliSeconds());
    }

    void OnFail(const ui64 bytes, const TDuration d) const {
        FailsCount->Add(1);
        FailBytes->Add(bytes);
        FailDuration->Collect(d.MilliSeconds());
    }
};

}
