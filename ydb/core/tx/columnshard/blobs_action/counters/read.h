#pragma once
#include <ydb/library/signals/owner.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NOlap::NBlobOperations {

class TConsumerCounters;

class TReadCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    NMonitoring::TDynamicCounters::TCounterPtr RequestsCount;
    NMonitoring::TDynamicCounters::TCounterPtr RequestBytes;

    NMonitoring::TDynamicCounters::TCounterPtr RepliesCount;
    NMonitoring::TDynamicCounters::TCounterPtr ReplyBytes;
    NMonitoring::THistogramPtr ReplyDurationByCount;
    NMonitoring::THistogramPtr ReplyDurationBySize;

    NMonitoring::TDynamicCounters::TCounterPtr FailsCount;
    NMonitoring::TDynamicCounters::TCounterPtr FailBytes;
    NMonitoring::THistogramPtr FailDurationByCount;
    NMonitoring::THistogramPtr FailDurationBySize;

    NMonitoring::TDynamicCounters::TCounterPtr RetryEnqueueCount;
    NMonitoring::TDynamicCounters::TCounterPtr RetryEnqueueBytes;
    NMonitoring::TDynamicCounters::TCounterPtr RetryExecuteCount;
    NMonitoring::TDynamicCounters::TCounterPtr RetryExhaustedCount;

public:
    TReadCounters(const TConsumerCounters& owner);

    void OnRequest(const ui64 bytes) const {
        RequestsCount->Add(1);
        RequestBytes->Add(bytes);
    }

    void OnReply(const ui64 bytes, const TDuration d) const {
        RepliesCount->Add(1);
        ReplyBytes->Add(bytes);
        ReplyDurationByCount->Collect((i64)d.MilliSeconds());
        ReplyDurationBySize->Collect((i64)d.MilliSeconds(), (i64)bytes);
    }

    void OnFail(const ui64 bytes, const TDuration d) const {
        FailsCount->Add(1);
        FailBytes->Add(bytes);
        FailDurationByCount->Collect((i64)d.MilliSeconds());
        FailDurationBySize->Collect((i64)d.MilliSeconds(), (i64)bytes);
    }

    void OnRetryEnqueue(const ui64 bytes) const {
        RetryEnqueueCount->Add(1);
        RetryEnqueueBytes->Add(bytes);
    }

    void OnRetryExecute() const {
        RetryExecuteCount->Add(1);
    }

    void OnRetryExhausted() const {
        RetryExhaustedCount->Add(1);
    }
};

}   // namespace NKikimr::NOlap::NBlobOperations
