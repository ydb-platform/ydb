#pragma once
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <ydb/core/tx/columnshard/counters/common/owner.h>

namespace NKikimr::NOlap::NBlobOperations {

class TConsumerCounters;

class TRemoveGCCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    NMonitoring::TDynamicCounters::TCounterPtr RequestsCount;
    NMonitoring::TDynamicCounters::TCounterPtr RequestBytes;

    NMonitoring::TDynamicCounters::TCounterPtr RepliesCount;
    NMonitoring::TDynamicCounters::TCounterPtr ReplyBytes;

    NMonitoring::TDynamicCounters::TCounterPtr GCFinishedCount;
    NMonitoring::THistogramPtr GCFinishedBlobsCount;
    NMonitoring::THistogramPtr GCFinishedBytes;

    NMonitoring::TDynamicCounters::TCounterPtr FailsCount;
    NMonitoring::TDynamicCounters::TCounterPtr FailBytes;
public:
    TRemoveGCCounters(const TConsumerCounters& owner);

    void OnGCFinished(const ui64 bytes, const ui32 count) const {
        GCFinishedCount->Add(1);
        GCFinishedBytes->Collect(bytes);
        GCFinishedBlobsCount->Collect(count);
    }

    void OnRequest(const ui64 bytes) const {
        RequestsCount->Add(1);
        RequestBytes->Add(bytes);
    }

    void OnReply(const ui64 bytes) const {
        RepliesCount->Add(1);
        ReplyBytes->Add(bytes);
    }

    void OnFail(const ui64 bytes) const {
        FailsCount->Add(1);
        FailBytes->Add(bytes);
    }
};

}
