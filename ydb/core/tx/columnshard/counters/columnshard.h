#pragma once
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include "common/owner.h"

namespace NKikimr::NColumnShard {

class TCSCounters: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;

    NMonitoring::TDynamicCounters::TCounterPtr StartBackgroundCount;
    NMonitoring::TDynamicCounters::TCounterPtr TooEarlyBackgroundCount;
    NMonitoring::TDynamicCounters::TCounterPtr SetupCompactionCount;
    NMonitoring::TDynamicCounters::TCounterPtr SetupIndexationCount;
    NMonitoring::TDynamicCounters::TCounterPtr SetupTtlCount;
    NMonitoring::TDynamicCounters::TCounterPtr SetupCleanupCount;

    NMonitoring::TDynamicCounters::TCounterPtr SkipIndexationInputDueToGranuleOverloadBytes;
    NMonitoring::TDynamicCounters::TCounterPtr SkipIndexationInputDueToGranuleOverloadCount;
    NMonitoring::TDynamicCounters::TCounterPtr SkipIndexationInputDueToSplitCompactionBytes;
    NMonitoring::TDynamicCounters::TCounterPtr SkipIndexationInputDueToSplitCompactionCount;
    NMonitoring::TDynamicCounters::TCounterPtr FutureIndexationInputBytes;
    NMonitoring::TDynamicCounters::TCounterPtr IndexationInputBytes;

    NMonitoring::TDynamicCounters::TCounterPtr OverloadInsertTableBytes;
    NMonitoring::TDynamicCounters::TCounterPtr OverloadInsertTableCount;
    NMonitoring::TDynamicCounters::TCounterPtr OverloadShardBytes;
    NMonitoring::TDynamicCounters::TCounterPtr OverloadShardCount;

    std::shared_ptr<TValueAggregationClient> InternalCompactionGranuleBytes;
    std::shared_ptr<TValueAggregationClient> InternalCompactionGranulePortionsCount;

    std::shared_ptr<TValueAggregationClient> SplitCompactionGranuleBytes;
    std::shared_ptr<TValueAggregationClient> SplitCompactionGranulePortionsCount;

    NMonitoring::THistogramPtr HistogramSuccessWritePutBlobsDurationMs;
    NMonitoring::THistogramPtr HistogramFailedWritePutBlobsDurationMs;
    NMonitoring::THistogramPtr HistogramWriteTxCompleteDurationMs;
    NMonitoring::TDynamicCounters::TCounterPtr WritePutBlobsCount;
    NMonitoring::TDynamicCounters::TCounterPtr WriteRequests;
    NMonitoring::TDynamicCounters::TCounterPtr FailedWriteRequests;
    NMonitoring::TDynamicCounters::TCounterPtr SuccessWriteRequests;
public:
    void OnStartWriteRequest() const {
        WriteRequests->Add(1);
    }

    void OnFailedWriteResponse() const {
        WriteRequests->Sub(1);
        FailedWriteRequests->Add(1);
    }

    void OnSuccessWriteResponse() const {
        WriteRequests->Sub(1);
        SuccessWriteRequests->Add(1);
    }

    void OnWritePutBlobsSuccess(const ui32 milliseconds) const {
        HistogramSuccessWritePutBlobsDurationMs->Collect(milliseconds);
        WritePutBlobsCount->Sub(1);
    }

    void OnWritePutBlobsFail(const ui32 milliseconds) const {
        HistogramFailedWritePutBlobsDurationMs->Collect(milliseconds);
        WritePutBlobsCount->Sub(1);
    }

    void OnWritePutBlobsStart() const {
        WritePutBlobsCount->Add(1);
    }

    void OnWriteTxComplete(const ui32 milliseconds) const {
        HistogramWriteTxCompleteDurationMs->Collect(milliseconds);
    }

    void OnInternalCompactionInfo(const ui64 bytes, const ui32 portionsCount) const {
        InternalCompactionGranuleBytes->SetValue(bytes);
        InternalCompactionGranulePortionsCount->SetValue(portionsCount);
    }

    void OnSplitCompactionInfo(const ui64 bytes, const ui32 portionsCount) const {
        SplitCompactionGranuleBytes->SetValue(bytes);
        SplitCompactionGranulePortionsCount->SetValue(portionsCount);
    }

    void OnOverloadInsertTable(const ui64 size) const {
        OverloadInsertTableBytes->Add(size);
        OverloadInsertTableCount->Add(1);
    }

    void OnOverloadShard(const ui64 size) const {
        OverloadShardBytes->Add(size);
        OverloadShardCount->Add(1);
    }

    void SkipIndexationInputDueToSplitCompaction(const ui64 size) const {
        SkipIndexationInputDueToSplitCompactionBytes->Add(size);
        SkipIndexationInputDueToSplitCompactionCount->Add(1);
    }

    void SkipIndexationInputDueToGranuleOverload(const ui64 size) const {
        SkipIndexationInputDueToGranuleOverloadBytes->Add(size);
        SkipIndexationInputDueToGranuleOverloadCount->Add(1);
    }

    void FutureIndexationInput(const ui64 size) const {
        FutureIndexationInputBytes->Add(size);
    }

    void IndexationInput(const ui64 size) const {
        IndexationInputBytes->Add(size);
    }

    void OnStartBackground() const {
        StartBackgroundCount->Add(1);
    }

    void OnTooEarly() const {
        TooEarlyBackgroundCount->Add(1);
    }

    void OnSetupCompaction() const {
        SetupCompactionCount->Add(1);
    }

    void OnSetupIndexation() const {
        SetupIndexationCount->Add(1);
    }

    void OnSetupTtl() const {
        SetupTtlCount->Add(1);
    }

    void OnSetupCleanup() const {
        SetupCleanupCount->Add(1);
    }

    TCSCounters();
};

}
