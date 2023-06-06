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
    NMonitoring::TDynamicCounters::TCounterPtr OverloadGranuleBytes;
    NMonitoring::TDynamicCounters::TCounterPtr OverloadGranuleCount;
    NMonitoring::TDynamicCounters::TCounterPtr OverloadShardBytes;
    NMonitoring::TDynamicCounters::TCounterPtr OverloadShardCount;

    std::shared_ptr<TValueAggregationClient> InternalCompactionGranuleBytes;
    std::shared_ptr<TValueAggregationClient> InternalCompactionGranulePortionsCount;

    std::shared_ptr<TValueAggregationClient> SplitCompactionGranuleBytes;
    std::shared_ptr<TValueAggregationClient> SplitCompactionGranulePortionsCount;
public:
    void OnInternalCompactionInfo(const ui64 bytes, const ui32 portionsCount) const {
        InternalCompactionGranuleBytes->Set(bytes);
        InternalCompactionGranulePortionsCount->Set(portionsCount);
    }

    void OnSplitCompactionInfo(const ui64 bytes, const ui32 portionsCount) const {
        SplitCompactionGranuleBytes->Set(bytes);
        SplitCompactionGranulePortionsCount->Set(portionsCount);
    }

    void OnOverloadInsertTable(const ui64 size) const {
        OverloadInsertTableBytes->Add(size);
        OverloadInsertTableCount->Add(1);
    }

    void OnOverloadGranule(const ui64 size) const {
        OverloadGranuleBytes->Add(size);
        OverloadGranuleCount->Add(1);
    }

    void OnOverloadShard(const ui64 size) const {
        OverloadShardBytes->Add(size);
        OverloadShardCount->Add(1);
    }

    void SkipIndexationInputDutToSplitCompaction(const ui64 size) const {
        SkipIndexationInputDueToSplitCompactionBytes->Add(size);
        SkipIndexationInputDueToSplitCompactionCount->Add(1);
    }

    void SkipIndexationInputDutToGranuleOverload(const ui64 size) const {
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
