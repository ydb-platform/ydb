#pragma once
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include "common/owner.h"

namespace NKikimr::NColumnShard {

class TEngineLogsCounters: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;
    NMonitoring::TDynamicCounters::TCounterPtr PortionToDropCount;
    NMonitoring::TDynamicCounters::TCounterPtr PortionToDropBytes;

    NMonitoring::TDynamicCounters::TCounterPtr PortionToEvictCount;
    NMonitoring::TDynamicCounters::TCounterPtr PortionToEvictBytes;

    NMonitoring::TDynamicCounters::TCounterPtr PortionNoTtlColumnCount;
    NMonitoring::TDynamicCounters::TCounterPtr PortionNoTtlColumnBytes;

    NMonitoring::TDynamicCounters::TCounterPtr PortionNoBorderCount;
    NMonitoring::TDynamicCounters::TCounterPtr PortionNoBorderBytes;
public:
    NMonitoring::TDynamicCounters::TCounterPtr OverloadGranules;
    NMonitoring::TDynamicCounters::TCounterPtr CompactOverloadGranulesSelection;
    NMonitoring::TDynamicCounters::TCounterPtr NoCompactGranulesSelection;
    NMonitoring::TDynamicCounters::TCounterPtr SplitCompactGranulesSelection;
    NMonitoring::TDynamicCounters::TCounterPtr InternalCompactGranulesSelection;

    void OnPortionToEvict(const ui64 size) const {
        PortionToEvictCount->Add(1);
        PortionToEvictBytes->Add(size);
    }

    void OnPortionToDrop(const ui64 size) const {
        PortionToDropCount->Add(1);
        PortionToDropBytes->Add(size);
    }

    void OnPortionNoTtlColumn(const ui64 size) const {
        PortionNoTtlColumnCount->Add(1);
        PortionNoTtlColumnBytes->Add(size);
    }

    void OnPortionNoBorder(const ui64 size) const {
        PortionNoBorderCount->Add(1);
        PortionNoBorderBytes->Add(size);
    }

    TEngineLogsCounters();
};

}
