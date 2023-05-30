#pragma once
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NColumnShard {

class TEngineLogsCounters {
private:
    ::NMonitoring::TDynamicCounterPtr SubGroup;
public:
    NMonitoring::TDynamicCounters::TCounterPtr OverloadGranules;
    NMonitoring::TDynamicCounters::TCounterPtr CompactOverloadGranulesSelection;
    NMonitoring::TDynamicCounters::TCounterPtr NoCompactGranulesSelection;
    NMonitoring::TDynamicCounters::TCounterPtr SplitCompactGranulesSelection;
    NMonitoring::TDynamicCounters::TCounterPtr InternalCompactGranulesSelection;

    TEngineLogsCounters();
};

}
