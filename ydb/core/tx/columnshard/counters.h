#pragma once
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NColumnShard {

class TScanCounters {
private:
    YDB_READONLY_DEF(NMonitoring::TDynamicCounters::TCounterPtr, PortionBytes);
    YDB_READONLY_DEF(NMonitoring::TDynamicCounters::TCounterPtr, FilterBytes);
    YDB_READONLY_DEF(NMonitoring::TDynamicCounters::TCounterPtr, PostFilterBytes);
    YDB_READONLY_DEF(NMonitoring::TDynamicCounters::TCounterPtr, PostFilterPortionsCount);
    YDB_READONLY_DEF(NMonitoring::TDynamicCounters::TCounterPtr, FilterOnlyPortionsCount);
    YDB_READONLY_DEF(NMonitoring::TDynamicCounters::TCounterPtr, FilterOnlyPortionsBytes);
    YDB_READONLY_DEF(NMonitoring::TDynamicCounters::TCounterPtr, EmptyFilterPortionsCount);
    YDB_READONLY_DEF(NMonitoring::TDynamicCounters::TCounterPtr, EmptyFilterPortionsBytes);
    YDB_READONLY_DEF(NMonitoring::TDynamicCounters::TCounterPtr, FilteredRowsCount);
    YDB_READONLY_DEF(NMonitoring::TDynamicCounters::TCounterPtr, UsefulFilterBytes);
    YDB_READONLY_DEF(NMonitoring::TDynamicCounters::TCounterPtr, UsefulPostFilterBytes);
    YDB_READONLY_DEF(NMonitoring::TDynamicCounters::TCounterPtr, OriginalRowsCount);
    
public:
    TScanCounters(const TString& module = "Scan");
};


}
