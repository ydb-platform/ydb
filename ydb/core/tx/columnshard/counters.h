#pragma once
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NColumnShard {

class TScanCounters {
private:
    YDB_READONLY_DEF(NMonitoring::TDynamicCounters::TCounterPtr, PortionBytes);
    YDB_READONLY_DEF(NMonitoring::TDynamicCounters::TCounterPtr, FilterBytes);
    YDB_READONLY_DEF(NMonitoring::TDynamicCounters::TCounterPtr, PostFilterBytes);

    YDB_READONLY_DEF(NMonitoring::TDynamicCounters::TCounterPtr, AssembleFilterCount);

    YDB_READONLY_DEF(NMonitoring::TDynamicCounters::TCounterPtr, FilterOnlyCount);
    YDB_READONLY_DEF(NMonitoring::TDynamicCounters::TCounterPtr, FilterOnlyFetchedBytes);
    YDB_READONLY_DEF(NMonitoring::TDynamicCounters::TCounterPtr, FilterOnlyUsefulBytes);

    YDB_READONLY_DEF(NMonitoring::TDynamicCounters::TCounterPtr, EmptyFilterCount);
    YDB_READONLY_DEF(NMonitoring::TDynamicCounters::TCounterPtr, EmptyFilterFetchedBytes);

    YDB_READONLY_DEF(NMonitoring::TDynamicCounters::TCounterPtr, OriginalRowsCount);
    YDB_READONLY_DEF(NMonitoring::TDynamicCounters::TCounterPtr, FilteredRowsCount);
    YDB_READONLY_DEF(NMonitoring::TDynamicCounters::TCounterPtr, SkippedBytes);

    YDB_READONLY_DEF(NMonitoring::TDynamicCounters::TCounterPtr, TwoPhasesCount);
    YDB_READONLY_DEF(NMonitoring::TDynamicCounters::TCounterPtr, TwoPhasesFilterFetchedBytes);
    YDB_READONLY_DEF(NMonitoring::TDynamicCounters::TCounterPtr, TwoPhasesFilterUsefulBytes);
    YDB_READONLY_DEF(NMonitoring::TDynamicCounters::TCounterPtr, TwoPhasesPostFilterFetchedBytes);
    YDB_READONLY_DEF(NMonitoring::TDynamicCounters::TCounterPtr, TwoPhasesPostFilterUsefulBytes);
public:
    TScanCounters(const TString& module = "Scan");
};


}
