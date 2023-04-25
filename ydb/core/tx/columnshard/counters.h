#pragma once
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NColumnShard {

class TScanCounters {
private:
    YDB_READONLY_DEF(NMonitoring::TDynamicCounters::TCounterPtr, PortionBytes);
    YDB_READONLY_DEF(NMonitoring::TDynamicCounters::TCounterPtr, FilterBytes);
    YDB_READONLY_DEF(NMonitoring::TDynamicCounters::TCounterPtr, PostFilterBytes);
public:
    TScanCounters(const TString& module = "Scan");
};


}
