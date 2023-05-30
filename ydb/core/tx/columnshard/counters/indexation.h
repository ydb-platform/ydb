#pragma once
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NColumnShard {

class TIndexationCounters {
private:
    ::NMonitoring::TDynamicCounterPtr SubGroup;
public:
    NMonitoring::TDynamicCounters::TCounterPtr ReadBytes;
    NMonitoring::TDynamicCounters::TCounterPtr AnalizeCompactedPortions;
    NMonitoring::TDynamicCounters::TCounterPtr AnalizeInsertedPortions;
    NMonitoring::TDynamicCounters::TCounterPtr AnalizeCompactedBytes;
    NMonitoring::TDynamicCounters::TCounterPtr AnalizeInsertedBytes;
    NMonitoring::TDynamicCounters::TCounterPtr RepackedInsertedPortions;
    NMonitoring::TDynamicCounters::TCounterPtr RepackedInsertedPortionBytes;
    NMonitoring::TDynamicCounters::TCounterPtr SkipPortionsMoveThroughIntersection;
    NMonitoring::TDynamicCounters::TCounterPtr SkipPortionBytesMoveThroughIntersection;
    NMonitoring::TDynamicCounters::TCounterPtr RepackedCompactedPortions;
    NMonitoring::TDynamicCounters::TCounterPtr MovedPortions;
    NMonitoring::TDynamicCounters::TCounterPtr MovedPortionBytes;

    NMonitoring::TDynamicCounters::TCounterPtr TrashDataSerializationBytes;
    NMonitoring::TDynamicCounters::TCounterPtr TrashDataSerialization;
    NMonitoring::TDynamicCounters::TCounterPtr CorrectDataSerializationBytes;
    NMonitoring::TDynamicCounters::TCounterPtr CorrectDataSerialization;

    NMonitoring::THistogramPtr SplittedPortionsSize;

    TIndexationCounters(const TString& module);
};

}
