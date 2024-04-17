#pragma once
#include "common/owner.h"
#include "splitter.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NColumnShard {

class TIndexationCounters: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;
    NMonitoring::THistogramPtr HistogramCompactionInputBytes;
public:
    NMonitoring::TDynamicCounters::TCounterPtr CompactionInputBytes;

    NMonitoring::TDynamicCounters::TCounterPtr ReadErrors;
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

    std::shared_ptr<TSplitterCounters> SplitterCounters;

    NMonitoring::THistogramPtr SplittedPortionLargestColumnSize;
    NMonitoring::THistogramPtr SimpleSplitPortionLargestColumnSize;
    NMonitoring::THistogramPtr SplittedPortionColumnSize;
    NMonitoring::TDynamicCounters::TCounterPtr TooSmallBlob;
    NMonitoring::TDynamicCounters::TCounterPtr TooSmallBlobFinish;
    NMonitoring::TDynamicCounters::TCounterPtr TooSmallBlobStart;

    NMonitoring::THistogramPtr CompactionDuration;
    NMonitoring::TDynamicCounters::TCounterPtr CompactionExceptions;
    NMonitoring::TDynamicCounters::TCounterPtr CompactionFails;

    TIndexationCounters(const TString& module);

    void CompactionInputSize(const ui64 size) const {
        HistogramCompactionInputBytes->Collect(size);
        CompactionInputBytes->Add(size);
    }
};

}
