#pragma once
#include "error_collector.h"
#include "splitter.h"
#include "sub_columns.h"

#include <ydb/library/signals/owner.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NColumnShard {

class TIndexationCounters: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;
    NMonitoring::THistogramPtr HistogramCompactionInputBytes;

    NMonitoring::THistogramPtr HistogramCompactionCorrectRawBytes;
    NMonitoring::THistogramPtr HistogramCompactionHugeRawBytes;
    NMonitoring::TDynamicCounters::TCounterPtr CompactionHugePartsCount;

public:
    const std::shared_ptr<NKikimr::NColumnShard::TErrorCollector> TieringErrors = std::make_shared<NKikimr::NColumnShard::TErrorCollector>();
    std::shared_ptr<TSubColumnCounters> SubColumnCounters;
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

    void OnCompactionCorrectMemory(const ui64 memorySize) const {
        HistogramCompactionCorrectRawBytes->Collect(memorySize);
    }

    void OnCompactionHugeMemory(const ui64 memorySize, const ui32 partsCount) const {
        HistogramCompactionHugeRawBytes->Collect(memorySize);
        CompactionHugePartsCount->Add(partsCount);
    }
};

}   // namespace NKikimr::NColumnShard
