#pragma once
#include "common/owner.h"
#include "splitter.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NColumnShard {

class TWriteCounters: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;
    
    NMonitoring::TDynamicCounters::TCounterPtr InsertedRows;
    NMonitoring::TDynamicCounters::TCounterPtr UpdatedRows;
    NMonitoring::TDynamicCounters::TCounterPtr ;
    NMonitoring::TDynamicCounters::TCounterPtr InsertedRows;

    // TODO: remove counters below
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

public:
    TWriteCounters();

    void OnRowsUpserted(ui64 rows) const {

    }

    void OnRowsInserted(ui64 rows) const {

    }

    void OnRowsUpdated(ui64 rows) const {

    }

    void OnRowsReplaced(ui64 rows) const {

    }

    void OnRowsDeleted(ui64 rows) const {

    }
};

}
