#pragma once

#include <ydb/core/protos/counters_columnshard.pb.h>
#include <ydb/core/tablet/tablet_counters.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NColumnShard {

class TWritesMonitor {
private:
    TTabletCountersBase& Stats;
    
    YDB_READONLY(ui64, WritesInFlight, 0);
    YDB_READONLY(ui64, WritesSizeInFlight, 0);

public:
    TWritesMonitor(TTabletCountersBase& stats)
        : Stats(stats) {
    }

    void OnStartWrite(const ui64 dataSize) {
        ++WritesInFlight;
        WritesSizeInFlight += dataSize;
        UpdateTabletCounters();
    }

    void OnFinishWrite(const ui64 dataSize, const ui32 writesCount = 1) {
        Y_ABORT_UNLESS(WritesInFlight > 0);
        Y_ABORT_UNLESS(WritesSizeInFlight >= dataSize);
        WritesInFlight -= writesCount;
        WritesSizeInFlight -= dataSize;
        UpdateTabletCounters();
    }

    TString DebugString() const {
        return TStringBuilder() << "{object=write_monitor;count=" << WritesInFlight << ";size=" << WritesSizeInFlight
                                << "}";
    }

private:
    void UpdateTabletCounters() {
        Stats.Simple()[COUNTER_WRITES_IN_FLY].Set(WritesInFlight);
    }
};

}
