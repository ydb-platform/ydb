#pragma once

#include <ydb/core/protos/counters_columnshard.pb.h>
#include <ydb/core/tablet/tablet_counters.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NColumnShard {

class TWritesMonitor: TNonCopyable {
private:
    TTabletCountersBase& Stats;
    
    static TAtomicCounter WritesInFlight;
    static TAtomicCounter WritesSizeInFlight;
    ui64 WritesInFlightLocal = 0;
    ui64 WritesSizeInFlightLocal = 0;

public:
    TWritesMonitor(TTabletCountersBase& stats)
        : Stats(stats) {
    }

    ~TWritesMonitor() {
        OnFinishWrite(WritesSizeInFlightLocal, WritesInFlightLocal, true);
    }

    void OnStartWrite(const ui64 dataSize);

    void OnFinishWrite(const ui64 dataSize, const ui32 writesCount = 1, const bool onDestroy = false);

    TString DebugString() const;

    ui64 GetWritesInFlight() const {
        return WritesInFlight.Val();
    }
    ui64 GetWritesSizeInFlight() const {
        return WritesSizeInFlight.Val();
    }

private:
    void UpdateTabletCounters() {
        Stats.Simple()[COUNTER_WRITES_IN_FLY].Set(WritesInFlightLocal);
    }
};

}
