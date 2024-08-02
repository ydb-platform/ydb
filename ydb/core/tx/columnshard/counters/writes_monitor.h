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
    class TGuard: public TNonCopyable {
        friend class TWritesMonitor;

    private:
        TWritesMonitor& Owner;

        explicit TGuard(TWritesMonitor& owner)
            : Owner(owner) {
        }

    public:
        ~TGuard() {
            Owner.UpdateCounters();
        }
    };

    TWritesMonitor(TTabletCountersBase& stats)
        : Stats(stats) {
    }

    TGuard OnStartWrite(const ui64 dataSize) {
        ++WritesInFlight;
        WritesSizeInFlight += dataSize;
        return TGuard(*this);
    }

    TGuard OnFinishWrite(const ui64 dataSize, const ui32 writesCount = 1) {
        Y_ABORT_UNLESS(WritesInFlight > 0);
        Y_ABORT_UNLESS(WritesSizeInFlight >= dataSize);
        WritesInFlight -= writesCount;
        WritesSizeInFlight -= dataSize;
        return TGuard(*this);
    }

    TString DebugString() const {
        return TStringBuilder() << "{object=write_monitor;count=" << WritesInFlight << ";size=" << WritesSizeInFlight
                                << "}";
    }

private:
    void UpdateCounters() {
        Stats.Simple()[COUNTER_WRITES_IN_FLY].Set(WritesInFlight);
    }
};

}
