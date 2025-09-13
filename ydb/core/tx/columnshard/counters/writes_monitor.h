#pragma once

#include <ydb/core/protos/counters_columnshard.pb.h>
#include <ydb/core/tablet/tablet_counters.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/columnshard/overload_manager/overload_manager_service.h>

namespace NKikimr::NColumnShard {

class TWritesMonitor: TNonCopyable {
private:
    TTabletCountersBase& Stats;

    ui64 WritesInFlightLocal = 0;
    ui64 WritesSizeInFlightLocal = 0;

public:
    TWritesMonitor(TTabletCountersBase& stats)
        : Stats(stats) {
    }

    ~TWritesMonitor() {
        OnFinishWrite(WritesSizeInFlightLocal, WritesInFlightLocal, true);
    }

    NOverload::EResourcesStatus OnStartWrite(const ui64 dataSize);

    void OnFinishWrite(const ui64 dataSize, const ui32 writesCount = 1, const bool onDestroy = false);

    TString DebugString() const;

private:
    void UpdateTabletCounters() {
        Stats.Simple()[COUNTER_WRITES_IN_FLY].Set(WritesInFlightLocal);
    }
};

}
