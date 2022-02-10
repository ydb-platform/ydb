#pragma once

#include <ydb/core/tablet/tablet_counters.h>

#include <util/system/hp_timer.h>

namespace NPersQueue {

class TCounterTimeKeeper {
public:
    TCounterTimeKeeper (NKikimr::TTabletCumulativeCounter& counter)
        : Counter(counter)
    {}
    ~TCounterTimeKeeper() {
        Counter += ui64(CpuTimer.PassedReset() * 1000000.);
    }
private:
    NKikimr::TTabletCumulativeCounter& Counter;
    THPTimer CpuTimer;
};

} // namespace NPersQueue
