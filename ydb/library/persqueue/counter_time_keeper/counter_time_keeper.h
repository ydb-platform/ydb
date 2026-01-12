#pragma once

#include <ydb/core/tablet/tablet_counters.h>

#include <util/system/hp_timer.h>

namespace NPersQueue {

template<typename TCounter = NKikimr::TTabletCumulativeCounter>
class TCounterTimeKeeper {
public:
    TCounterTimeKeeper (TCounter& counter)
        : Counter(counter)
    {}
    ~TCounterTimeKeeper() {
        Counter += ui64(CpuTimer.PassedReset() * 1000000.);
    }
private:
    TCounter& Counter;
    THPTimer CpuTimer;
};

} // namespace NPersQueue
