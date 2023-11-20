#pragma once
#include "appdata.h"
#include "feature_flags.h"

#include <util/system/datetime.h>

namespace NKikimr::NCpuTime {

class TCpuTimer {
    ui64 GetCpuTime() const {
        if (UseClockGettime) {
            return ThreadCPUTime();
        } else {
            return GetCycleCountFast();
        }
    }
public:
    TCpuTimer()
        : UseClockGettime(AppData()->FeatureFlags.GetEnableClockGettimeForUserCpuAccounting())
        , StartTime(GetCpuTime())
        , Counter(nullptr)
    {}

    TCpuTimer(TDuration& counter)
        : UseClockGettime(AppData()->FeatureFlags.GetEnableClockGettimeForUserCpuAccounting())
        , StartTime(GetCpuTime())
        , Counter(&counter)
    {}

    ~TCpuTimer() {
        if (Counter)
            *Counter += GetTime();
    }

    TDuration GetTime() const {
        if (UseClockGettime) {
            return TDuration::MicroSeconds(GetCpuTime() - StartTime);
        } else {
            double time = NHPTimer::GetSeconds(GetCpuTime() - StartTime);
            return TDuration::MicroSeconds(time * 1000000.0);
        }
    }
private:
    const bool UseClockGettime;
    const ui64 StartTime;
    TDuration* Counter;
};

} // namespace NKikimr::NCpuTime
