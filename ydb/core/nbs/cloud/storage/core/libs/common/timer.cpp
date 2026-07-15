#include "timer.h"

#include <util/datetime/cputimer.h>

namespace NYdb::NBS {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TWallClockTimer final: public ITimer
{
public:
    TInstant Now() override
    {
        return TInstant::Now();
    }

    void Sleep(TDuration duration) override
    {
        ::Sleep(duration);
    }
};

////////////////////////////////////////////////////////////////////////////////

TInstant InitTime = TInstant::Now();
ui64 InitCycleCount = GetCycleCount();

class TCpuCycleTimer final: public ITimer
{
    TInstant Now() override
    {
        return InitTime +
               CyclesToDurationSafe(GetCycleCount() - InitCycleCount);
    }

    void Sleep(TDuration duration) override
    {
        ::Sleep(duration);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ITimerPtr CreateWallClockTimer()
{
    return std::make_shared<TWallClockTimer>();
}

ITimerPtr CreateCpuCycleTimer()
{
    return std::make_shared<TCpuCycleTimer>();
}

}   // namespace NYdb::NBS
