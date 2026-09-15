#include "timer_test.h"

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

TInstant TTestTimer::Now()
{
    return TInstant::MilliSeconds(AtomicGet(Timestamp));
}

void TTestTimer::Sleep(TDuration duration)
{
    SleepDurations.push_back(duration);
    AdvanceTime(duration);
}

void TTestTimer::AdvanceTime(TDuration delay)
{
    AtomicAdd(Timestamp, delay.MilliSeconds());
}

const TVector<TDuration>& TTestTimer::GetSleepDurations() const
{
    return SleepDurations;
}

}   // namespace NYdb::NBS
