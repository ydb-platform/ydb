#include "periodic_yielder.h"

#include "scheduler.h"

namespace NYT::NConcurrency {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

TPeriodicYielder::TPeriodicYielder(TDuration period)
    : TContextSwitchGuard(
        [this] () noexcept { Stop(); },
        [this] () noexcept { Restart(); })
    , CpuPeriod_(DurationToCpuDuration(period))
{ }

bool TPeriodicYielder::NeedYield() const
{
    return GetElapsedCpuTime() > CpuPeriod_;
}

bool TPeriodicYielder::TryYield() const
{
    if (NeedYield()) {
        Yield();
        return true;
    }

    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
