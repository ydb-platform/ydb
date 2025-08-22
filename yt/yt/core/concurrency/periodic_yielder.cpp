#include "periodic_yielder.h"

#include "scheduler.h"

namespace NYT::NConcurrency {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

TPeriodicYielderGuard::TPeriodicYielderGuard(std::optional<TDuration> period)
    : TContextSwitchGuard(
        [this] () noexcept { Stop(); },
        [this] () noexcept { Restart(); })
    , CpuPeriod_(period ? std::optional(DurationToCpuDuration(*period)) : std::nullopt)
{ }

bool TPeriodicYielderGuard::NeedYield() const
{
    return CpuPeriod_ && *CpuPeriod_ < GetElapsedCpuTime();
}

bool TPeriodicYielderGuard::TryYield() const
{
    if (!NeedYield()) {
        return false;
    }

    if (!IsContextSwitchForbidden()) {
        return false;
    }

    Yield();
    return true;
}

TPeriodicYielderGuard CreatePeriodicYielder(std::optional<TDuration> period)
{
    return {period};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
