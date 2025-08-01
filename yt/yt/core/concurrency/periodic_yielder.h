#pragma once

#include "public.h"

#include <yt/yt/core/profiling/timing.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TPeriodicYielderGuard
    : public NProfiling::TWallTimer
    , private TContextSwitchGuard
{
public:
    bool NeedYield() const;

    //! Returns true, if we have released the thread and got back to execution.
    bool TryYield() const;

private:
    std::optional<TCpuDuration> CpuPeriod_;

    TPeriodicYielderGuard(std::optional<TDuration> period = std::nullopt);

    friend TPeriodicYielderGuard CreatePeriodicYielder(std::optional<TDuration> period);
};

TPeriodicYielderGuard CreatePeriodicYielder(std::optional<TDuration> period = std::nullopt);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
