#pragma once

#include "public.h"

#include <yt/yt/core/profiling/timing.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TPeriodicYielder
{
public:
    TPeriodicYielder() = default;

    explicit TPeriodicYielder(TDuration period);

    //! Returns true, if we have released the thread and got back to execution.
    bool TryYield();

    void SetPeriod(TDuration period);

    void SetDisabled(bool value);

private:
    NProfiling::TCpuDuration Period_;
    NProfiling::TCpuInstant LastYieldTime_ = NProfiling::GetCpuInstant();
    bool Disabled_ = true;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
