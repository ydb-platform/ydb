#pragma once

#include "public.h"

#include <yt/yt/core/profiling/timing.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////


class TPeriodicYielder
    : public NProfiling::TWallTimer
    , private TContextSwitchGuard
{
public:
    TPeriodicYielder(TDuration period = TDuration::MilliSeconds(30));

    bool NeedYield() const;

    //! Returns true, if we have released the thread and got back to execution.
    bool TryYield() const;

private:
    TCpuDuration CpuPeriod_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
