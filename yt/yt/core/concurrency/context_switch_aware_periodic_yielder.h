#pragma once

#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TContextSwitchAwarePeriodicYielder
    : public NProfiling::TWallTimer
    , private TContextSwitchGuard
{
public:
    explicit TContextSwitchAwarePeriodicYielder(TDuration period);

    void Checkpoint(const NLogging::TLogger& logger);

private:
    const TDuration YieldThreshold_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
