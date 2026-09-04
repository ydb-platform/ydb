#pragma once

#include "public.h"

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

struct IStatsHandler
{
    virtual ~IStatsHandler() = default;

    virtual void UpdateStats(bool updateIntervalFinished) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IStatsHandlerPtr CreateStatsHandlerStub();

}   // namespace NYdb::NBS
