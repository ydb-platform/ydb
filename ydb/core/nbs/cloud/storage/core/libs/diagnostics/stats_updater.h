#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/startable.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

struct IStats
{
    virtual ~IStats() = default;

    virtual void UpdateStats(bool updatePercentiles) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IStatsUpdater: public IStartable
{
    virtual ~IStatsUpdater() = default;
};

////////////////////////////////////////////////////////////////////////////////

IStatsUpdaterPtr CreateStatsUpdater(
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    IStatsHandlerPtr statsHandler);

}   // namespace NYdb::NBS
