#pragma once

#include "public.h"

#include <yt/yt/core/actions/public.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

struct TNewTwoLevelFairShareThreadPoolOptions
{
    IPoolWeightProviderPtr PoolWeightProvider = nullptr;
    bool VerboseLogging = false;
    TDuration PollingPeriod = TDuration::MilliSeconds(10);
};

ITwoLevelFairShareThreadPoolPtr CreateNewTwoLevelFairShareThreadPool(
    int threadCount,
    const TString& threadNamePrefix,
    const TNewTwoLevelFairShareThreadPoolOptions& options = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
