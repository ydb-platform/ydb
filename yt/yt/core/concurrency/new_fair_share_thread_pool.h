#pragma once

#include "public.h"

#include <yt/yt/core/actions/public.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

ITwoLevelFairShareThreadPoolPtr CreateNewTwoLevelFairShareThreadPool(
    int threadCount,
    const TString& threadNamePrefix,
    IPoolWeightProviderPtr poolWeightProvider = nullptr,
    bool verboseLogging = false);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
