#pragma once

#include "public.h"

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/actions/future.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TAdaptiveHedgingManagerStatistics
{
    //! Number of primary requests registered for hedging.
    int PrimaryRequestCount = 0;
    //! Number of requests that were actually hedged.
    int SecondaryRequestCount = 0;
    //! Number of secondary requests put into the queue waiting for the hedging quota.
    int QueuedRequestCount = 0;
    //! Maximum number of requests simultaneously present in the waiting queue.
    int MaxQueueSize = 0;
    //! Current hedging delay before invocation of secondary request.
    TDuration HedgingDelay;

    bool operator==(const TAdaptiveHedgingManagerStatistics& other) const = default;
};

////////////////////////////////////////////////////////////////////////////////

struct IAdaptiveHedgingManager
    : public TRefCounted
{
    //! Registers request and schedule invocation of its corresponding secondary request.
    //! #startSecondaryRequest will be called when the conditions are met, i.e.
    //! (adaptive) deadline is reached, quota is not exhausted and primary request has not finished yet.
    virtual void RegisterRequest(
        TFuture<void> requestFuture,
        TClosure startSecondaryRequest) = 0;

    //! Returns a set of statistics accumulated since last call to this method.
    virtual TAdaptiveHedgingManagerStatistics CollectStatistics() = 0;
};

DEFINE_REFCOUNTED_TYPE(IAdaptiveHedgingManager)

////////////////////////////////////////////////////////////////////////////////

IAdaptiveHedgingManagerPtr CreateAdaptiveHedgingManager(
    TAdaptiveHedgingManagerConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
