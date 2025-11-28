#pragma once

#include "public.h"

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/actions/future.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TNewHedgingManagerStatistics
{
    //! Number of primary requests registered for hedging.
    int PrimaryRequestCount;
    //! Number of requests that were actually hedged.
    int SecondaryRequestCount;
    //! Number of secondary requests put into the queue waiting for the quota to be hedged.
    int QueuedRequestCount;
    //! Maximum number of requests simultaneously present in the waiting queue.
    int MaxQueueSize;
    //! Current hedging delay in invocating of secondary request.
    TDuration HedgingDelay;

    bool operator==(const TNewHedgingManagerStatistics& other) const = default;
};

////////////////////////////////////////////////////////////////////////////////

// NB: Entities that use hedging manager shall implement this interface.
struct ISecondaryRequestGenerator
    : public virtual TRefCounted
{
    //! Hedging manager will call this method when all conditions are met to
    //! generate secondary request corresponding to this generator entity.
    virtual void GenerateSecondaryRequest() = 0;
};

DEFINE_REFCOUNTED_TYPE(ISecondaryRequestGenerator)

////////////////////////////////////////////////////////////////////////////////

struct INewHedgingManager
    : public TRefCounted
{
    //! Will register new sent primary request and to hedge it will schedule corresponding secndary request.
    //! #secondaryRequestGenerator will be called when the conditions are met, i.e.
    //! (adaptive) deadline is reached, quota is not exhausted and primary request has not finished.
    virtual void RegisterRequest(
        TFuture<void> requestFuture,
        ISecondaryRequestGeneratorPtr secondaryRequestGenerator) = 0;

    //! Will return a set of statistics gathered since last call to this method.
    virtual TNewHedgingManagerStatistics CollectStatistics() = 0;
};

DEFINE_REFCOUNTED_TYPE(INewHedgingManager)

////////////////////////////////////////////////////////////////////////////////

INewHedgingManagerPtr CreateNewAdaptiveHedgingManager(
    TAdaptiveHedgingManagerConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
