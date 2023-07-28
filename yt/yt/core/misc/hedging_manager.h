#pragma once

#include "public.h"

#include <yt/yt/library/profiling/sensor.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Hedging manager determines hedging policy and may be used to control request hedging.
/*!
 *  There are two policies:
 *  - Simple one with constant hedging delay.
 *  - Adaptive one with adaptive hedging delay to satisfy ratio of backup requests.
 *    This policy also restrains sending backup requests if there were too many recently.
 */
struct IHedgingManager
    : public TRefCounted
{
    //! This function is called upon start of #requestCount requests that may be hedged.
    //! Returns delay to initiate backup requests.
    virtual TDuration OnPrimaryRequestsStarted(int requestCount) = 0;

    //! This function is called if #requestCount primary requests failed to finish within the determined delay.
    //! Returns whether hedging may actually be performed according to policy-based restraints.
    virtual bool OnHedgingDelayPassed(int requestCount) = 0;
};

DEFINE_REFCOUNTED_TYPE(IHedgingManager)

////////////////////////////////////////////////////////////////////////////////

IHedgingManagerPtr CreateSimpleHedgingManager(
    TDuration hedgingDelay);

IHedgingManagerPtr CreateAdaptiveHedgingManager(
    TAdaptiveHedgingManagerConfigPtr config,
    const NProfiling::TProfiler& profiler = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
