#pragma once

#include "public.h"

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/misc/atomic_ptr.h>

#include <library/cpp/yt/compact_containers/compact_vector.h>

#include <library/cpp/yt/threading/public.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

// Overall rpc-method congestion state.
// Can be a a measure of allowed concurrency or probability of acceptance more calls.
struct TCongestionState
{
    std::optional<int> CurrentWindow;
    std::optional<int> MaxWindow;
    double WaitingTimeoutFraction = 0;

    using TTrackersList = TCompactVector<std::string, 4>;
    TTrackersList OverloadedTrackers;
};

// Probabilistic predicate based on congestion state of the the method.
bool ShouldThrottleCall(const TCongestionState& congestionState);

////////////////////////////////////////////////////////////////////////////////

struct IOverloadController
    : public TRefCounted
{
public:
    DECLARE_INTERFACE_SIGNAL(void(), LoadAdjusted);

    virtual void Start() = 0;
    virtual TFuture<void> Stop() = 0;
    virtual void Reconfigure(TOverloadControllerConfigPtr config) = 0;

    virtual void TrackInvoker(
        TStringBuf name,
        const IInvokerPtr& invoker) = 0;
    virtual void TrackFSHThreadPool(
        TStringBuf name,
        const NConcurrency::ITwoLevelFairShareThreadPoolPtr& threadPool) = 0;

    virtual IInvoker::TWaitTimeObserver CreateGenericWaitTimeObserver(
        TStringBuf trackerType,
        std::optional<TStringBuf> id = {}) = 0;

    virtual TCongestionState GetCongestionState(TStringBuf service, TStringBuf method) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IOverloadController);

IOverloadControllerPtr CreateOverloadController(
    TOverloadControllerConfigPtr config,
    NProfiling::TProfiler profiler = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
