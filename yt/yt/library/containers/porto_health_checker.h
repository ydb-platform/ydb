#pragma once

#include "public.h"

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/error.h>

#include <atomic>

namespace NYT::NContainers {

////////////////////////////////////////////////////////////////////////////////

class TPortoHealthChecker
    : public TRefCounted
{
public:
    TPortoHealthChecker(
        TPortoExecutorDynamicConfigPtr config,
        IInvokerPtr invoker,
        NLogging::TLogger logger);

    void Start();

    void OnDynamicConfigChanged(const TPortoExecutorDynamicConfigPtr& newConfig);

    DEFINE_SIGNAL(void(), Success);

    DEFINE_SIGNAL(void(const TError&), Failed);

private:
    const TPortoExecutorDynamicConfigPtr Config_;
    const NLogging::TLogger Logger;
    const IInvokerPtr CheckInvoker_;
    const IPortoExecutorPtr Executor_;
    NConcurrency::TPeriodicExecutorPtr PeriodicExecutor_;

    void OnCheck();
};

DEFINE_REFCOUNTED_TYPE(TPortoHealthChecker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
