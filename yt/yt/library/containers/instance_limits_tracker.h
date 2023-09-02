#pragma once

#include "public.h"

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NContainers {

////////////////////////////////////////////////////////////////////////////////

class TInstanceLimitsTracker
    : public TRefCounted
{
public:
    //! Raises when container limits change.
    DEFINE_SIGNAL(void(const TInstanceLimits&), LimitsUpdated);

public:
    TInstanceLimitsTracker(
        IInstancePtr instance,
        IInstancePtr root,
        IInvokerPtr invoker,
        TDuration updatePeriod);

    void Start();
    void Stop();

    NYTree::IYPathServicePtr GetOrchidService();

private:
    void DoUpdateLimits();
    void DoBuildOrchid(NYson::IYsonConsumer* consumer) const;

    TPortoResourceTrackerPtr SelfTracker_;
    TPortoResourceTrackerPtr RootTracker_;
    const IInvokerPtr Invoker_;
    const NConcurrency::TPeriodicExecutorPtr Executor_;

    std::optional<TDuration> CpuGuarantee_;
    std::optional<TInstanceLimits> InstanceLimits_;
    std::optional<i64> MemoryUsage_;
    bool Running_ = false;
};

DEFINE_REFCOUNTED_TYPE(TInstanceLimitsTracker)

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TInstanceLimits& limits, TStringBuf format);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
