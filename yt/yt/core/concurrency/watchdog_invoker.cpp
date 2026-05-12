#include "watchdog_invoker.h"

#include <yt/yt/core/actions/current_invoker.h>
#include <yt/yt/core/actions/invoker_detail.h>
#include <yt/yt/core/actions/callback.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NConcurrency {

using namespace NLogging;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

class TWatchdogInvoker
    : public TInvokerWrapper<false>
{
public:
    TWatchdogInvoker(
        IInvokerPtr invoker,
        const NLogging::TLogger& logger,
        TDuration threshold)
        : TInvokerWrapper(std::move(invoker))
        , Logger(logger)
        , Threshold_(DurationToCpuDuration(threshold))
    { }

    using TInvokerWrapper::Invoke;

    void Invoke(TClosure callback) override
    {
        UnderlyingInvoker_->Invoke(BIND_NO_PROPAGATE(
            &TWatchdogInvoker::RunCallback,
            MakeStrong(this),
            Passed(std::move(callback))));
    }

private:
    const NLogging::TLogger Logger;
    const TCpuDuration Threshold_;

    void RunCallback(TClosure callback)
    {
        TCurrentInvokerGuard currentInvokerGuard(this);
        TFiberSliceTimer fiberSliceTimer(Threshold_, [&, this] (TCpuDuration execution) {
            YT_LOG_WARNING("Callback executed for too long without interruptions (Callback: %v, Execution: %v)",
                callback.GetHandle(),
                CpuDurationToDuration(execution));
        });
        callback();
    }
};

IInvokerPtr CreateWatchdogInvoker(
    IInvokerPtr underlyingInvoker,
    const NLogging::TLogger& logger,
    TDuration threshold)
{
    return New<TWatchdogInvoker>(std::move(underlyingInvoker), logger, threshold);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
