#include "periodic_executor.h"

#include <yt/yt/core/utilex/random.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

TPeriodicExecutorOptions TPeriodicExecutorOptions::WithJitter(TDuration period)
{
    return {
        .Period = period,
        .Jitter = DefaultJitter
    };
}

////////////////////////////////////////////////////////////////////////////////

TPeriodicExecutor::TPeriodicExecutor(
    IInvokerPtr invoker,
    TClosure callback,
    std::optional<TDuration> period)
    : TPeriodicExecutor(
        std::move(invoker),
        std::move(callback),
        {.Period = period})
{ }

TPeriodicExecutor::TPeriodicExecutor(
    IInvokerPtr invoker,
    TClosure callback,
    TPeriodicExecutorOptions options)
    : TRecurringExecutorBase(std::move(invoker), std::move(callback))
    , Period_(options.Period)
    , Splay_(options.Splay)
    , Jitter_(options.Jitter)
{ }

void TPeriodicExecutor::SetPeriod(std::optional<TDuration> period)
{
    auto guard = Guard(SpinLock_);
    auto oldPeriod = Period_;
    Period_ = period;

    if (period && (!oldPeriod || *period < *oldPeriod)) {
        KickStartInvocationIfNeeded();
    }
}

TDuration TPeriodicExecutor::NextDelay()
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock_);

    if (Jitter_ == 0.0) {
        return *Period_;
    } else {
        auto period = *Period_;
        period += RandomDuration(period) * Jitter_ - period * Jitter_ / 2.;
        return period;
    }
}

void TPeriodicExecutor::ScheduleFirstCallback()
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock_);

    if (Period_) {
        PostDelayedCallback(RandomDuration(Splay_));
    }
}

void TPeriodicExecutor::ScheduleCallback()
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock_);

    if (Period_) {
        PostDelayedCallback(NextDelay());
    }
}

TError TPeriodicExecutor::MakeStoppedError()
{
    return TError(NYT::EErrorCode::Canceled, "Periodic executor is stopped");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
