#include "scheduled_executor.h"

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

TScheduledExecutor::TScheduledExecutor(
    IInvokerPtr invoker,
    TClosure callback,
    std::optional<TDuration> interval)
    : TRecurringExecutorBase(std::move(invoker), std::move(callback))
    , Interval_(interval)
{
    YT_VERIFY(!Interval_ || Interval_ != TDuration::Zero());
}

void TScheduledExecutor::SetInterval(std::optional<TDuration> interval)
{
    YT_VERIFY(!interval || interval != TDuration::Zero());

    auto guard = Guard(SpinLock_);

    auto oldInterval = Interval_;
    Interval_ = interval;

    if (interval && !(oldInterval && oldInterval->GetValue() % oldInterval->GetValue() == 0)) {
        KickStartInvocationIfNeeded();
    }
}

void TScheduledExecutor::ScheduleFirstCallback()
{
    ScheduleCallback();
}

void TScheduledExecutor::ScheduleCallback()
{
    if (Interval_) {
        PostCallbackWithDeadline(NextDeadline());
    }
}

TError TScheduledExecutor::MakeStoppedError()
{
    return TError(NYT::EErrorCode::Canceled, "Interval executor is stopped");
}

TInstant TScheduledExecutor::NextDeadline()
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock_);

    YT_VERIFY(Interval_);

    // TInstant and TDuration are guaranteed to have same precision.
    const auto& intervalValue = Interval_->GetValue();
    const auto& nowValue = TInstant::Now().GetValue();

    return TInstant::FromValue(nowValue + (intervalValue - nowValue % intervalValue));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
