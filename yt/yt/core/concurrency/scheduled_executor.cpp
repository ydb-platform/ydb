#include "scheduled_executor.h"
#include "scheduler.h"

#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/actions/invoker_util.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/jitter.h>

#include <yt/yt/core/utilex/random.h>

#include <type_traits>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

TScheduledInvocationTimePolicy::TScheduledInvocationTimePolicy(
    const TOptions& options)
    : Interval_(options)
{
    YT_VERIFY(!Interval_ || Interval_ != TDuration::Zero());
}

void TScheduledInvocationTimePolicy::ProcessResult()
{ }

TInstant TScheduledInvocationTimePolicy::KickstartDeadline()
{
    return NextDeadline();
}

bool TScheduledInvocationTimePolicy::IsEnabled()
{
    return static_cast<bool>(Interval_);
}

bool TScheduledInvocationTimePolicy::ShouldKickstart(const TOptions&)
{
    return IsEnabled();
}

void TScheduledInvocationTimePolicy::SetOptions(TOptions interval)
{
    YT_VERIFY(!interval || interval != TDuration::Zero());

    Interval_ = interval;
}

//! Returns the next time instant which is a multiple of the configured interval.
//! NB: If the current instant is itself a multiple of the configured interval, this method will return the next
//! suitable instant.
TInstant TScheduledInvocationTimePolicy::NextDeadline()
{
    YT_VERIFY(Interval_);

    // TInstant and TDuration are guaranteed to have same precision.
    auto intervalValue = Interval_->GetValue();
    auto nowValue = TInstant::Now().GetValue();

    return TInstant::FromValue(nowValue + (intervalValue - nowValue % intervalValue));
}

bool TScheduledInvocationTimePolicy::IsOutOfBandProhibited()
{
    return false;
}

void TScheduledInvocationTimePolicy::Reset()
{
    // No-Op
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TScheduledExecutor::TScheduledExecutor(
    IInvokerPtr invoker,
    TClosure callback,
    std::optional<TDuration> interval)
    : TBase(
        std::move(invoker),
        std::move(callback),
        interval)
{ }

void TScheduledExecutor::SetInterval(std::optional<TDuration> interval)
{
    SetOptions(interval);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
