#include "periodic_executor.h"

#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/actions/invoker_util.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/jitter.h>

#include <yt/yt/core/utilex/random.h>

#include <algorithm>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

TDefaultInvocationTimePolicy::TDefaultInvocationTimePolicy(
    const TOptions& options)
    : TPeriodicExecutorOptions(options)
{ }

void TDefaultInvocationTimePolicy::ProcessResult()
{ }

TInstant TDefaultInvocationTimePolicy::GenerateKickstartDeadline()
{
    return LastDeadline_ = TInstant::Now() + RandomDuration(Splay);
}

bool TDefaultInvocationTimePolicy::IsEnabled()
{
    return Period.has_value();
}

bool TDefaultInvocationTimePolicy::ShouldKickstart(const TOptions& newOptions)
{
    return ShouldKickstart(newOptions.Period);
}

bool TDefaultInvocationTimePolicy::ShouldKickstart(const std::optional<TDuration>& period)
{
    return period && (!Period || *period < *Period);
}

void TDefaultInvocationTimePolicy::SetOptions(TOptions newOptions)
{
    TPeriodicExecutorOptions::operator=(newOptions);
}

void TDefaultInvocationTimePolicy::SetOptions(std::optional<TDuration> period)
{
    Period = period;
}

TInstant TDefaultInvocationTimePolicy::GenerateNextDeadline()
{
    switch (DelayMode) {
        case EPeriodicExecutorDelayMode::FromPreviousStart:
            // Keep a fixed start-to-start period; never schedule into the past.
            return LastDeadline_ = std::max(LastDeadline_ + GenerateDelay(), TInstant::Now());
        case EPeriodicExecutorDelayMode::FromPreviousEnd:
            return LastDeadline_ = TInstant::Now() + GenerateDelay();
    }
    YT_ABORT();
}

bool TDefaultInvocationTimePolicy::IsOutOfBandProhibited()
{
    return false;
}

void TDefaultInvocationTimePolicy::Reset()
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TPeriodicExecutor::TPeriodicExecutor(
    IInvokerPtr invoker,
    TPeriodicCallback callback,
    std::optional<TDuration> period)
    : TPeriodicExecutor(
        std::move(invoker),
        std::move(callback),
        {.Period = period})
{ }

TPeriodicExecutor::TPeriodicExecutor(
    IInvokerPtr invoker,
    TPeriodicCallback callback,
    NConcurrency::TPeriodicExecutorOptions options)
    : TBase(
        std::move(invoker),
        std::move(callback),
        options)
{ }

void TPeriodicExecutor::SetPeriod(std::optional<TDuration> period)
{
    TBase::SetOptions(period);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
