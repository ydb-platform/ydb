#include "retrying_periodic_executor.h"

#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/actions/invoker_util.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/jitter.h>

#include <yt/yt/core/utilex/random.h>

#include <type_traits>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

TRetryingInvocationTimePolicy::TRetryingInvocationTimePolicy(
    const TOptions& options)
    : TDefaultInvocationTimePolicy(options.PeriodicOptions)
    , Backoff_(options.BackoffOptions)
{ }

void TRetryingInvocationTimePolicy::ProcessResult(TError result)
{
    if (result.IsOK()) {
        Backoff_.Restart();
    } else {
        Backoff_.Next();
    }

    CachedBackoffDuration_.store(
        Backoff_.GetBackoff(),
        std::memory_order::relaxed);
}

bool TRetryingInvocationTimePolicy::ShouldKickstart(const TOptions& newOptions)
{
    return ShouldKickstart(newOptions.PeriodicOptions, std::nullopt);
}

bool TRetryingInvocationTimePolicy::ShouldKickstart(
    const std::optional<NConcurrency::TPeriodicExecutorOptions>& periodicOptions,
    const std::optional<TExponentialBackoffOptions>& /*backoffOptions*/)
{
    return !IsInBackoffMode() &&
        periodicOptions &&
        TDefaultInvocationTimePolicy::ShouldKickstart(*periodicOptions);
}

void TRetryingInvocationTimePolicy::SetOptions(TOptions newOptions)
{
    SetOptions(newOptions.PeriodicOptions, newOptions.BackoffOptions);
}

void TRetryingInvocationTimePolicy::SetOptions(
    std::optional<NConcurrency::TPeriodicExecutorOptions> periodicOptions,
    std::optional<TExponentialBackoffOptions> backoffOptions)
{
    if (periodicOptions) {
        TDefaultInvocationTimePolicy::SetOptions(*periodicOptions);
    }

    if (backoffOptions) {
        CachedBackoffMultiplier_.store(
            backoffOptions->BackoffMultiplier,
            std::memory_order::relaxed);

        Backoff_.UpdateOptions(*backoffOptions);
    }
}

TInstant TRetryingInvocationTimePolicy::NextDeadline()
{
    if (IsInBackoffMode()) {
        return TInstant::Now() + Backoff_.GetBackoff();
    }

    return TDefaultInvocationTimePolicy::NextDeadline();
}

bool TRetryingInvocationTimePolicy::IsOutOfBandProhibited()
{
    return IsInBackoffMode();
}

void TRetryingInvocationTimePolicy::Reset()
{
    Backoff_.Restart();
}

TDuration TRetryingInvocationTimePolicy::GetBackoffTimeEstimate() const
{
    return
        CachedBackoffDuration_.load(std::memory_order::relaxed) *
        CachedBackoffMultiplier_.load(std::memory_order::relaxed);
}

bool TRetryingInvocationTimePolicy::IsInBackoffMode() const
{
    return Backoff_.GetInvocationIndex() > 0;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TRetryingPeriodicExecutor::TRetryingPeriodicExecutor(
    IInvokerPtr invoker,
    TPeriodicCallback callback,
    TRetryingPeriodicExecutorOptions options)
    : TBase(
        std::move(invoker),
        std::move(callback),
        options)
{ }

TRetryingPeriodicExecutor::TRetryingPeriodicExecutor(
    IInvokerPtr invoker,
    TPeriodicCallback callback,
    NConcurrency::TPeriodicExecutorOptions periodicOptions,
    TExponentialBackoffOptions backoffOptions)
    : TRetryingPeriodicExecutor(
        std::move(invoker),
        std::move(callback),
        TRetryingPeriodicExecutorOptions{
            periodicOptions,
            backoffOptions,
        })
{ }

TRetryingPeriodicExecutor::TRetryingPeriodicExecutor(
    IInvokerPtr invoker,
    TPeriodicCallback callback,
    TExponentialBackoffOptions backoffOptions,
    std::optional<TDuration> period)
    : TRetryingPeriodicExecutor(
        std::move(invoker),
        std::move(callback),
        NConcurrency::TPeriodicExecutorOptions{
            .Period = period,
        },
        backoffOptions)
{ }

TDuration TRetryingPeriodicExecutor::GetBackoffTimeEstimate() const
{
    return TBase::GetBackoffTimeEstimate();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
