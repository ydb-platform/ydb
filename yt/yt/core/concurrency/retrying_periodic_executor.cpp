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
    : TDefaultInvocationTimePolicy(options)
    , Backoff_(options)
{
    CachedBackoffDuration_.store(options.MinBackoff, std::memory_order::relaxed);
    CachedBackoffMultiplier_.store(options.BackoffJitter, std::memory_order::relaxed);
    CachedBackoffJitter_.store(options.BackoffJitter,std::memory_order::relaxed);
}

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
    return ShouldKickstart(newOptions, std::nullopt);
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
    SetOptions(newOptions, newOptions);
}

void TRetryingInvocationTimePolicy::SetOptions(
    std::optional<NConcurrency::TPeriodicExecutorOptions> periodicOptions,
    std::optional<TExponentialBackoffOptions> backoffOptions)
{
    if (periodicOptions) {
        TDefaultInvocationTimePolicy::SetOptions(*periodicOptions);
    }

    if (backoffOptions) {
        Backoff_.UpdateOptions(*backoffOptions);

        if (!IsInBackoffMode()) {
            Backoff_.Restart();
        }

        CachedBackoffDuration_.store(
            Backoff_.GetBackoff(),
            std::memory_order::relaxed);
        CachedBackoffMultiplier_.store(
            backoffOptions->BackoffMultiplier,
            std::memory_order::relaxed);
        CachedBackoffJitter_.store(
            backoffOptions->BackoffJitter,
            std::memory_order::relaxed);
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

std::tuple<TDuration, TDuration> TRetryingInvocationTimePolicy::GetBackoffInterval() const
{
    auto backoffMedian =
        CachedBackoffDuration_.load(std::memory_order::relaxed) *
        CachedBackoffMultiplier_.load(std::memory_order::relaxed);

    auto backoffJitter = CachedBackoffJitter_.load(std::memory_order::relaxed);

    return std::tuple(backoffMedian * (1 - backoffJitter), backoffMedian * (1 + backoffJitter));
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

std::tuple<TDuration, TDuration> TRetryingPeriodicExecutor::GetBackoffInterval() const
{
    return TBase::GetBackoffInterval();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
