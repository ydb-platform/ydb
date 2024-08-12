#pragma once

#include "periodic_executor.h"
#include "public.h"

#include <yt/yt/core/actions/callback.h>
#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/config.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

class TRetryingInvocationTimePolicy
    : private TDefaultInvocationTimePolicy
{
public:
    using TCallbackResult = TError;
    using TOptions = TRetryingPeriodicExecutorOptions;

    explicit TRetryingInvocationTimePolicy(const TOptions& options);

    void ProcessResult(TCallbackResult result);

    using TDefaultInvocationTimePolicy::KickstartDeadline;

    using TDefaultInvocationTimePolicy::IsEnabled;

    bool ShouldKickstart(const TOptions& newOptions);

    void SetOptions(TOptions newOptions);

    bool ShouldKickstart(
        const std::optional<NConcurrency::TPeriodicExecutorOptions>& periodicOptions,
        const std::optional<TExponentialBackoffOptions>& backofOptions);

    void SetOptions(
        std::optional<NConcurrency::TPeriodicExecutorOptions> periodicOptions,
        std::optional<TExponentialBackoffOptions> backofOptions);

    TInstant NextDeadline();

    bool IsOutOfBandProhibited();

    void Reset();

    std::tuple<TDuration, TDuration> GetBackoffInterval() const;

private:
    //! Used for backoff time estimation
    std::atomic<TDuration> CachedBackoffDuration_;
    std::atomic<double> CachedBackoffMultiplier_;
    std::atomic<double> CachedBackoffJitter_;

    TBackoffStrategy Backoff_;

    bool IsInBackoffMode() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

// Periodically executes callback which can fail using retries. Specifics:
// Fallible callback is modelled as TCallback<TError()>
// Any non-OK error is considered a failure.
// Retries are made with exponential backoff; see yt/yt/core/misc/backoff_strategy.h .
class TRetryingPeriodicExecutor
    : public NDetail::TPeriodicExecutorBase<NDetail::TRetryingInvocationTimePolicy>
{
public:
    //! Initializes the instance.
    /*!
     *  \note
     *  We must call #Start to activate the instance.
     *
     *  \param invoker Invoker used for wrapping actions.
     *  \param callback Callback<TError()> to invoke periodically.
     *  \param options Period, splay, etc. and backoff options
     */
    TRetryingPeriodicExecutor(
        IInvokerPtr invoker,
        TPeriodicCallback callback,
        TRetryingPeriodicExecutorOptions options);

    TRetryingPeriodicExecutor(
        IInvokerPtr invoker,
        TPeriodicCallback callback,
        NConcurrency::TPeriodicExecutorOptions periodicOptions,
        TExponentialBackoffOptions backoffOptions);

    TRetryingPeriodicExecutor(
        IInvokerPtr invoker,
        TPeriodicCallback callback,
        TExponentialBackoffOptions backoffOptions,
        std::optional<TDuration> period = {});

    std::tuple<TDuration, TDuration> GetBackoffInterval() const;

private:
    using TBase = NDetail::TPeriodicExecutorBase<NDetail::TRetryingInvocationTimePolicy>;
};

DEFINE_REFCOUNTED_TYPE(TRetryingPeriodicExecutor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
