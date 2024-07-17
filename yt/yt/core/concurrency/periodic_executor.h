#pragma once

#include "config.h"
#include "periodic_executor_base.h"
#include "public.h"

#include <yt/yt/core/actions/callback.h>
#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/backoff_strategy.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

class TDefaultInvocationTimePolicy
    : private TPeriodicExecutorOptions
{
public:
    using TCallbackResult = void;
    using TOptions = TPeriodicExecutorOptions;

    explicit TDefaultInvocationTimePolicy(const TOptions& options);

    void ProcessResult();

    TInstant KickstartDeadline();

    bool IsEnabled();

    bool ShouldKickstart(const TOptions& newOptions);

    void SetOptions(TOptions newOptions);

    bool ShouldKickstart(const std::optional<TDuration>& period);

    void SetOptions(std::optional<TDuration> period);

    TInstant NextDeadline();

    bool IsOutOfBandProhibited();

    void Reset();
};

} // namespace NDetail

//! Helps to perform certain actions periodically.
class TPeriodicExecutor
    : public NDetail::TPeriodicExecutorBase<NDetail::TDefaultInvocationTimePolicy>
{
public:
    //! Initializes the instance.
    /*!
     *  \note
     *  We must call #Start to activate the instance.
     *
     *  \param invoker Invoker used for wrapping actions.
     *  \param callback Callback to invoke periodically.
     *  \param options Period, splay, etc.
     */
    TPeriodicExecutor(
        IInvokerPtr invoker,
        TPeriodicCallback callback,
        NConcurrency::TPeriodicExecutorOptions options);

    TPeriodicExecutor(
        IInvokerPtr invoker,
        TPeriodicCallback callback,
        std::optional<TDuration> period = {});

    //! Changes execution period.
    void SetPeriod(std::optional<TDuration> period);

private:
    using TBase = NDetail::TPeriodicExecutorBase<NDetail::TDefaultInvocationTimePolicy>;

};

DEFINE_REFCOUNTED_TYPE(TPeriodicExecutor)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
