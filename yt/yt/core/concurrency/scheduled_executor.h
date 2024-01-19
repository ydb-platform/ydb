#pragma once

#include "periodic_executor_base.h"
#include "public.h"

#include <yt/yt/core/actions/callback.h>
#include <yt/yt/core/actions/future.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

class TScheduledInvocationTimePolicy
{
public:
    using TCallbackResult = void;
    using TOptions = std::optional<TDuration>;

    explicit TScheduledInvocationTimePolicy(const TOptions& options);

    void ProcessResult();

    TInstant KickstartDeadline();

    bool IsEnabled();

    bool ShouldKickstart(const TOptions& newOptions);

    void SetOptions(TOptions newOptions);

    //! Returns the next time instant which is a multiple of the configured interval.
    //! NB: If the current instant is itself a multiple of the configured interval, this method will return the next
    //! suitable instant.
    TInstant NextDeadline();

    bool IsOutOfBandProhibited();

    void Reset();

private:
    std::optional<TDuration> Interval_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

//! Invokes callbacks according to a primitive schedule.
//! Given a set non-zero interval, callbacks will be executed at times, which are a multiple of this interval.
//! E.g. if the interval is 5 minutes, callbacks will be executed at 00:00, 00:05, 00:10, etc.
class TScheduledExecutor
    : public NDetail::TPeriodicExecutorBase<NDetail::TScheduledInvocationTimePolicy>
{
public:
    //! Initializes an instance.
    /*!
     *  \note
     *  We must call #Start to activate the instance.
     *
     *  \param invoker Invoker used for wrapping actions.
     *  \param callback Callback to invoke periodically.
     *  \param interval Determines the moments of time at which the callback will be executed.
     */
    TScheduledExecutor(
        IInvokerPtr invoker,
        TClosure callback,
        std::optional<TDuration> interval);

    void SetInterval(std::optional<TDuration> interval);

private:
    using TBase = NDetail::TPeriodicExecutorBase<NDetail::TScheduledInvocationTimePolicy>;
};

DEFINE_REFCOUNTED_TYPE(TScheduledExecutor)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
