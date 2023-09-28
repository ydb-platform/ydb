#pragma once

#include "public.h"
#include "recurring_executor_base.h"

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Invokes callbacks according to a primitive schedule.
//! Given a set non-zero interval, callbacks will be executed at times, which are a multiple of this interval.
//! E.g. if the interval is 5 minutes, callbacks will be executed at 00:00, 00:05, 00:10, etc.
class TScheduledExecutor
    : public TRecurringExecutorBase
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

protected:
    void ScheduleFirstCallback() override;
    void ScheduleCallback() override;

    TError MakeStoppedError() override;

private:
    std::optional<TDuration> Interval_;

    //! Returns the next time instant which is a multiple of the configured interval.
    //! NB: If the current instant is itself a multiple of the configured interval, this method will return the next
    //! suitable instant.
    TInstant NextDeadline();
};

DEFINE_REFCOUNTED_TYPE(TScheduledExecutor)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
