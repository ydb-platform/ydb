#pragma once

#include "public.h"
#include "recurring_executor_base.h"

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

struct TPeriodicExecutorOptions
{
    static constexpr double DefaultJitter = 0.2;

    //! Interval between usual consequent invocations; if null then no invocations will be happening.
    std::optional<TDuration> Period;
    TDuration Splay;
    double Jitter = 0.0;

    //! Sets #Period and Applies set#DefaultJitter.
    static TPeriodicExecutorOptions WithJitter(TDuration period);
};

//! Helps to perform certain actions periodically.
class TPeriodicExecutor
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
     *  \param options Period, splay, etc.
     */
    TPeriodicExecutor(
        IInvokerPtr invoker,
        TClosure callback,
        TPeriodicExecutorOptions options);

    TPeriodicExecutor(
        IInvokerPtr invoker,
        TClosure callback,
        std::optional<TDuration> period = {});

    //! Changes execution period.
    void SetPeriod(std::optional<TDuration> period);

protected:
    void ScheduleFirstCallback() override;
    void ScheduleCallback() override;

    TError MakeStoppedError() override;

private:
    std::optional<TDuration> Period_;
    const TDuration Splay_;
    const double Jitter_;

    TDuration NextDelay();
};

DEFINE_REFCOUNTED_TYPE(TPeriodicExecutor)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
