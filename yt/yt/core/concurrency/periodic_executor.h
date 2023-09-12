#pragma once

#include "public.h"
#include "delayed_executor.h"

#include <yt/yt/core/actions/callback.h>
#include <yt/yt/core/actions/future.h>

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
    : public TRefCounted
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

    //! Starts the instance.
    //! The first invocation happens with a random delay within splay time.
    void Start();

    //! Stops the instance, cancels all subsequent invocations.
    //! Returns a future that becomes set when all outstanding callback
    //! invocations are finished and no more invocations are expected to happen.
    TFuture<void> Stop();

    //! Requests an immediate invocation.
    void ScheduleOutOfBand();

    //! Changes execution period.
    void SetPeriod(std::optional<TDuration> period);

    //! Returns the future that become set when
    //! at least one action be fully executed from the moment of method call.
    //! Cancellation of the returned future will not affect the action
    //! or other futures returned by this method.
    TFuture<void> GetExecutedEvent();

private:
    const IInvokerPtr Invoker_;
    const TClosure Callback_;
    std::optional<TDuration> Period_;
    const TDuration Splay_;
    const double Jitter_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    bool Started_ = false;
    bool Busy_ = false;
    bool OutOfBandRequested_ = false;
    bool ExecutingCallback_ = false;
    TCallback<void(const TError&)> ExecutionCanceler_;
    TDelayedExecutorCookie Cookie_;
    TPromise<void> IdlePromise_;
    TPromise<void> ExecutedPromise_;

    void DoStop(TGuard<NThreading::TSpinLock>& guard);

    static TError MakeStoppedError();

    void InitIdlePromise();
    void InitExecutedPromise();

    void PostDelayedCallback(TDuration delay);
    void PostCallback();

    void OnTimer(bool aborted);
    void OnCallbackSuccess();
    void OnCallbackInvocationFailed();

    TDuration NextDelay();
};

DEFINE_REFCOUNTED_TYPE(TPeriodicExecutor)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
