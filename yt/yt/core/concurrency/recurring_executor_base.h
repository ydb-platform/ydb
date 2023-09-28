#pragma once

#include "public.h"
#include "delayed_executor.h"

#include <yt/yt/core/actions/callback.h>
#include <yt/yt/core/actions/future.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Abstract base class providing possibility to perform actions with certain schedule.
class TRecurringExecutorBase
    : public TRefCounted
{
public:
    //! Initializes an instance.
    /*!
     *  \note
     *  We must call #Start to activate the instance.
     *
     *  \param invoker Invoker used for wrapping actions.
     *  \param callback Callback to invoke according to the schedule.
     */
    TRecurringExecutorBase(
        IInvokerPtr invoker,
        TClosure callback);

    //! Starts the instance.
    //! The first invocation happens with a delay defined by the class heir.
    void Start();

    //! Stops the instance, cancels all subsequent invocations.
    //! Returns a future that becomes set when all outstanding callback
    //! invocations are finished and no more invocations are expected to happen.
    TFuture<void> Stop();

    //! Requests an immediate invocation.
    void ScheduleOutOfBand();

    //! Returns the future that become set when
    //! at least one action be fully executed from the moment of method call.
    //! Cancellation of the returned future will not affect the action
    //! or other futures returned by this method.
    TFuture<void> GetExecutedEvent();

private:
    const IInvokerPtr Invoker_;
    const TClosure Callback_;

    bool Started_ = false;
    bool Busy_ = false;
    bool OutOfBandRequested_ = false;
    bool ExecutingCallback_ = false;
    TCallback<void(const TError&)> ExecutionCanceler_;
    TDelayedExecutorCookie Cookie_;
    TPromise<void> IdlePromise_;
    TPromise<void> ExecutedPromise_;

    void InitIdlePromise();
    void InitExecutedPromise();

    void OnTimer(bool aborted);
    void OnCallbackSuccess();
    void OnCallbackInvocationFailed();

    void DoStop(TGuard<NThreading::TSpinLock>& guard);

protected:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);

    void PostCallbackWithDeadline(TInstant deadline);
    void PostDelayedCallback(TDuration delay);
    void PostCallback();

    void KickStartInvocationIfNeeded();

    bool IsExecutingCallback() const;

    virtual void ScheduleFirstCallback() = 0;
    virtual void ScheduleCallback() = 0;

    virtual TError MakeStoppedError() = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

