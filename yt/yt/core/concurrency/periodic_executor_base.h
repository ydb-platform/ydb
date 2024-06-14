#pragma once

#include "public.h"

#include <yt/yt/core/actions/callback.h>
#include <yt/yt/core/actions/future.h>

#include <concepts>

namespace NYT::NConcurrency::NDetail {

////////////////////////////////////////////////////////////////////////////////

//! Processor must declare what return value it expects from a callback
//! and have a function to process given result.
template <class T>
concept CCallbackResultProcessor =
    requires {
        typename T::TCallbackResult;
    } && (std::is_void_v<typename T::TCallbackResult> &&
    requires (T processor) {
        processor.ProcessResult();
    }) || (!std::is_void_v<typename T::TCallbackResult> &&
    requires (T processor, typename T::TCallbackResult result) {
        processor.ProcessResult(std::move(result));
    });

//! TimePolicy regulates the workflow of the core rescheduling loop
//! of the periodic executor base:
//! 1) It must be configurable by some structure (a non-void type).
//! 2) IsEnabled() tells if rescheduling loop should continue.
//! 3) SetOptions(options) updates options of the policy under spinlock.
//! 4) ShouldKickstart(options) decides whether newer options should imply
//!    rescheduling as if we've just started the executor.
//! 5) KickstartDeadline() gives deadline for the "starting" callback scheduling.
//! 6) NextDeadline() gives deadline for normal rescheduling procedure after callback was called.
//! 7) IsOutOfBandProhibited() should return true if for some reason (we are in the retrying phase for e.g.)
//!    we must not request out of band invocation.
//! 8) Reset() resets the entire state of the policy.
template <class T>
concept CInvocationTimePolicy = CCallbackResultProcessor<T> &&
    requires (T policy, typename T::TOptions options)
{
    typename T::TOptions;

    T(options);

    { policy.IsEnabled() } -> std::same_as<bool>;

    { policy.SetOptions(options) } -> std::same_as<void>;

    { policy.ShouldKickstart(options) } -> std::same_as<bool>;
    { policy.KickstartDeadline() } -> std::same_as<TInstant>;

    { policy.NextDeadline() } -> std::same_as<TInstant>;
    { policy.IsOutOfBandProhibited() } -> std::same_as<bool>;
    { policy.Reset() } -> std::same_as<void>;
};

//! Your favourite periodic executor can optionally support some other
//! structures to be parsed into options.
//! Example:
//! TPeriodicExecutorOptions is TOption type for DefaultInvocationTimePolicy.
//! It has fields Period, Jitter, Splay.
//! DefaultInvocationTimePolicy wants to be able to change only the period
//! So it defines SetOptions(Period) and ShouldKickstart(Period).
//! After this is done, Period is a partial option for DefaultInvocationTimePolicy.
//! Concept below accounts for desire to change several options (e.g. Peiod and Splay)
//! at the same time.
template <class TPolicy, class... TOptions>
concept CPartialOptions = requires (TOptions... partialOptions, TPolicy policy)
{
    { policy.SetOptions(partialOptions...) } -> std::same_as<void>;

    { policy.ShouldKickstart(partialOptions...) } -> std::same_as<bool>;
};

////////////////////////////////////////////////////////////////////////////////

template <CInvocationTimePolicy TInvocationTimePolicy>
class TPeriodicExecutorBase
    : public TRefCounted
    , protected TInvocationTimePolicy
{
public:
    using TOptions = typename TInvocationTimePolicy::TOptions;

    //! Starts the instance.
    void Start();

    bool IsStarted() const;

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

    //! This is usually a TOptions class but can be something else
    //! if TInvocationTimePolicy has proper overloads.
    template <class... TPartialOptions>
        requires CPartialOptions<TInvocationTimePolicy, TPartialOptions...>
    void SetOptions(TPartialOptions... partialOptions);

protected:
    using TCallbackResult = typename TInvocationTimePolicy::TCallbackResult;
    using TPeriodicCallback = TCallback<TCallbackResult()>;

    TPeriodicExecutorBase(
        IInvokerPtr invoker,
        TPeriodicCallback callback,
        TOptions options);

private:
    using TThis = TPeriodicExecutorBase<TInvocationTimePolicy>;

    const IInvokerPtr Invoker_;
    const TPeriodicCallback Callback_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    bool Started_ = false;
    bool Busy_ = false;
    bool OutOfBandScheduled_ = false;
    bool ExecutingCallback_ = false;
    TCallback<void(const TError&)> ExecutionCanceler_;
    TDelayedExecutorCookie Cookie_;
    TPromise<void> IdlePromise_;
    TPromise<void> ExecutedPromise_;

    void DoStop(TGuard<NThreading::TSpinLock>& guard);

    static TError MakeStoppedError();

    void InitIdlePromise();
    void InitExecutedPromise();

    void PostDelayedCallback(TInstant deadline);

    void PostCallback();

    void OnTimer(bool aborted);
    void RunCallback();
    void OnCallbackCancelled();

    void DoRunCallback();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency::NDetail

#define PERIODIC_EXECUTOR_BASE_H_
#include "periodic_executor_base-inl.h"
#undef PERIODIC_EXECUTOR_BASE_H_
