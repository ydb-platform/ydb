#ifndef PERIODIC_EXECUTOR_BASE_H_
#error "Direct inclusion of this file is not allowed, include periodic_executor_base.h"
// For the sake of sane code completion.
#include "periodic_executor_base.h"
#endif
#undef PERIODIC_EXECUTOR_BASE_H_

namespace NYT::NConcurrency::NDetail {

////////////////////////////////////////////////////////////////////////////////

template <CInvocationTimePolicy TInvocationTimePolicy>
TPeriodicExecutorBase<TInvocationTimePolicy>::TPeriodicExecutorBase(
    IInvokerPtr invoker,
    TPeriodicCallback callback,
    TOptions options)
    : TInvocationTimePolicy(options)
    , Invoker_(std::move(invoker))
    , Callback_(std::move(callback))
{
    YT_VERIFY(Invoker_);
    YT_VERIFY(Callback_);
}

template <CInvocationTimePolicy TInvocationTimePolicy>
void TPeriodicExecutorBase<TInvocationTimePolicy>::Start()
{
    auto guard = Guard(SpinLock_);

    if (Started_) {
        return;
    }

    ExecutedPromise_ = TPromise<void>();
    IdlePromise_ = TPromise<void>();
    Started_ = true;
    if (TInvocationTimePolicy::IsEnabled()) {
        PostDelayedCallback(TInvocationTimePolicy::KickstartDeadline());
    }
}

template <CInvocationTimePolicy TInvocationTimePolicy>
void TPeriodicExecutorBase<TInvocationTimePolicy>::DoStop(TGuard<NThreading::TSpinLock>& guard)
{
    if (!Started_) {
        return;
    }

    Started_ = false;
    OutOfBandScheduled_ = false;
    TInvocationTimePolicy::Reset();

    auto executedPromise = ExecutedPromise_;
    auto executionCanceler = ExecutionCanceler_;
    TDelayedExecutor::CancelAndClear(Cookie_);

    guard.Release();

    if (executedPromise) {
        executedPromise.TrySet(MakeStoppedError());
    }

    if (executionCanceler) {
        executionCanceler(MakeStoppedError());
    }
}

template <CInvocationTimePolicy TInvocationTimePolicy>
TFuture<void> TPeriodicExecutorBase<TInvocationTimePolicy>::Stop()
{
    auto guard = Guard(SpinLock_);
    if (ExecutingCallback_) {
        InitIdlePromise();
        auto idlePromise = IdlePromise_;
        DoStop(guard);
        return idlePromise;
    } else {
        DoStop(guard);
        return VoidFuture;
    }
}

template <CInvocationTimePolicy TInvocationTimePolicy>
TError TPeriodicExecutorBase<TInvocationTimePolicy>::MakeStoppedError()
{
    return TError(NYT::EErrorCode::Canceled, "Periodic executor is stopped");
}

template <CInvocationTimePolicy TInvocationTimePolicy>
void TPeriodicExecutorBase<TInvocationTimePolicy>::InitIdlePromise()
{
    if (IdlePromise_) {
        return;
    }

    if (Started_) {
        IdlePromise_ = NewPromise<void>();
    } else {
        IdlePromise_ = MakePromise<void>(TError());
    }
}

template <CInvocationTimePolicy TInvocationTimePolicy>
void TPeriodicExecutorBase<TInvocationTimePolicy>::InitExecutedPromise()
{
    if (ExecutedPromise_) {
        return;
    }

    if (Started_) {
        ExecutedPromise_ = NewPromise<void>();
    } else {
        ExecutedPromise_ = MakePromise<void>(MakeStoppedError());
    }
}

template <CInvocationTimePolicy TInvocationTimePolicy>
void TPeriodicExecutorBase<TInvocationTimePolicy>::ScheduleOutOfBand()
{
    auto guard = Guard(SpinLock_);

    if (!Started_) {
        return;
    }

    if (TInvocationTimePolicy::IsOutOfBandProhibited()) {
        return;
    }

    if (Busy_) {
        OutOfBandScheduled_ = true;
    } else {
        guard.Release();
        PostCallback();
    }
}

template <CInvocationTimePolicy TInvocationTimePolicy>
void TPeriodicExecutorBase<TInvocationTimePolicy>::PostDelayedCallback(TInstant deadline)
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock_);
    TDelayedExecutor::CancelAndClear(Cookie_);
    Cookie_ = TDelayedExecutor::Submit(
        BIND_NO_PROPAGATE(&TThis::OnTimer, MakeWeak(this)),
        deadline,
        GetSyncInvoker());
}

template <CInvocationTimePolicy TInvocationTimePolicy>
void TPeriodicExecutorBase<TInvocationTimePolicy>::PostCallback()
{
    GuardedInvoke(
        Invoker_,
        [weakThis = MakeWeak(this)] {
            if (auto strongThis = weakThis.Lock()) {
                strongThis->RunCallback();
            }
        },
        [weakThis = MakeWeak(this)] {
            if (auto strongThis = weakThis.Lock()) {
                strongThis->OnCallbackCancelled();
            }
        });
}

template <CInvocationTimePolicy TInvocationTimePolicy>
void TPeriodicExecutorBase<TInvocationTimePolicy>::OnTimer(bool aborted)
{
    if (aborted) {
        return;
    }
    PostCallback();
}

template <CInvocationTimePolicy TInvocationTimePolicy>
void TPeriodicExecutorBase<TInvocationTimePolicy>::RunCallback()
{
    TPromise<void> executedPromise;
    {
        auto guard = Guard(SpinLock_);
        if (!Started_ || Busy_) {
            return;
        }
        Busy_ = true;
        ExecutingCallback_ = true;
        ExecutionCanceler_ = GetCurrentFiberCanceler();
        TDelayedExecutor::CancelAndClear(Cookie_);
        if (ExecutedPromise_) {
            executedPromise = ExecutedPromise_;
            ExecutedPromise_ = TPromise<void>();
        }
        if (IdlePromise_) {
            IdlePromise_ = NewPromise<void>();
        }
    }

    auto cleanup = [=, this] (bool aborted) {
        if (aborted) {
            return;
        }

        TPromise<void> idlePromise;
        {
            auto guard = Guard(SpinLock_);
            idlePromise = IdlePromise_;
            ExecutingCallback_ = false;
            ExecutionCanceler_.Reset();
        }

        if (idlePromise) {
            idlePromise.TrySet();
        }

        if (executedPromise) {
            executedPromise.TrySet();
        }

        auto guard = Guard(SpinLock_);

        YT_VERIFY(Busy_);
        Busy_ = false;

        if (!Started_) {
            return;
        }

        if (std::exchange(OutOfBandScheduled_, false) &&
            !TInvocationTimePolicy::IsOutOfBandProhibited())
        {
            guard.Release();
            PostCallback();
        } else if (TInvocationTimePolicy::IsEnabled()) {
            PostDelayedCallback(TInvocationTimePolicy::NextDeadline());
        }
    };

    try {
        DoRunCallback();
    } catch (const TFiberCanceledException&) {
        // There's very little we can do here safely;
        // in particular, we should refrain from setting promises;
        // let's forward the call to the delayed executor.
        TDelayedExecutor::Submit(
            BIND([this_ = MakeStrong(this), cleanup = std::move(cleanup)] (bool aborted) {
                cleanup(aborted);
            }),
            TDuration::Zero());
        throw;
    }

    cleanup(false);
}

template <CInvocationTimePolicy TInvocationTimePolicy>
void TPeriodicExecutorBase<TInvocationTimePolicy>::OnCallbackCancelled()
{
    auto guard = Guard(SpinLock_);

    if (!Started_) {
        return;
    }

    if (TInvocationTimePolicy::IsEnabled()) {
        PostDelayedCallback(TInvocationTimePolicy::NextDeadline());
    }
}

template <CInvocationTimePolicy TInvocationTimePolicy>
template <class... TPartialOptions>
    requires CPartialOptions<TInvocationTimePolicy, TPartialOptions...>
void TPeriodicExecutorBase<TInvocationTimePolicy>::SetOptions(TPartialOptions... options)
{
    auto guard = Guard(SpinLock_);

    // Kickstart invocations, if needed.
    //! NB: We set options after checking if we should kickstart because such a
    //! decision can be affected by our previous state.
    if (Started_ && !Busy_ && TInvocationTimePolicy::ShouldKickstart(options...)) {
        TInvocationTimePolicy::SetOptions(std::move(options)...);

        PostDelayedCallback(TInvocationTimePolicy::KickstartDeadline());
    } else {
        TInvocationTimePolicy::SetOptions(std::move(options)...);
    }
}

template <CInvocationTimePolicy TInvocationTimePolicy>
TFuture<void> TPeriodicExecutorBase<TInvocationTimePolicy>::GetExecutedEvent()
{
    auto guard = Guard(SpinLock_);
    InitExecutedPromise();
    return ExecutedPromise_.ToFuture().ToUncancelable();
}

template <CInvocationTimePolicy TInvocationTimePolicy>
void TPeriodicExecutorBase<TInvocationTimePolicy>::DoRunCallback()
{
    if constexpr (std::same_as<TCallbackResult, void>) {
        Callback_();
        TInvocationTimePolicy::ProcessResult();
    } else {
        TInvocationTimePolicy::ProcessResult(Callback_());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency::NDetail
