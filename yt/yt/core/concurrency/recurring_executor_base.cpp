#include "recurring_executor_base.h"
#include "scheduler.h"

#include <yt/yt/core/actions/bind.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

TRecurringExecutorBase::TRecurringExecutorBase(
    IInvokerPtr invoker,
    TClosure callback)
    : Invoker_(std::move(invoker))
    , Callback_(std::move(callback))
{
    YT_VERIFY(Invoker_);
    YT_VERIFY(Callback_);
}

void TRecurringExecutorBase::Start()
{
    auto guard = Guard(SpinLock_);

    if (Started_) {
        return;
    }

    ExecutedPromise_ = TPromise<void>();
    IdlePromise_ = TPromise<void>();
    Started_ = true;
    ScheduleFirstCallback();
}

void TRecurringExecutorBase::DoStop(TGuard<NThreading::TSpinLock>& guard)
{
    if (!Started_) {
        return;
    }

    Started_ = false;
    OutOfBandRequested_ = false;
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

TFuture<void> TRecurringExecutorBase::Stop()
{
    auto guard = Guard(SpinLock_);
    if (IsExecutingCallback()) {
        InitIdlePromise();
        auto idlePromise = IdlePromise_;
        DoStop(guard);
        return idlePromise;
    } else {
        DoStop(guard);
        return VoidFuture;
    }
}

TFuture<void> TRecurringExecutorBase::GetExecutedEvent()
{
    auto guard = Guard(SpinLock_);
    InitExecutedPromise();
    return ExecutedPromise_.ToFuture().ToUncancelable();
}

void TRecurringExecutorBase::InitIdlePromise()
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock_);

    if (IdlePromise_) {
        return;
    }

    if (Started_) {
        IdlePromise_ = NewPromise<void>();
    } else {
        IdlePromise_ = MakePromise<void>(TError());
    }
}

void TRecurringExecutorBase::InitExecutedPromise()
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock_);

    if (ExecutedPromise_) {
        return;
    }

    if (Started_) {
        ExecutedPromise_ = NewPromise<void>();
    } else {
        ExecutedPromise_ = MakePromise<void>(MakeStoppedError());
    }
}

void TRecurringExecutorBase::ScheduleOutOfBand()
{
    auto guard = Guard(SpinLock_);
    if (!Started_) {
        return;
    }

    if (Busy_) {
        OutOfBandRequested_ = true;
    } else {
        guard.Release();
        PostCallback();
    }
}

void TRecurringExecutorBase::PostCallbackWithDeadline(TInstant deadline)
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock_);

    TDelayedExecutor::CancelAndClear(Cookie_);
    Cookie_ = TDelayedExecutor::Submit(
        BIND_NO_PROPAGATE(&TRecurringExecutorBase::OnTimer, MakeWeak(this)),
        deadline,
        GetSyncInvoker());
}

void TRecurringExecutorBase::PostDelayedCallback(TDuration delay)
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock_);

    TDelayedExecutor::CancelAndClear(Cookie_);
    Cookie_ = TDelayedExecutor::Submit(
        BIND_NO_PROPAGATE(&TRecurringExecutorBase::OnTimer, MakeWeak(this)),
        delay,
        GetSyncInvoker());
}

void TRecurringExecutorBase::PostCallback()
{
    auto this_ = MakeWeak(this);
    GuardedInvoke(
        Invoker_,
        BIND_NO_PROPAGATE(&TRecurringExecutorBase::OnCallbackSuccess, this_),
        BIND_NO_PROPAGATE(&TRecurringExecutorBase::OnCallbackInvocationFailed, this_));
}

void TRecurringExecutorBase::OnTimer(bool aborted)
{
    if (aborted) {
        return;
    }
    PostCallback();
}

void TRecurringExecutorBase::OnCallbackSuccess()
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
        if (OutOfBandRequested_) {
            OutOfBandRequested_ = false;
            guard.Release();
            PostCallback();
        } else {
            ScheduleCallback();
        }
    };

    try {
        Callback_();
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

void TRecurringExecutorBase::OnCallbackInvocationFailed()
{
    auto guard = Guard(SpinLock_);

    if (!Started_) {
        return;
    }

    ScheduleCallback();
}

void TRecurringExecutorBase::KickStartInvocationIfNeeded()
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock_);

    if (Started_ && !Busy_) {
        ScheduleFirstCallback();
    }
}

bool TRecurringExecutorBase::IsExecutingCallback() const
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock_);

    return ExecutingCallback_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
