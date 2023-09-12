#include "periodic_executor.h"
#include "scheduler.h"

#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/actions/invoker_util.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/utilex/random.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

TPeriodicExecutorOptions TPeriodicExecutorOptions::WithJitter(TDuration period)
{
    return {
        .Period = period,
        .Jitter = DefaultJitter
    };
}

////////////////////////////////////////////////////////////////////////////////

TPeriodicExecutor::TPeriodicExecutor(
    IInvokerPtr invoker,
    TClosure callback,
    std::optional<TDuration> period)
    : TPeriodicExecutor(
        std::move(invoker),
        std::move(callback),
        {.Period = period})
{ }

TPeriodicExecutor::TPeriodicExecutor(
    IInvokerPtr invoker,
    TClosure callback,
    TPeriodicExecutorOptions options)
    : Invoker_(std::move(invoker))
    , Callback_(std::move(callback))
    , Period_(options.Period)
    , Splay_(options.Splay)
    , Jitter_(options.Jitter)
{
    YT_VERIFY(Invoker_);
    YT_VERIFY(Callback_);
}

void TPeriodicExecutor::Start()
{
    auto guard = Guard(SpinLock_);

    if (Started_) {
        return;
    }

    ExecutedPromise_ = TPromise<void>();
    IdlePromise_ = TPromise<void>();
    Started_ = true;
    if (Period_) {
        PostDelayedCallback(RandomDuration(Splay_));
    }
}

void TPeriodicExecutor::DoStop(TGuard<NThreading::TSpinLock>& guard)
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

TFuture<void> TPeriodicExecutor::Stop()
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

TError TPeriodicExecutor::MakeStoppedError()
{
    return TError(NYT::EErrorCode::Canceled, "Periodic executor is stopped");
}

void TPeriodicExecutor::InitIdlePromise()
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

void TPeriodicExecutor::InitExecutedPromise()
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

void TPeriodicExecutor::ScheduleOutOfBand()
{
    auto guard = Guard(SpinLock_);
    if (!Started_)
        return;

    if (Busy_) {
        OutOfBandRequested_ = true;
    } else {
        guard.Release();
        PostCallback();
    }
}

void TPeriodicExecutor::PostDelayedCallback(TDuration delay)
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock_);
    TDelayedExecutor::CancelAndClear(Cookie_);
    Cookie_ = TDelayedExecutor::Submit(
        BIND_NO_PROPAGATE(&TPeriodicExecutor::OnTimer, MakeWeak(this)),
        delay,
        GetSyncInvoker());
}

void TPeriodicExecutor::PostCallback()
{
    auto this_ = MakeWeak(this);
    GuardedInvoke(
        Invoker_,
        BIND_NO_PROPAGATE(&TPeriodicExecutor::OnCallbackSuccess, this_),
        BIND_NO_PROPAGATE(&TPeriodicExecutor::OnCallbackInvocationFailed, this_));
}

void TPeriodicExecutor::OnTimer(bool aborted)
{
    if (aborted) {
        return;
    }
    PostCallback();
}

void TPeriodicExecutor::OnCallbackSuccess()
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
        } else if (Period_) {
            PostDelayedCallback(NextDelay());
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

void TPeriodicExecutor::OnCallbackInvocationFailed()
{
    auto guard = Guard(SpinLock_);

    if (!Started_) {
        return;
    }

    if (Period_) {
        PostDelayedCallback(NextDelay());
    }
}

void TPeriodicExecutor::SetPeriod(std::optional<TDuration> period)
{
    auto guard = Guard(SpinLock_);

    // Kick-start invocations, if needed.
    if (Started_ && period && (!Period_ || *period < *Period_) && !Busy_) {
        PostDelayedCallback(RandomDuration(Splay_));
    }

    Period_ = period;
}

TFuture<void> TPeriodicExecutor::GetExecutedEvent()
{
    auto guard = Guard(SpinLock_);
    InitExecutedPromise();
    return ExecutedPromise_.ToFuture().ToUncancelable();
}

TDuration TPeriodicExecutor::NextDelay()
{
    if (Jitter_ == 0.0) {
        return *Period_;
    } else {
        auto period = *Period_;
        period += RandomDuration(period) * Jitter_ - period * Jitter_ / 2.;
        return period;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
