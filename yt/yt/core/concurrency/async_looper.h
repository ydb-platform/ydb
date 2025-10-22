#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

// Class which indefenetely runs two tasks:
// 1. Async start which creates async action.
// 2. Sync finish which is called after async action is finished.
// Both are ran in Invoker_.
// Both |Start| and |Stop| are completely
// thread-safe, can be executed in any order and any number of times.
// Dropping last reference to looper at any point is also safe.
class TAsyncLooper
    : public TRefCounted
{
public:
    TAsyncLooper(
        IInvokerPtr invoker,
        TCallback<TFuture<void>()> asyncStart,
        TClosure syncFinish,
        const NLogging::TLogger& logger = NLogging::TLogger("AsyncLooper"));

    // Starts polling.
    // First loop will have cleanStart == true
    // Calling after stop will act as
    // if for the first time.
    void Start();

    // Cancels the current loop if one is present.
    void Stop();

private:
    const IInvokerPtr Invoker_;
    const TCallback<TFuture<void>()> AsyncStart_;
    const TClosure SyncFinish_;
    const NLogging::TLogger Logger;

    YT_DECLARE_SPIN_LOCK(NYT::NThreading::TSpinLock, StateLock_);
    using TGuard = TGuard<NYT::NThreading::TSpinLock>;

    enum class EState
    {
        NotRunning,
        Running,
        Restarting,
    };

    // Default transitions are (only observable ones are listed):
    // Idle -> AsyncBusy -> Idle -> Busy ->
    // AsyncBusy -> Idle -> Busy -> ...
    // NB(arkady-e1ppa): Technically, AsyncBusy
    // is redundant and can be replaced with Busy.
    // It does not produce noticeable overhead to keep it
    // and it is helpful for understanding what is going on
    // in the implementation (e.g. why it is correct).
    enum class EStage
    {
        Idle,
        AsyncBusy,
        Busy,
    };

    EState State_ = EState::NotRunning;
    EStage Stage_ = EStage::Idle;

    ui64 EpochNumber_ = 0;
    TFuture<void> Future_;

    void DoStart();

    void StartLoop(const TGuard& guard);
    void AfterStart(ui64 epochNumber, const TError& error);
    void DoStep();
    void FinishStep();
};

DEFINE_REFCOUNTED_TYPE(TAsyncLooper);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
