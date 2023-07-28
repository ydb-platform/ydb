#pragma once

#include "scheduler_thread.h"

#include <yt/yt/core/misc/shutdown.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TThreadPoolBase
    : public virtual TRefCounted
{
public:
    static constexpr int MaxThreadCount = 64;

    explicit TThreadPoolBase(
        TString threadNamePrefix,
        NThreading::EThreadPriority threadPriority = NThreading::EThreadPriority::Normal);

    void Configure(int threadCount);
    void Shutdown();
    void EnsureStarted();

    int GetThreadCount();

protected:
    const TString ThreadNamePrefix_;
    const NThreading::EThreadPriority ThreadPriority_;

    const TShutdownCookie ShutdownCookie_;

    std::atomic<int> ThreadCount_ = 0;
    std::atomic<bool> StartFlag_ = false;
    std::atomic<bool> ShutdownFlag_ = false;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    std::vector<TSchedulerThreadBasePtr> Threads_;

    void Resize();

    TString MakeThreadName(int index);

    virtual void DoStart();
    virtual void DoShutdown();
    virtual TClosure MakeFinalizerCallback();
    virtual void DoConfigure(int threadCount);

    virtual TSchedulerThreadBasePtr SpawnThread(int index) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
