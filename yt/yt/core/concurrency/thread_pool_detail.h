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

    explicit TThreadPoolBase(TString threadNamePrefix);

    void Configure(int threadCount);
    void Shutdown();
    void EnsureStarted();

    int GetThreadCount();

protected:
    const TString ThreadNamePrefix_;

    const TShutdownCookie ShutdownCookie_;

    std::atomic<int> ThreadCount_ = 0;
    std::atomic<bool> StartFlag_ = false;
    std::atomic<bool> ShutdownFlag_ = false;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    std::vector<TSchedulerThreadPtr> Threads_;

    void Resize();

    TString MakeThreadName(int index);

    virtual void DoStart();
    virtual void DoShutdown();
    virtual TClosure MakeFinalizerCallback();
    virtual void DoConfigure(int threadCount);

    virtual TSchedulerThreadPtr SpawnThread(int index) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
