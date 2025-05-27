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

    explicit TThreadPoolBase(std::string threadNamePrefix);

    void SetThreadCount(int threadCount);
    void Shutdown();
    void EnsureStarted();

    int GetThreadCount();

protected:
    const std::string ThreadNamePrefix_;

    const TShutdownCookie ShutdownCookie_;

    std::atomic<int> ThreadCount_ = 0;
    std::atomic<bool> StartFlag_ = false;
    std::atomic<bool> ShutdownFlag_ = false;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    std::vector<TSchedulerThreadPtr> Threads_;

    void Resize();

    std::string MakeThreadName(int index);

    virtual void DoStart();
    virtual void DoShutdown();
    virtual void DoSetThreadCount(int threadCount);

    virtual TSchedulerThreadPtr SpawnThread(int index) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
