#pragma once

#include "public.h"

#include <yt/yt/core/misc/shutdown.h>

#include <library/cpp/yt/threading/event_count.h>
#include <library/cpp/yt/threading/spin_lock.h>

#include <util/system/thread.h>

namespace NYT::NThreading {

////////////////////////////////////////////////////////////////////////////////

struct TThreadOptions
{
    NThreading::EThreadPriority ThreadPriority = NThreading::EThreadPriority::Normal;
    std::function<void()> ThreadInitializer;
    int ShutdownPriority = 0;
};

//! A shutdown-aware thread wrapper.
class TThread
    : public virtual TRefCounted
{
public:
    explicit TThread(
        TString threadName,
        TThreadOptions options = {});
    ~TThread();

    //! Ensures the thread is started.
    /*!
     *  Also invokes start hooks (in the caller's thread).
     *  Safe to call multiple times. Fast on fastpath.
     *  Returns true if the thread has been indeed started.
     */
    bool Start();

    //! Ensures the thread is stopped.
    /*!
     *  Safe to call multiple times.
     *  Also invokes stop hooks (in the caller's thread).
     */
    void Stop();

    bool IsStarted() const;
    bool IsStopping() const;

    TThreadId GetThreadId() const;
    TString GetThreadName() const;

protected:
    virtual void StartPrologue();
    virtual void StartEpilogue();
    virtual void StopPrologue();
    virtual void StopEpilogue();

    virtual void ThreadMain() = 0;

private:
    const TString ThreadName_;
    const TThreadOptions Options_;

    const TThreadId UniqueThreadId_;

    YT_DECLARE_SPIN_LOCK(TSpinLock, SpinLock_);
    std::atomic<bool> Started_ = false;
    std::atomic<bool> Stopping_ = false;
    TShutdownCookie ShutdownCookie_;

    NThreading::TEvent StartedEvent_;
    NThreading::TEvent StoppedEvent_;

    TThreadId ThreadId_ = InvalidThreadId;
    ::TThread UnderlyingThread_;

    void SetThreadPriority();
    void ConfigureSignalHandlerStack();

    bool StartSlow();

    bool CanWaitForThreadShutdown() const;

    static void* StaticThreadMainTrampoline(void* opaque);
    void ThreadMainTrampoline();
};

DEFINE_REFCOUNTED_TYPE(TThread)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NThreading

#define THREAD_INL_H_
#include "thread-inl.h"
#undef THREAD_INL_H_
