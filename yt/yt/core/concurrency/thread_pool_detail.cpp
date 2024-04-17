#include "thread_pool_detail.h"

#include "system_invokers.h"
#include "private.h"

#include <yt/yt/core/actions/invoker_util.h>

#include <algorithm>

namespace NYT::NConcurrency {

static const auto& Logger = ConcurrencyLogger;

////////////////////////////////////////////////////////////////////////////////

TThreadPoolBase::TThreadPoolBase(TString threadNamePrefix)
    : ThreadNamePrefix_(std::move(threadNamePrefix))
    , ShutdownCookie_(RegisterShutdownCallback(
        Format("ThreadPool(%v)", ThreadNamePrefix_),
        BIND_NO_PROPAGATE(&TThreadPoolBase::Shutdown, MakeWeak(this)),
        /*priority*/ 100))
{ }

void TThreadPoolBase::Configure(int threadCount)
{
    DoConfigure(std::clamp(threadCount, 1, MaxThreadCount));
}

void TThreadPoolBase::Shutdown()
{
    if (!ShutdownFlag_.exchange(true)) {
        StartFlag_ = true;
        DoShutdown();
    }
}

void TThreadPoolBase::EnsureStarted()
{
    if (!StartFlag_.exchange(true)) {
        Resize();
        DoStart();
    }
}

TString TThreadPoolBase::MakeThreadName(int index)
{
    return Format("%v:%v", ThreadNamePrefix_, index);
}

void TThreadPoolBase::DoStart()
{
    decltype(Threads_) threads;
    {
        auto guard = Guard(SpinLock_);
        threads = Threads_;
    }

    for (const auto& thread : threads) {
        thread->Start();
    }
}

void TThreadPoolBase::DoShutdown()
{
    GetFinalizerInvoker()->Invoke(MakeFinalizerCallback());
}

TClosure TThreadPoolBase::MakeFinalizerCallback()
{
    decltype(Threads_) threads;
    {
        auto guard = Guard(SpinLock_);
        std::swap(threads, Threads_);
    }

    return BIND_NO_PROPAGATE([threads = std::move(threads)] () {
        for (const auto& thread : threads) {
            thread->Stop();
        }
    });
}

int TThreadPoolBase::GetThreadCount()
{
    auto guard = Guard(SpinLock_);
    return std::ssize(Threads_);
}

void TThreadPoolBase::DoConfigure(int threadCount)
{
    ThreadCount_.store(threadCount);
    if (StartFlag_.load()) {
        Resize();
    }
}

void TThreadPoolBase::Resize()
{
    decltype(Threads_) threadsToStart;
    decltype(Threads_) threadsToStop;

    int threadCount;
    int oldThreadCount;
    {
        auto guard = Guard(SpinLock_);
        oldThreadCount = std::ssize(Threads_);

        threadCount = ThreadCount_.load();

        while (std::ssize(Threads_) < threadCount) {
            auto thread = SpawnThread(std::ssize(Threads_));
            threadsToStart.push_back(thread);
            Threads_.push_back(thread);
        }

        while (std::ssize(Threads_) > threadCount) {
            threadsToStop.push_back(Threads_.back());
            Threads_.pop_back();
        }
    }

    YT_LOG_DEBUG("Thread pool reconfigured (ThreadNamePrefix: %v, ThreadPoolSize: %v -> %v)",
        ThreadNamePrefix_,
        oldThreadCount,
        threadCount);

    for (const auto& thread : threadsToStop) {
        thread->Stop();
    }

    DoStart();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
