#include "thread_pool.h"
#include "poller.h"
#include "thread_pool_poller.h"
#include "private.h"
#include "profiling_helpers.h"
#include "scheduler_thread.h"

#include <yt/yt/core/misc/proc.h>
#include <yt/yt/core/misc/mpsc_stack.h>
#include <yt/yt/core/misc/ref_tracked.h>

#include <yt/yt/core/profiling/tscp.h>

#include <library/cpp/yt/threading/notification_handle.h>

#include <util/system/thread.h>

#include <util/thread/lfqueue.h>

#include <util/network/pollerimpl.h>

#include <array>

namespace NYT::NConcurrency {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto PollerThreadQuantum = TDuration::MilliSeconds(100);
static constexpr int MaxEventsPerPoll = 1024;

////////////////////////////////////////////////////////////////////////////////

class TThreadPoolPoller;

namespace {

struct TPollableCookie
    : public TRefCounted
{
    explicit TPollableCookie(TThreadPoolPoller* pollerThread)
        : PollerThread(pollerThread)
    { }

    static TPollableCookie* FromPollable(IPollable* pollable)
    {
        return static_cast<TPollableCookie*>(pollable->GetCookie());
    }

    TThreadPoolPoller* const PollerThread = nullptr;

    // Active event count is equal to 2 * (active events) + (1 for unregister flag).
    std::atomic<int> ActiveEventCount = 1;
    const TPromise<void> UnregisterPromise = NewPromise<void>();
};

EContPoll ToImplControl(EPollControl control)
{
    int implControl = CONT_POLL_ONE_SHOT;
    if (Any(control & EPollControl::EdgeTriggered)) {
        implControl = CONT_POLL_EDGE_TRIGGERED;
    }
    if (Any(control & EPollControl::BacklogEmpty)) {
        implControl |= CONT_POLL_BACKLOG_EMPTY;
    }
    if (Any(control & EPollControl::Read)) {
        implControl |= CONT_POLL_READ;
    }
    if (Any(control & EPollControl::Write)) {
        implControl |= CONT_POLL_WRITE;
    }
    if (Any(control & EPollControl::ReadHup)) {
        implControl |= CONT_POLL_RDHUP;
    }
    return EContPoll(implControl);
}

EPollControl FromImplControl(int implControl)
{
    auto control = EPollControl::None;
    if (implControl & CONT_POLL_READ) {
        control |= EPollControl::Read;
    }
    if (implControl & CONT_POLL_WRITE) {
        control |= EPollControl::Write;
    }
    if (implControl & CONT_POLL_RDHUP) {
        control |= EPollControl::ReadHup;
    }
    return control;
}

bool TryAcquireEventCount(IPollable* pollable)
{
    auto* cookie = TPollableCookie::FromPollable(pollable);
    YT_VERIFY(cookie);
    YT_VERIFY(cookie->GetRefCount() > 0);

    auto oldEventCount = cookie->ActiveEventCount.fetch_add(2);
    if (oldEventCount & 1) {
        return true;
    }

    cookie->ActiveEventCount.fetch_sub(2);
    return false;
}

EThreadPriority PollablePriorityToThreadPriority(EPollablePriority priority)
{
    switch (priority) {
        case EPollablePriority::RealTime:
            return EThreadPriority::RealTime;

        default:
            return EThreadPriority::Normal;
    }
}

TString PollablePriorityToPollerThreadNameSuffix(EPollablePriority priority)
{
    switch (priority) {
        case EPollablePriority::RealTime:
            return "RT";

        default:
            return "";
    }
}

} // namespace

class TThreadPoolPoller
    : public IThreadPoolPoller
    , public NThreading::TThread
{
public:
    TThreadPoolPoller(int threadCount, const TString& threadNamePrefix, const TDuration pollingPeriod)
        : TThread(Format("%v:%v", threadNamePrefix, "Poll"))
        , Logger(ConcurrencyLogger.WithTag("ThreadNamePrefix: %v", threadNamePrefix))
    {
        PollerImpl_.Set(nullptr, WakeupHandle_.GetFD(), CONT_POLL_EDGE_TRIGGERED | CONT_POLL_READ);

        for (auto priority : TEnumTraits<EPollablePriority>::GetDomainValues()) {
            HandlerThreadPool_[priority] = CreateThreadPool(
                threadCount,
                threadNamePrefix + PollablePriorityToPollerThreadNameSuffix(priority),
                PollablePriorityToThreadPriority(priority),
                pollingPeriod);
            HandlerInvoker_[priority] = HandlerThreadPool_[priority]->GetInvoker();
        }
    }

    void Reconfigure(int threadCount) override
    {
        for (auto priority : TEnumTraits<EPollablePriority>::GetDomainValues()) {
            HandlerThreadPool_[priority]->Configure(threadCount);
        }
    }

    // TODO(lukyan): Remove TryRegister and Unregister. Do it in Arm/Unarm.
    bool TryRegister(const IPollablePtr& pollable) override
    {
        if (IsStopping()) {
            return false;
        }

        pollable->SetCookie(New<TPollableCookie>(this));
        RegisterQueue_.Enqueue(pollable);

        YT_LOG_DEBUG("Pollable registered (%v)",
            pollable->GetLoggingTag());

        return true;
    }

    // TODO(lukyan): Method OnShutdown in the interface and returned future are redundant.
    // Shutdown can be done by subscribing returned future or some promise can be set inside OnShutdown.
    TFuture<void> Unregister(const IPollablePtr& pollable) override
    {
        auto* cookie = TPollableCookie::FromPollable(pollable.Get());

        if (!cookie) {
            // Pollable was not registered.
            return VoidFuture;
        }

        DoUnregister(pollable);
        return cookie->UnregisterPromise.ToFuture();
    }

    void Arm(TFileDescriptor fd, const IPollablePtr& pollable, EPollControl control) override
    {
        YT_LOG_TRACE("Arming poller (FD: %v, Control: %v, %v)",
            fd,
            control,
            pollable->GetLoggingTag());
        PollerImpl_.Set(pollable.Get(), fd, ToImplControl(control));
    }

    void Unarm(TFileDescriptor fd, const IPollablePtr&) override
    {
        YT_LOG_TRACE("Unarming poller (FD: %v)",
            fd);
        PollerImpl_.Remove(fd);
    }

    void Retry(const IPollablePtr& pollable, bool /*wakeup*/) override
    {
        if (TryAcquireEventCount(pollable.Get())) {
            HandlerInvoker_[pollable->GetPriority()]->Invoke(BIND(TRunEventGuard(pollable.Get(), EPollControl::Retry)));
        }
    }

    IInvokerPtr GetInvoker() const override
    {
        return HandlerInvoker_[EPollablePriority::Normal];
    }

    void Shutdown() override
    {
        TThread::Stop();
    }

private:
    class TRunEventGuard
    {
    public:
        TRunEventGuard(IPollable* pollable, EPollControl control)
            : Pollable_(pollable)
            , Control_(control)
        { }

        explicit TRunEventGuard(TRunEventGuard&& other)
            : Pollable_(std::move(other.Pollable_))
            , Control_(std::move(other.Control_))
        {
            other.Pollable_ = nullptr;
        }

        TRunEventGuard(const TRunEventGuard&) = delete;

        TRunEventGuard& operator=(const TRunEventGuard&) = delete;
        TRunEventGuard& operator=(TRunEventGuard&&) = delete;

        ~TRunEventGuard()
        {
            if (Pollable_) {
                // This is unlikely but might happen on thread pool termination.
                GetFinalizerInvoker()->Invoke(BIND(&Destroy, Unretained(Pollable_)));
            }
        }

        void operator()()
        {
            Pollable_->OnEvent(Control_);
            Destroy(Pollable_);
            Pollable_ = nullptr;
        }

    private:
        IPollable* Pollable_;
        EPollControl Control_;

        static void Destroy(IPollable* pollable)
        {
            auto* cookie = TPollableCookie::FromPollable(pollable);
            YT_VERIFY(cookie);
            auto activeEventCount = cookie->ActiveEventCount.fetch_sub(2) - 2;
            if (activeEventCount == 0) {
                pollable->OnShutdown();
                cookie->UnregisterPromise.Set();
                auto pollerThread = MakeStrong(cookie->PollerThread);
                pollerThread->UnregisterQueue_.Enqueue(pollable);
                pollerThread->WakeupHandle_.Raise();
            }
        }
    };

    const NLogging::TLogger Logger;

    TEnumIndexedVector<EPollablePriority, IThreadPoolPtr> HandlerThreadPool_;
    TEnumIndexedVector<EPollablePriority, IInvokerPtr> HandlerInvoker_;

    // Only makes sense for "select" backend.
    struct TMutexLocking
    {
        using TMyMutex = TMutex;
    };

    using TPollerImpl = ::TPollerImpl<TMutexLocking>;
    TPollerImpl PollerImpl_;

    TNotificationHandle WakeupHandle_;
    TMpscStack<IPollablePtr> RegisterQueue_;
    TMpscStack<IPollablePtr> UnregisterQueue_;
    THashSet<IPollablePtr> Pollables_;

    std::array<TPollerImpl::TEvent, MaxEventsPerPoll> PooledImplEvents_;

    TEnumIndexedVector<EPollablePriority, std::vector<TClosure>> Callbacks_;

    bool DoUnregister(const IPollablePtr& pollable)
    {
        YT_LOG_DEBUG("Requesting pollable unregistration (%v)",
            pollable->GetLoggingTag());

        auto* cookie = TPollableCookie::FromPollable(pollable.Get());
        YT_VERIFY(cookie);
        auto activeEventCount = cookie->ActiveEventCount.load();

        while (true) {
            // Otherwise pollable has been already unregistered.
            if (!(activeEventCount & 1)) {
                YT_LOG_DEBUG("Pollable is already unregistered (%v)",
                    pollable->GetLoggingTag());
                return false;
            }

            if (cookie->ActiveEventCount.compare_exchange_weak(activeEventCount, activeEventCount & ~1)) {
                // Poller guarantees that OnShutdown is never executed concurrently with OnEvent().
                // Otherwise it will be removed in TRunEventGuard.
                if (activeEventCount == 1) {
                    pollable->OnShutdown();
                    cookie->UnregisterPromise.Set();
                    cookie->PollerThread->UnregisterQueue_.Enqueue(pollable);
                    cookie->PollerThread->WakeupHandle_.Raise();
                }
                return true;
            }
        }

        return false;
    }

    void HandleEvents()
    {
        int eventCount = PollerImpl_.Wait(PooledImplEvents_.data(), PooledImplEvents_.size(), PollerThreadQuantum.MicroSeconds());

        for (int index = 0; index < eventCount; ++index) {
            const auto& event = PooledImplEvents_[index];
            auto control = FromImplControl(PollerImpl_.ExtractFilter(&event));
            auto* pollable = static_cast<IPollable*>(PollerImpl_.ExtractEvent(&event));

            if (!pollable) {
                WakeupHandle_.Clear();
                continue;
            }

            YT_LOG_TRACE("Got pollable event (Pollable: %v, Control: %v)",
                pollable->GetLoggingTag(),
                control);

            YT_VERIFY(pollable->GetRefCount() > 0);

            // Can safely dereference pollable because even unregistered pollables are hold in Pollables_.
            if (TryAcquireEventCount(pollable)) {
                auto priority = pollable->GetPriority();
                Callbacks_[priority].push_back(BIND(TRunEventGuard(pollable, control)));
            }
        }

        for (auto priority : TEnumTraits<EPollablePriority>::GetDomainValues()) {
            HandlerInvoker_[priority]->Invoke(Callbacks_[priority]);
            Callbacks_[priority].clear();
        }
    }

    void ThreadMain() override
    {
        // Hold this strongly.
        auto this_ = MakeStrong(this);

        std::vector<IPollablePtr> unregisterItems;

        try {
            YT_LOG_DEBUG("Thread started (Name: %v)",
                GetThreadName());

            while (!IsStopping()) {
                // Save items from unregister queue before processing register queue.
                // Otherwise registration and unregistration can be reordered:
                // item was enqueued in register and unregister queues after processing register queue;
                // thus we try to remove item that has not been inserted in Pollables.
                // Newely enqueued items in Unregister and Register queues will be processed at the next iteration.
                UnregisterQueue_.DequeueAll(false, [&] (const auto& pollable) {
                    unregisterItems.push_back(std::move(pollable));
                });

                RegisterQueue_.DequeueAll(false, [&] (auto& pollable) {
                    InsertOrCrash(Pollables_, std::move(pollable));
                });

                HandleEvents();

                for (const auto& pollable : unregisterItems) {
                    EraseOrCrash(Pollables_, pollable);
                }

                unregisterItems.clear();
            }

            YT_LOG_DEBUG("Thread stopped (Name: %v)",
                GetThreadName());
        } catch (const std::exception& ex) {
            YT_LOG_FATAL(ex, "Unhandled exception in executor thread (Name: %v)",
                GetThreadName());
        }

        // Shutdown here.
        for (const auto& pollable : Pollables_) {
            DoUnregister(pollable);
        }
    }

    void StopPrologue() override
    {
        WakeupHandle_.Raise();
    }
};

////////////////////////////////////////////////////////////////////////////////

IThreadPoolPollerPtr CreateThreadPoolPoller(
    int threadCount,
    const TString& threadNamePrefix,
    const TDuration pollingPeriod)
{
    auto poller = New<TThreadPoolPoller>(threadCount, threadNamePrefix, pollingPeriod);
    poller->Start();
    return poller;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

