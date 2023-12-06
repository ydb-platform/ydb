#include "thread_pool.h"
#include "poller.h"
#include "thread_pool_poller.h"
#include "private.h"
#include "profiling_helpers.h"
#include "scheduler_thread.h"
#include "two_level_fair_share_thread_pool.h"
#include "new_fair_share_thread_pool.h"

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

class TCookieState
{
public:
    // AquireControl is called from poller thread.
    bool AquireControl(ui32 control)
    {
        auto currentState = State_.load();
        do {
            if (currentState & UnregisterFlag) {
                return false;
            }

            if ((static_cast<ui32>(currentState) & control) == control) {
                return false;
            }
        } while (!State_.compare_exchange_weak(currentState, (currentState | static_cast<ui64>(control)) + RefValue));

        return true;
    }

    void ResetControl(ui32 control)
    {
        auto currentState = State_.load();
        do {
            if (!(static_cast<ui32>(currentState) & control)) {
                break;
            }
        } while (!State_.compare_exchange_weak(currentState, currentState & ~static_cast<ui64>(control)));
    }

    // Returns destroy flag.
    bool SetUnregisterFlag()
    {
        auto currentState = State_.load();
        do {
            // Pollable has been already unregistered.
            if (currentState & UnregisterFlag) {
                return false;
            }
        } while (!State_.compare_exchange_weak(currentState, currentState | UnregisterFlag));

        // No refs.
        return currentState == 0;
    }

    // Returns destroy flag.
    bool ReleaseRef()
    {
        auto prevState = State_.fetch_sub(RefValue);

        YT_VERIFY(prevState >= RefValue);
        auto currentState = prevState - RefValue;
        if ((currentState >> ControlShift) == 1) {
            // Verify that control flags are empty when there are no refs and unregister flag is set.
            YT_VERIFY(static_cast<ui32>(currentState) == 0);
        }

        return prevState == (RefValue | UnregisterFlag);
    }

private:
    static constexpr auto ControlShift = sizeof(ui32) * 8;
    static constexpr ui64 UnregisterFlag = 1ULL << ControlShift;
    static constexpr ui64 RefValue = UnregisterFlag * 2;
    // No contention expected when accessing this atomic variable.
    // So we can safely (regarding to performance) use CAS.
    std::atomic<ui64> State_ = 0;
};

struct TPollableCookie
    : public TRefCounted
    , public TCookieState
{
    const TPromise<void> UnregisterPromise = NewPromise<void>();

    TIntrusivePtr<TThreadPoolPoller> PollerThread;
    IInvokerPtr Invoker;

    explicit TPollableCookie(TThreadPoolPoller* pollerThread)
        : PollerThread(pollerThread)
    { }

    static TPollableCookie* TryFromPollable(IPollable* pollable)
    {
        return static_cast<TPollableCookie*>(pollable->GetCookie());
    }

    static TPollableCookie* FromPollable(IPollable* pollable)
    {
        auto* cookie = TryFromPollable(pollable);
        YT_VERIFY(cookie);
        return cookie;
    }
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

} // namespace

class TThreadPoolPoller
    : public IThreadPoolPoller
    , public NThreading::TThread
{
public:
    TThreadPoolPoller(
        int threadCount,
        const TString& threadNamePrefix,
        TDuration pollingPeriod)
        : TThread(Format("%v:%v", threadNamePrefix, "Poll"))
        , Logger(ConcurrencyLogger.WithTag("ThreadNamePrefix: %v", threadNamePrefix))
    {
        // Register auxilary notifictation handle to wake up poller thread when deregistering
        // pollables and on shutdown.
        PollerImpl_.Set(nullptr, WakeupHandle_.GetFD(), CONT_POLL_EDGE_TRIGGERED | CONT_POLL_READ);

        FairShareThreadPool_ = CreateNewTwoLevelFairShareThreadPool(
            threadCount,
            threadNamePrefix + "FS",
            {
                .PollingPeriod = pollingPeriod,
                .PoolRetentionTime = TDuration::Zero()
            });
        AuxInvoker_ = FairShareThreadPool_->GetInvoker("aux", "default");
    }

    void Reconfigure(int threadCount) override
    {
        FairShareThreadPool_->Configure(threadCount);
    }

    bool TryRegister(const IPollablePtr& pollable) override
    {
        // FIXME(lukyan): Enqueueing in register queue may happen after stopping.
        // Create cookie when dequeueing from register queue?
        // How to prevent arming FD when stopping.
        if (IsStopping()) {
            return false;
        }

        auto cookie = New<TPollableCookie>(this);
        cookie->Invoker = FairShareThreadPool_->GetInvoker("main", Format("%v", pollable.Get()));
        pollable->SetCookie(std::move(cookie));
        RegisterQueue_.Enqueue(pollable);

        YT_LOG_DEBUG("Pollable registered (%v)",
            pollable->GetLoggingTag());

        return true;
    }

    // TODO(lukyan): Method OnShutdown in the interface and returned future are redundant.
    // Shutdown can be done by subscribing returned future or some promise can be set inside OnShutdown.
    TFuture<void> Unregister(const IPollablePtr& pollable) override
    {
        auto* cookie = TPollableCookie::TryFromPollable(pollable.Get());
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

    void Retry(const IPollablePtr& pollable) override
    {
        if (auto guard = TryAcquireRunEventGuard(pollable.Get(), EPollControl::Retry)) {
            auto* cookie = TPollableCookie::FromPollable(pollable.Get());
            cookie->Invoker->Invoke(BIND(std::move(guard)));
        }
    }

    IInvokerPtr GetInvoker() const override
    {
        return AuxInvoker_;
    }

    void Shutdown() override
    {
        TThread::Stop();
    }

private:
    class TRunEventGuard
    {
    public:
        TRunEventGuard() = default;

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
                GetFinalizerInvoker()->Invoke(BIND(&ResetAndDestroy, Unretained(Pollable_), Control_));
            }
        }

        explicit operator bool() const
        {
            return static_cast<bool>(Pollable_);
        }

        void operator()()
        {
            auto* cookie = TPollableCookie::FromPollable(Pollable_);
            cookie->ResetControl(ToUnderlying(Control_));

            Pollable_->OnEvent(Control_);
            Destroy(Pollable_);
            Pollable_ = nullptr;
        }

    private:
        IPollable* Pollable_ = nullptr;
        EPollControl Control_ = EPollControl::None;

        static void Destroy(IPollable* pollable)
        {
            auto* cookie = TPollableCookie::FromPollable(pollable);
            if (cookie->ReleaseRef()) {
                pollable->OnShutdown();
                cookie->UnregisterPromise.Set();
                cookie->Invoker.Reset();
                auto pollerThread = std::move(cookie->PollerThread);
                pollerThread->UnregisterQueue_.Enqueue(pollable);
                pollerThread->WakeupHandle_.Raise();
            }
        }

        static void ResetAndDestroy(IPollable* pollable, EPollControl control)
        {
            auto* cookie = TPollableCookie::FromPollable(pollable);
            cookie->ResetControl(ToUnderlying(control));
            Destroy(pollable);
        }
    };

    const NLogging::TLogger Logger;

    ITwoLevelFairShareThreadPoolPtr FairShareThreadPool_;
    IInvokerPtr AuxInvoker_;

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

    static TRunEventGuard TryAcquireRunEventGuard(IPollable* pollable, EPollControl control)
    {
        auto* cookie = TPollableCookie::FromPollable(pollable);
        YT_VERIFY(cookie->GetRefCount() > 0);

        if (cookie->AquireControl(ToUnderlying(control))) {
            return {pollable, control};
        }

        return {};
    }

    void DoUnregister(const IPollablePtr& pollable)
    {
        YT_LOG_DEBUG("Requesting pollable unregistration (%v)",
            pollable->GetLoggingTag());

        auto* cookie = TPollableCookie::TryFromPollable(pollable.Get());
        YT_VERIFY(cookie);

        if (cookie->SetUnregisterFlag()) {
            // Poller guarantees that OnShutdown is never executed concurrently with OnEvent().
            // Otherwise it will be removed in TRunEventGuard.

            pollable->OnShutdown();
            cookie->UnregisterPromise.Set();
            cookie->Invoker.Reset();
            auto pollerThread = std::move(cookie->PollerThread);
            pollerThread->UnregisterQueue_.Enqueue(pollable);
            pollerThread->WakeupHandle_.Raise();
        }
    }

    void HandleEvents(int eventCount)
    {
        for (int index = 0; index < eventCount; ++index) {
            const auto& event = PooledImplEvents_[index];
            auto control = FromImplControl(PollerImpl_.ExtractFilter(&event));
            auto* pollable = static_cast<IPollable*>(PollerImpl_.ExtractEvent(&event));

            // Null pollable stands for wakeup handle.
            if (!pollable) {
                WakeupHandle_.Clear();
                continue;
            }

            YT_LOG_TRACE("Got pollable event (Pollable: %v, Control: %v)",
                pollable->GetLoggingTag(),
                control);

            YT_VERIFY(pollable->GetRefCount() > 0);

            // Can safely dereference pollable because even unregistered pollables are hold in Pollables_.
            if (auto guard = TryAcquireRunEventGuard(pollable, control)) {
                auto* cookie = TPollableCookie::FromPollable(pollable);
                cookie->Invoker->Invoke(BIND(std::move(guard)));
            }
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

            while (true) {
                int eventCount = PollerImpl_.Wait(PooledImplEvents_.data(), PooledImplEvents_.size(), PollerThreadQuantum.MicroSeconds());

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

                HandleEvents(eventCount);

                for (const auto& pollable : unregisterItems) {
                    EraseOrCrash(Pollables_, pollable);
                }

                unregisterItems.clear();

                if (IsStopping()) {
                    if (Pollables_.empty()) {
                        break;
                    }
                    // Need to unregister pollables when stopping to break reference cycles between pollables and poller.
                    for (const auto& pollable : Pollables_) {
                        DoUnregister(pollable);
                    }
                }
            }

            YT_LOG_DEBUG("Thread stopped (Name: %v)",
                GetThreadName());
        } catch (const std::exception& ex) {
            YT_LOG_FATAL(ex, "Unhandled exception in executor thread (Name: %v)",
                GetThreadName());
        }

        RegisterQueue_.DequeueAll(false, [&] (const auto&) { });
        UnregisterQueue_.DequeueAll(false, [&] (const auto&) { });
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
    TDuration pollingPeriod)
{
    auto poller = New<TThreadPoolPoller>(threadCount, threadNamePrefix, pollingPeriod);
    poller->Start();
    return poller;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

