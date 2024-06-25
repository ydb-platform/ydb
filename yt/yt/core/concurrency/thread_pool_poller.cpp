#include "poller.h"
#include "thread_pool_poller.h"
#include "private.h"
#include "two_level_fair_share_thread_pool.h"
#include "new_fair_share_thread_pool.h"

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/proc.h>
#include <yt/yt/core/misc/mpsc_stack.h>

#include <yt/yt/core/profiling/tscp.h>

#include <yt/yt/core/threading/thread.h>

#include <library/cpp/yt/threading/notification_handle.h>

#include <library/cpp/yt/memory/ref_tracked.h>

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

class TThreadPoolPollerImpl;

namespace {

DEFINE_ENUM(EFinishResult,
    (None)
    (Repeat)
    (Shutdown)
);

class TCookieState
{
public:
    // AquireControl is called from poller thread and from Retry.
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
        } while (!State_.compare_exchange_weak(currentState, currentState | static_cast<ui64>(control) | RunningFlag));

        return !(currentState & RunningFlag);
    }

    // Resets control and returns previous value.
    ui32 ResetControl()
    {
        auto currentState = State_.load();
        while (!State_.compare_exchange_weak(currentState, currentState & (UnregisterFlag | RunningFlag)));
        return static_cast<ui32>(currentState);
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

        return !(currentState & RunningFlag);
    }

    EFinishResult Finish()
    {
        auto currentState = State_.load();

        YT_VERIFY(currentState & RunningFlag);

        do {
            if (currentState & UnregisterFlag) {
                // Run destroy.
                return EFinishResult::Shutdown;
            }

            if (currentState & ~(UnregisterFlag | RunningFlag)) {
                // Has state. Retry.
                return EFinishResult::Repeat;
            }

        } while (!State_.compare_exchange_weak(currentState, currentState & ~RunningFlag));

        return EFinishResult::None;
    }

private:
    static constexpr auto ControlShift = sizeof(ui32) * 8;
    static constexpr ui64 UnregisterFlag = 1ULL << ControlShift;
    static constexpr ui64 RunningFlag = 1ULL << (ControlShift + 1);

    // No contention expected when accessing this atomic variable.
    // So we can safely (regarding to performance) use CAS.
    std::atomic<ui64> State_ = 0;
};

struct TPollableCookie
    : public TRefCounted
    , public TCookieState
{
    const TPromise<void> UnregisterPromise = NewPromise<void>();

    TIntrusivePtr<TThreadPoolPollerImpl> PollerThread;
    IInvokerPtr Invoker;

    explicit TPollableCookie(TThreadPoolPollerImpl* pollerThread)
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
        // N.B. Edge-triggered mode disables one shot mode.
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

class TThreadPoolPollerImpl
    : public IThreadPoolPoller
    , public NThreading::TThread
{
public:
    TThreadPoolPollerImpl(
        int threadCount,
        const TString& threadNamePrefix,
        TDuration pollingPeriod)
        : TThread(Format("%v:%v", threadNamePrefix, "Poll"))
        , Logger(ConcurrencyLogger().WithTag("ThreadNamePrefix: %v", threadNamePrefix))
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

    void Reconfigure(TDuration pollingPeriod) override
    {
        FairShareThreadPool_->Configure(pollingPeriod);
    }

    bool TryRegister(const IPollablePtr& pollable, TString poolName) override
    {
        // FIXME(lukyan): Enqueueing in register queue may happen after stopping.
        // Create cookie when dequeueing from register queue?
        // How to prevent arming FD when stopping.
        if (IsStopping()) {
            return false;
        }

        auto cookie = New<TPollableCookie>(this);
        cookie->Invoker = FairShareThreadPool_->GetInvoker(
            poolName,
            Format("%v", pollable.Get()));
        pollable->SetCookie(std::move(cookie));
        RegisterQueue_.Enqueue(pollable);

        YT_LOG_DEBUG("Pollable registered (%v)",
            pollable->GetLoggingTag());

        return true;
    }

    void SetExecutionPool(const IPollablePtr& pollable, TString poolName) override
    {
        auto* cookie = TPollableCookie::FromPollable(pollable.Get());
        cookie->Invoker = FairShareThreadPool_->GetInvoker(
            poolName,
            Format("%v", pollable.Get()));
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
        ScheduleEvent(pollable, EPollControl::Retry);
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

        explicit TRunEventGuard(IPollable* pollable)
            : Pollable_(pollable)
        { }

        explicit TRunEventGuard(TRunEventGuard&& other)
            : Pollable_(std::move(other.Pollable_))
        {
            other.Pollable_ = nullptr;
        }

        TRunEventGuard(const TRunEventGuard&) = delete;

        TRunEventGuard& operator=(const TRunEventGuard&) = delete;
        TRunEventGuard& operator=(TRunEventGuard&&) = delete;

        ~TRunEventGuard()
        {
            if (!Pollable_) {
                return;
            }

            auto* cookie = TPollableCookie::FromPollable(Pollable_);
            cookie->ResetControl();
            Destroy(Pollable_);
        }

        void operator()()
        {
            auto* cookie = TPollableCookie::FromPollable(Pollable_);
            auto control = EPollControl(cookie->ResetControl());
            RunNoExcept([&] {
                Pollable_->OnEvent(control);
            });
            Destroy(Pollable_);
            Pollable_ = nullptr;
        }

    private:
        IPollable* Pollable_ = nullptr;

        static void Destroy(IPollable* pollable)
        {
            auto* cookie = TPollableCookie::FromPollable(pollable);

            auto result = cookie->Finish();
            switch (result) {
                case EFinishResult::Shutdown:
                    DoShutdownPollable(cookie, pollable);
                    break;
                case EFinishResult::Repeat:
                    cookie->Invoker->Invoke(BIND(TRunEventGuard(pollable)));
                    break;
                case EFinishResult::None:
                    break;
            }
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

    // TODO(lukyan): Move static functions in Cookie?
    static void ScheduleEvent(const IPollablePtr& pollable, EPollControl control)
    {
        // Can safely dereference pollable because even unregistered pollables are hold in Pollables_.
        auto* cookie = TPollableCookie::FromPollable(pollable.Get());
        if (cookie->AquireControl(ToUnderlying(control))) {
            cookie->Invoker->Invoke(BIND(TRunEventGuard(pollable.Get())));
        }
    }

    static void DoShutdownPollable(TPollableCookie* cookie, IPollable* pollable)
    {
        // Poller guarantees that OnShutdown is never executed concurrently with OnEvent().
        // Otherwise it will be removed in TRunEventGuard.
        RunNoExcept([&] {
            pollable->OnShutdown();
        });

        cookie->UnregisterPromise.Set();
        cookie->Invoker.Reset();
        auto pollerThread = std::move(cookie->PollerThread);
        pollerThread->UnregisterQueue_.Enqueue(pollable);
        pollerThread->WakeupHandle_.Raise();
    }

    void DoUnregister(const IPollablePtr& pollable)
    {
        YT_LOG_DEBUG("Requesting pollable unregistration (%v)",
            pollable->GetLoggingTag());

        auto* cookie = TPollableCookie::TryFromPollable(pollable.Get());
        YT_VERIFY(cookie);

        if (cookie->SetUnregisterFlag()) {
            DoShutdownPollable(cookie, pollable.Get());
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

            ScheduleEvent(pollable, control);
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

// TThreadPoolPollerImpl::ThreadMain holds strong reference to `this`.
// therefore object cannot be removed until thread is running.
// User MUST call Shutdown explicitly to destroy object.
//
// This wrapper class solves this problem. Thread is stopped in destructor and resources are released.
class TThreadPoolPoller
    : public IThreadPoolPoller
{
public:
    TThreadPoolPoller(int threadCount, const TString& threadNamePrefix, TDuration pollingPeriod)
        : Poller_(New<TThreadPoolPollerImpl>(threadCount, threadNamePrefix, pollingPeriod))
    { }

    ~TThreadPoolPoller()
    {
        Poller_->Shutdown();
    }

    void Start()
    {
        Poller_->Start();
    }

    void Reconfigure(int threadCount) override
    {
        return Poller_->Reconfigure(threadCount);
    }

    void Reconfigure(TDuration pollingPeriod) override
    {
        return Poller_->Reconfigure(pollingPeriod);
    }

    void Shutdown() override
    {
        Poller_->Shutdown();
    }

    bool TryRegister(const IPollablePtr& pollable, TString poolName = "default") override
    {
        return Poller_->TryRegister(pollable, std::move(poolName));
    }

    void SetExecutionPool(const IPollablePtr& pollable, TString poolName) override
    {
        Poller_->SetExecutionPool(pollable, std::move(poolName));
    }

    TFuture<void> Unregister(const IPollablePtr& pollable) override
    {
        return Poller_->Unregister(pollable);
    }

    void Arm(TFileDescriptor fd, const IPollablePtr& pollable, EPollControl control) override
    {
        Poller_->Arm(fd, pollable, control);
    }

    void Retry(const IPollablePtr& pollable) override
    {
        Poller_->Retry(pollable);
    }

    void Unarm(TFileDescriptor fd, const IPollablePtr& pollable) override
    {
        Poller_->Unarm(fd, pollable);
    }

    IInvokerPtr GetInvoker() const override
    {
        return Poller_->GetInvoker();
    }

private:
    TIntrusivePtr<TThreadPoolPollerImpl> Poller_;
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
