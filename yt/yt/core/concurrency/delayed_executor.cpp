#include "delayed_executor.h"
#include "action_queue.h"
#include "scheduler.h"
#include "private.h"

#include <yt/yt/core/misc/relaxed_mpsc_queue.h>
#include <yt/yt/core/misc/singleton.h>

#include <yt/yt/core/threading/thread.h>

#include <util/datetime/base.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

static constexpr auto CoalescingInterval = TDuration::MicroSeconds(100);
static constexpr auto LateWarningThreshold = TDuration::Seconds(1);

static const auto& Logger = ConcurrencyLogger;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

struct TDelayedExecutorEntry
    : public TRefCounted
{
    struct TComparer
    {
        bool operator()(const TDelayedExecutorCookie& lhs, const TDelayedExecutorCookie& rhs) const
        {
            if (lhs->Deadline != rhs->Deadline) {
                return lhs->Deadline < rhs->Deadline;
            }
            // Break ties.
            return lhs < rhs;
        }
    };

    TDelayedExecutorEntry(
        TDelayedExecutor::TDelayedCallback callback,
        TInstant deadline,
        IInvokerPtr invoker)
        : Callback(std::move(callback))
        , Deadline(deadline)
        , Invoker(std::move(invoker))
    { }

    TDelayedExecutor::TDelayedCallback Callback;
    std::atomic<bool> CallbackTaken = false;
    TInstant Deadline;
    IInvokerPtr Invoker;

    bool Canceled = false;
    std::optional<std::set<TDelayedExecutorCookie, TComparer>::iterator> Iterator;
};

DEFINE_REFCOUNTED_TYPE(TDelayedExecutorEntry)

////////////////////////////////////////////////////////////////////////////////

class TDelayedExecutorImpl
{
public:
    using TDelayedCallback = TDelayedExecutor::TDelayedCallback;

    static TDelayedExecutorImpl* Get()
    {
        return LeakySingleton<TDelayedExecutorImpl>();
    }

    TFuture<void> MakeDelayed(TDuration delay, IInvokerPtr invoker)
    {
        auto promise = NewPromise<void>();

        auto cookie = Submit(
            BIND_NO_PROPAGATE([=] (bool aborted) mutable {
                if (aborted) {
                    promise.TrySet(TError(NYT::EErrorCode::Canceled, "Delayed promise aborted"));
                } else {
                    promise.TrySet();
                }
            }),
            delay,
            std::move(invoker));

        promise.OnCanceled(BIND_NO_PROPAGATE([=, cookie = std::move(cookie)] (const TError& error) {
            TDelayedExecutor::Cancel(cookie);
            promise.TrySet(TError(NYT::EErrorCode::Canceled, "Delayed callback canceled")
                << error);
        }));

        return promise;
    }

    void WaitForDuration(TDuration duration)
    {
        if (duration == TDuration::Zero()) {
            return;
        }

        auto error = WaitFor(MakeDelayed(duration, nullptr));
        if (error.GetCode() == NYT::EErrorCode::Canceled) {
            throw TFiberCanceledException();
        }

        error.ThrowOnError();
    }

    TDelayedExecutorCookie Submit(TClosure closure, TDuration delay, IInvokerPtr invoker)
    {
        YT_VERIFY(closure);
        return Submit(
            BIND_NO_PROPAGATE(&ClosureToDelayedCallbackAdapter, std::move(closure)),
            delay.ToDeadLine(),
            std::move(invoker));
    }

    TDelayedExecutorCookie Submit(TClosure closure, TInstant deadline, IInvokerPtr invoker)
    {
        YT_VERIFY(closure);
        return Submit(
            BIND_NO_PROPAGATE(&ClosureToDelayedCallbackAdapter, std::move(closure)),
            deadline,
            std::move(invoker));
    }

    TDelayedExecutorCookie Submit(TDelayedCallback callback, TDuration delay, IInvokerPtr invoker)
    {
        YT_VERIFY(callback);
        return Submit(
            std::move(callback),
            delay.ToDeadLine(),
            std::move(invoker));
    }

    TDelayedExecutorCookie Submit(TDelayedCallback callback, TInstant deadline, IInvokerPtr invoker)
    {
        YT_VERIFY(callback);
        auto entry = New<TDelayedExecutorEntry>(std::move(callback), deadline, std::move(invoker));
        PollerThread_->EnqueueSubmission(entry);

        if (!PollerThread_->Start()) {
            if (auto callback = TakeCallback(entry)) {
                callback(/*aborted*/ true);
            }
#if defined(_asan_enabled_)
            NSan::MarkAsIntentionallyLeaked(entry.Get());
#endif
        }

        return entry;
    }

    void Cancel(TDelayedExecutorEntryPtr entry)
    {
        if (!entry) {
            return;
        }
        PollerThread_->EnqueueCancelation(std::move(entry));
        // No #EnsureStarted call is needed here: having #entry implies that #Submit call has been previously made.
        // Also in contrast to #Submit we have no special handling for #entry in case the Poller Thread
        // has been already terminated.
    }

private:
    class TPollerThread
        : public NThreading::TThread
    {
    public:
        TPollerThread()
            : TThread(
                "DelayedPoller",
                NThreading::TThreadOptions{
                    .ShutdownPriority = 200,
                })
        { }

        void EnqueueSubmission(TDelayedExecutorEntryPtr entry)
        {
            SubmitQueue_.Enqueue(std::move(entry));

            if (NotificationScheduled_.load(std::memory_order::relaxed)) {
                return;
            }
            if (!NotificationScheduled_.exchange(true)) {
                EventCount_->NotifyOne();
            }
        }

        void EnqueueCancelation(TDelayedExecutorEntryPtr entry)
        {
            CancelQueue_.Enqueue(std::move(entry));
        }

    private:
        const TIntrusivePtr<NThreading::TEventCount> EventCount_ = New<NThreading::TEventCount>();

        std::atomic<bool> NotificationScheduled_ = false;

        //! Only touched from DelayedPoller thread.
        std::set<TDelayedExecutorEntryPtr, TDelayedExecutorEntry::TComparer> ScheduledEntries_;

        //! Enqueued from any thread, dequeued from DelayedPoller thread.
        TRelaxedMpscQueue<TDelayedExecutorEntryPtr> SubmitQueue_;
        TRelaxedMpscQueue<TDelayedExecutorEntryPtr> CancelQueue_;
        TActionQueuePtr DelayedQueue_;
        IInvokerPtr DelayedInvoker_;

        NProfiling::TGauge ScheduledCallbacksGauge_ = ConcurrencyProfiler.Gauge("/delayed_executor/scheduled_callbacks");
        NProfiling::TCounter SubmittedCallbacksCounter_ = ConcurrencyProfiler.Counter("/delayed_executor/submitted_callbacks");
        NProfiling::TCounter CanceledCallbacksCounter_ = ConcurrencyProfiler.Counter("/delayed_executor/canceled_callbacks");
        NProfiling::TCounter StaleCallbacksCounter_ = ConcurrencyProfiler.Counter("/delayed_executor/stale_callbacks");


        void StartPrologue() override
        {
            // Boot the Delayed Executor thread up.
            // Do it here to avoid weird crashes when execl is being used in another thread.
            DelayedQueue_ = New<TActionQueue>("DelayedExecutor");
            DelayedInvoker_ = DelayedQueue_->GetInvoker();
        }

        void StopPrologue() override
        {
            EventCount_->NotifyOne();
        }

        void ThreadMain() override
        {
            while (true) {
                auto cookie = EventCount_->PrepareWait();
                // Reset notification flag before processing queues but after prepare wait.
                // Otherwise notifies occurred after processing queues and before wait
                // can be lost. No new notifies can happen if notify flag is true before
                // prepare wait.
                NotificationScheduled_.store(false);

                ProcessQueues();

                if (IsStopping()) {
                    break;
                }

                auto now = GetInstant();

                auto deadline = !ScheduledEntries_.empty()
                    ? std::max((*ScheduledEntries_.begin())->Deadline, now + CoalescingInterval)
                    : TInstant::Max();
                EventCount_->Wait(cookie, deadline);
            }

            // Perform graceful shutdown.

            // First run the scheduled callbacks with |aborted = true|.
            // NB: The callbacks are forwarded to the DelayedExecutor thread to prevent any user-code
            // from leaking to the Delayed Poller thread, which is, e.g., fiber-unfriendly.
            auto runAbort = [&] (const TDelayedExecutorEntryPtr& entry) {
                RunCallback(entry, /*aborted*/ true);
            };
            for (const auto& entry : ScheduledEntries_) {
                runAbort(entry);
            }
            ScheduledEntries_.clear();

            // Now we handle the queued callbacks similarly.
            {
                TDelayedExecutorEntryPtr entry;
                while (SubmitQueue_.TryDequeue(&entry)) {
                    runAbort(entry);
                }
            }

            // As for the cancelation queue, we just drop these entries.
            {
                TDelayedExecutorEntryPtr entry;
                while (CancelQueue_.TryDequeue(&entry)) {
                }
            }

            // Gracefully (sic!) shut the Delayed Executor thread down
            // to ensure invocation of the callbacks scheduled above.
            DelayedQueue_->Shutdown(/*graceful*/ true);
        }

        void ProcessQueues()
        {
            auto now = TInstant::Now();

            {
                int submittedCallbacks = 0;
                TDelayedExecutorEntryPtr entry;
                while (SubmitQueue_.TryDequeue(&entry)) {
                    if (entry->Canceled) {
                        continue;
                    }
                    if (entry->Deadline + LateWarningThreshold < now) {
                        StaleCallbacksCounter_.Increment();
                        YT_LOG_DEBUG("Found a late delayed submitted callback (Deadline: %v, Now: %v)",
                            entry->Deadline,
                            now);
                    }
                    auto [it, inserted] = ScheduledEntries_.insert(entry);
                    YT_VERIFY(inserted);
                    entry->Iterator = it;
                    ++submittedCallbacks;
                }
                SubmittedCallbacksCounter_.Increment(submittedCallbacks);
            }

            {
                int canceledCallbacks = 0;
                TDelayedExecutorEntryPtr entry;
                while (CancelQueue_.TryDequeue(&entry)) {
                    if (entry->Canceled) {
                        continue;
                    }
                    entry->Canceled = true;
                    TakeCallback(entry);
                    if (entry->Iterator) {
                        ScheduledEntries_.erase(*entry->Iterator);
                        entry->Iterator.reset();
                    }
                    ++canceledCallbacks;
                }
                CanceledCallbacksCounter_.Increment(canceledCallbacks);
            }

            ScheduledCallbacksGauge_.Update(ScheduledEntries_.size());
            while (!ScheduledEntries_.empty()) {
                auto it = ScheduledEntries_.begin();
                const auto& entry = *it;
                if (entry->Deadline > now + CoalescingInterval) {
                    break;
                }
                if (entry->Deadline + LateWarningThreshold < now) {
                    StaleCallbacksCounter_.Increment();
                    YT_LOG_DEBUG("Found a late delayed scheduled callback (Deadline: %v, Now: %v)",
                        entry->Deadline,
                        now);
                }
                RunCallback(entry, false);
                entry->Iterator.reset();
                ScheduledEntries_.erase(it);
            }
        }

        void RunCallback(const TDelayedExecutorEntryPtr& entry, bool abort)
        {
            if (auto callback = TakeCallback(entry)) {
                (entry->Invoker ? entry->Invoker : DelayedInvoker_)->Invoke(BIND_NO_PROPAGATE(std::move(callback), abort));
            }
        }
    };

    using TPollerThreadPtr = TIntrusivePtr<TPollerThread>;
    const TPollerThreadPtr PollerThread_ = New<TPollerThread>();


    static void ClosureToDelayedCallbackAdapter(const TClosure& closure, bool aborted)
    {
        if (aborted) {
            return;
        }
        closure();
    }

    static TDelayedExecutor::TDelayedCallback TakeCallback(const TDelayedExecutorEntryPtr& entry)
    {
        if (entry->CallbackTaken.exchange(true)) {
            return {};
        } else {
            return std::move(entry->Callback);
        }
    }

    DECLARE_LEAKY_SINGLETON_FRIEND()
};

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TFuture<void> TDelayedExecutor::MakeDelayed(
    TDuration delay,
    IInvokerPtr invoker)
{
    return NDetail::TDelayedExecutorImpl::Get()->MakeDelayed(delay, std::move(invoker));
}

void TDelayedExecutor::WaitForDuration(TDuration duration)
{
    NDetail::TDelayedExecutorImpl::Get()->WaitForDuration(duration);
}

TDelayedExecutorCookie TDelayedExecutor::Submit(
    TDelayedCallback callback,
    TDuration delay,
    IInvokerPtr invoker)
{
    return NDetail::TDelayedExecutorImpl::Get()->Submit(std::move(callback), delay, std::move(invoker));
}

TDelayedExecutorCookie TDelayedExecutor::Submit(
    TClosure closure,
    TDuration delay,
    IInvokerPtr invoker)
{
    return NDetail::TDelayedExecutorImpl::Get()->Submit(std::move(closure), delay, std::move(invoker));
}

TDelayedExecutorCookie TDelayedExecutor::Submit(
    TDelayedCallback callback,
    TInstant deadline,
    IInvokerPtr invoker)
{
    return NDetail::TDelayedExecutorImpl::Get()->Submit(std::move(callback), deadline, std::move(invoker));
}

TDelayedExecutorCookie TDelayedExecutor::Submit(
    TClosure closure,
    TInstant deadline,
    IInvokerPtr invoker)
{
    return NDetail::TDelayedExecutorImpl::Get()->Submit(std::move(closure), deadline, std::move(invoker));
}

void TDelayedExecutor::Cancel(const TDelayedExecutorCookie& cookie)
{
    NDetail::TDelayedExecutorImpl::Get()->Cancel(cookie);
}

void TDelayedExecutor::CancelAndClear(TDelayedExecutorCookie& cookie)
{
    NDetail::TDelayedExecutorImpl::Get()->Cancel(std::move(cookie));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
