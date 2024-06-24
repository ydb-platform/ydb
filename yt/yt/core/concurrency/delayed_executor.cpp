#include "delayed_executor.h"
#include "action_queue.h"
#include "scheduler.h"
#include "private.h"

#include <yt/yt/core/actions/invoker_util.h>
#include <yt/yt/core/misc/relaxed_mpsc_queue.h>
#include <yt/yt/core/misc/singleton.h>

#include <yt/yt/core/threading/thread.h>

#include <util/datetime/base.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

static constexpr auto CoalescingInterval = TDuration::MicroSeconds(100);
static constexpr auto LateWarningThreshold = TDuration::Seconds(1);

static constexpr auto& Logger = ConcurrencyLogger;

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

//! (arkady-e1ppa) MO's/Fence explanation:
/*
    We want for shutdown to guarantee that no callback is left in any queue
    once it is over. Otherwise, memory leak or a deadlock of Poller/GrpcServer
    (or someone else who blocks thread until some callback is run) will occur.

    We model our queue with Enqueue being RMW^rel(Queue_, x, y) and Dequeue
    being RMW^acq(Queue_, x, y), where x is what we have read and y is what we
    have observed. Missing callback would imply that in Submit method enqueueing |CB|
    we have observed Stopping_ |false| (e.g. TThread::Start (c) returned |true|)
    but also in ThreadMain during the SubmitQueue drain (f) we have not observed the
    |CB|. Execution is schematically listed below:
        T1(Submit)                          T2(Shutdown)
    RMW^rel(Queue_, empty, CB) (a)       W^rel(Stopping_, true)        (d)
            |sequenced-before                   |simply hb
    Fence^sc                   (b)       Fence^sc                      (e)
            |sequenced-before                   |sequenced-before
    R^acq(Stopping_, false)    (c)       RMW^acq(Queue_, empty, empty) (f)

    Since (c) reads |false| it must be reading from Stopping_ ctor which is
    W^na(Stopping_, false) which preceedes (d) in modification order. Thus
    (c) must read-from some modification preceding (d) in modification order (ctor)
    and therefore (c) -cob-> (d) (coherence ordered before).
    Likewise, (f) reads |empty| which can only be read from Queue_ ctor or
    prior Dequeue both of which preceede (a) in modification order (ctor is obvious;
    former Dequeue by assumption that no one has read |CB| ever: if some (a) was
    prior to some Dequeue in modification order, |CB| would inevitably be read).
    So, (f) -cob-> (a). For fences we now have to relations:
    (b) -sb-> (c) -cob-> (d) -simply hb-> (e) => (b) -S-> (e)
    (e) -sb-> (f) -cob-> (a) -sb-> (b) => (e) -S-> (b)
    Here sb is sequenced-before and S is sequentially-consistent total ordering.
    We have formed a loop in S thus contradicting the assumption.
*/


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
        PollerThread_->EnqueueSubmission(entry); // <- (a)

        std::atomic_thread_fence(std::memory_order::seq_cst); // <- (b)

        if (!PollerThread_->Start()) { // <- (c)
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

        class TCallbackGuard
        {
        public:
            TCallbackGuard(TCallback<void(bool)> callback, bool aborted) noexcept
                : Callback_(std::move(callback))
                , Aborted_(aborted)
            { }

            TCallbackGuard(TCallbackGuard&& other) = default;

            TCallbackGuard(const TCallbackGuard&) = delete;

            TCallbackGuard& operator=(const TCallbackGuard&) = delete;
            TCallbackGuard& operator=(TCallbackGuard&&) = delete;

            void operator()()
            {
                auto callback = std::move(Callback_);
                YT_VERIFY(callback);
                callback.Run(Aborted_);
            }

            ~TCallbackGuard()
            {
                if (Callback_) {
                    YT_LOG_DEBUG("Aborting delayed executor callback");

                    auto callback = std::move(Callback_);
                    callback(/*aborted*/ true);
                }
            }

        private:
            TCallback<void(bool)> Callback_;
            bool Aborted_;
        };


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
                    // We have Stopping_.store(true) in simply happens-before relation.
                    // Assume Stopping_.store(true, release) is (d).
                    // NB(arkady-e1ppa): At the time of writing it is seq_cst
                    // actually, but
                    // 1. We don't need it to be for correctness hehe
                    // 2. It won't help us here anyway
                    // 3. It might be changed as it could be suboptimal.
                    break;
                }

                auto now = GetInstant();

                auto deadline = !ScheduledEntries_.empty()
                    ? std::max((*ScheduledEntries_.begin())->Deadline, now + CoalescingInterval)
                    : TInstant::Max();
                EventCount_->Wait(cookie, deadline);
            }

            std::atomic_thread_fence(std::memory_order::seq_cst); // <- (e)

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
                while (SubmitQueue_.TryDequeue(&entry)) { // <- (f)
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
                const auto& invoker = entry->Invoker
                    ? entry->Invoker
                    : DelayedInvoker_;
                invoker
                    ->Invoke(BIND_NO_PROPAGATE(TCallbackGuard(std::move(callback), abort)));
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
