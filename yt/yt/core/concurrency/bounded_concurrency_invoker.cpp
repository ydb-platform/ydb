#include "bounded_concurrency_invoker.h"

#include <yt/yt/core/actions/current_invoker.h>
#include <yt/yt/core/actions/invoker_detail.h>
#include <yt/yt/core/actions/callback.h>

#include <yt/yt/core/misc/ring_queue.h>

#include <library/cpp/yt/misc/tls.h>
#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TBoundedConcurrencyInvoker;

YT_DEFINE_THREAD_LOCAL(TBoundedConcurrencyInvoker*, CurrentBoundedConcurrencyInvoker);

class TBoundedConcurrencyInvoker
    : public IBoundedConcurrencyInvoker
    , public TInvokerWrapper<true>
{
public:
    TBoundedConcurrencyInvoker(
        IInvokerPtr underlyingInvoker,
        int maxConcurrentInvocations)
        : TInvokerWrapper(std::move(underlyingInvoker))
        , MaxConcurrentInvocations_(maxConcurrentInvocations)
    { }

    using TInvokerWrapper::Invoke;

    void Invoke(TClosure callback) override
    {
        auto guard = Guard(SpinLock_);
        if (Semaphore_ < MaxConcurrentInvocations_ && !PendingMaxConcurrentInvocations_.has_value()) {
            YT_VERIFY(Queue_.empty());
            IncrementSemaphore(+1);
            guard.Release();
            RunCallback(std::move(callback));
        } else {
            Queue_.push(std::move(callback));
        }
    }

    void SetMaxConcurrentInvocations(int newMaxConcurrentInvocations) override
    {
        // XXX(apachee): Check that newMaxConcurrentInvocations >= 0? Verify? If condition with throw?

        auto guard = Guard(SpinLock_);

        if (newMaxConcurrentInvocations == MaxConcurrentInvocations_) {
            return;
        }

        if (newMaxConcurrentInvocations >= Semaphore_) {
            i64 diff = newMaxConcurrentInvocations - Semaphore_;
            i64 numberOfCallbacksToRun = std::min(diff, std::ssize(Queue_));
            if (numberOfCallbacksToRun == 0) {
                // Fast path.

                PendingMaxConcurrentInvocations_ = {};
                MaxConcurrentInvocations_ = newMaxConcurrentInvocations;
            } else {
                // Slow path.

                std::vector<TClosure> callbacksToRun;
                callbacksToRun.reserve(numberOfCallbacksToRun);

                for (int i = 0; i < numberOfCallbacksToRun; i++) {
                    YT_ASSERT(!Queue_.empty());
                    callbacksToRun.push_back(std::move(Queue_.front()));
                    Queue_.pop();
                }

                PendingMaxConcurrentInvocations_ = {};
                MaxConcurrentInvocations_ = newMaxConcurrentInvocations;
                IncrementSemaphore(numberOfCallbacksToRun);

                guard.Release();
                for (auto& callback : callbacksToRun) {
                    RunCallback(std::move(callback));
                }
            }
        } else /* newMaxConcurrentInvocations < Semaphore_ */ {
            // NB(apachee): We have to wait for some of the callbacks to finish before updating MaxConcurrentInvocations_.
            PendingMaxConcurrentInvocations_ = newMaxConcurrentInvocations;
        }
    }

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    // If set, it is the next value of MaxConcurrentInvocations_.
    // Used only when decrease of MaxConcurrentInvocations_ value is requested.
    std::optional<int> PendingMaxConcurrentInvocations_;
    int MaxConcurrentInvocations_;
    TRingQueue<TClosure> Queue_;
    int Semaphore_ = 0;

private:
    class TInvocationGuard
    {
    public:
        explicit TInvocationGuard(TIntrusivePtr<TBoundedConcurrencyInvoker> owner)
            : Owner_(std::move(owner))
        { }

        TInvocationGuard(TInvocationGuard&& other) = default;
        TInvocationGuard(const TInvocationGuard& other) = delete;

        ~TInvocationGuard()
        {
            if (Owner_) {
                Owner_->OnFinished();
            }
        }

    private:
        TIntrusivePtr<TBoundedConcurrencyInvoker> Owner_;
    };

    void IncrementSemaphore(int delta)
    {
        YT_ASSERT_SPINLOCK_AFFINITY(SpinLock_);

        Semaphore_ += delta;
        YT_ASSERT(Semaphore_ >= 0 && Semaphore_ <= MaxConcurrentInvocations_ && (!PendingMaxConcurrentInvocations_.has_value() || delta <= 0));

        if (PendingMaxConcurrentInvocations_.has_value() && Semaphore_ <= *PendingMaxConcurrentInvocations_) {
            MaxConcurrentInvocations_ = *PendingMaxConcurrentInvocations_;
            PendingMaxConcurrentInvocations_ = {};
        }
    }

    void RunCallback(TClosure callback)
    {
        // If UnderlyingInvoker_ is already terminated, Invoke may drop the guard right away.
        // Protect by setting CurrentSchedulingInvoker_ and checking it on entering ScheduleMore.
        CurrentBoundedConcurrencyInvoker() = this;

        UnderlyingInvoker_->Invoke(BIND_NO_PROPAGATE(
            &TBoundedConcurrencyInvoker::DoRunCallback,
            MakeStrong(this),
            std::move(callback),
            Passed(TInvocationGuard(this))));

        // Don't leave a dangling pointer behind.
        CurrentBoundedConcurrencyInvoker() = nullptr;
    }

    void DoRunCallback(const TClosure& callback, TInvocationGuard /*invocationGuard*/)
    {
        TCurrentInvokerGuard guard(UnderlyingInvoker_.Get()); // sic!
        callback();
    }

    void OnFinished()
    {
        auto guard = Guard(SpinLock_);
        // See RunCallback.
        if (Queue_.empty() || CurrentBoundedConcurrencyInvoker() == this || PendingMaxConcurrentInvocations_.has_value()) {
            IncrementSemaphore(-1);
        } else {
            auto callback = std::move(Queue_.front());
            Queue_.pop();
            guard.Release();
            RunCallback(std::move(callback));
        }
    }
};

IBoundedConcurrencyInvokerPtr CreateBoundedConcurrencyInvoker(
    IInvokerPtr underlyingInvoker,
    int maxConcurrentInvocations)
{
    return New<TBoundedConcurrencyInvoker>(
        std::move(underlyingInvoker),
        maxConcurrentInvocations);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
