#include "fair_share_invoker_pool.h"

#include <yt/yt/core/actions/current_invoker.h>
#include <yt/yt/core/actions/invoker_detail.h>

#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/ring_queue.h>

#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/yt/memory/weak_ptr.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>
#include <library/cpp/yt/threading/spin_lock.h>

#include <optional>
#include <utility>

namespace NYT::NConcurrency {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

class TFairShareCallbackQueue
    : public IFairShareCallbackQueue
{
public:
    explicit TFairShareCallbackQueue(int bucketCount)
        : Buckets_(bucketCount)
        , ExcessTimes_(bucketCount, 0)
    { }

    void Enqueue(TClosure callback, int bucketIndex) override
    {
        auto guard = Guard(Lock_);

        YT_VERIFY(IsValidBucketIndex(bucketIndex));
        Buckets_[bucketIndex].push(std::move(callback));
    }

    bool TryDequeue(TClosure* resultCallback, int* resultBucketIndex) override
    {
        YT_VERIFY(resultCallback != nullptr);
        YT_VERIFY(resultBucketIndex != nullptr);

        auto guard = Guard(Lock_);

        auto optionalBucketIndex = GetStarvingBucketIndex();
        if (!optionalBucketIndex) {
            return false;
        }
        auto bucketIndex = *optionalBucketIndex;

        TruncateExcessTimes(ExcessTimes_[bucketIndex]);

        *resultCallback = std::move(Buckets_[bucketIndex].front());
        Buckets_[bucketIndex].pop();

        *resultBucketIndex = bucketIndex;

        return true;
    }

    void AccountCpuTime(int bucketIndex, TCpuDuration cpuTime) override
    {
        auto guard = Guard(Lock_);

        ExcessTimes_[bucketIndex] += cpuTime;
    }

private:
    using TBuckets = std::vector<TRingQueue<TClosure>>;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);

    TBuckets Buckets_;
    std::vector<TCpuDuration> ExcessTimes_;

    std::optional<int> GetStarvingBucketIndex() const
    {
        auto minExcessTime = std::numeric_limits<TCpuDuration>::max();
        std::optional<int> minBucketIndex;
        for (int index = 0; index < std::ssize(Buckets_); ++index) {
            if (Buckets_[index].empty()) {
                continue;
            }
            if (!minBucketIndex || ExcessTimes_[index] < minExcessTime) {
                minExcessTime = ExcessTimes_[index];
                minBucketIndex = index;
            }
        }
        return minBucketIndex;
    }

    void TruncateExcessTimes(TCpuDuration delta)
    {
        for (int index = 0; index < std::ssize(Buckets_); ++index) {
            if (ExcessTimes_[index] >= delta) {
                ExcessTimes_[index] -= delta;
            } else {
                ExcessTimes_[index] = 0;
            }
        }
    }

    bool IsValidBucketIndex(int index) const
    {
        return 0 <= index && index < std::ssize(Buckets_);
    }
};

////////////////////////////////////////////////////////////////////////////////

IFairShareCallbackQueuePtr CreateFairShareCallbackQueue(int bucketCount)
{
    YT_VERIFY(0 < bucketCount && bucketCount < 100);
    return New<TFairShareCallbackQueue>(bucketCount);
}

////////////////////////////////////////////////////////////////////////////////

class TFairShareInvokerPool
    : public TDiagnosableInvokerPool
{
public:
    TFairShareInvokerPool(
        IInvokerPtr underlyingInvoker,
        int invokerCount,
        TFairShareCallbackQueueFactory callbackQueueFactory,
        TDuration actionTimeRelevancyHalflife)
        : UnderlyingInvoker_(std::move(underlyingInvoker))
        , Queue_(callbackQueueFactory(invokerCount))
    {
        Invokers_.reserve(invokerCount);
        InvokerQueueStates_.reserve(invokerCount);
        for (int index = 0; index < invokerCount; ++index) {
            Invokers_.push_back(New<TInvoker>(UnderlyingInvoker_, index, MakeWeak(this)));
            InvokerQueueStates_.emplace_back(actionTimeRelevancyHalflife);
        }
    }

    void UpdateActionTimeRelevancyHalflife(TDuration newHalflife) override
    {
        auto guard = Guard(InvokerQueueStatesLock_);

        for (auto& queueState : InvokerQueueStates_) {
            queueState.UpdateActionTimeRelevancyHalflife(newHalflife);
        }
    }

    int GetSize() const override
    {
        return Invokers_.size();
    }

    void Enqueue(TClosure callback, int index)
    {
        {
            auto now = GetInstant();

            auto guard = Guard(InvokerQueueStatesLock_);

            auto& queueState = InvokerQueueStates_[index];
            queueState.OnActionEnqueued(now);
        }

        Queue_->Enqueue(std::move(callback), index);
        UnderlyingInvoker_->Invoke(BIND_NO_PROPAGATE(
            &TFairShareInvokerPool::Run,
            MakeStrong(this)));
    }

protected:
    const IInvokerPtr& DoGetInvoker(int index) const override
    {
        YT_VERIFY(IsValidInvokerIndex(index));
        return Invokers_[index];
    }

    TInvokerStatistics DoGetInvokerStatistics(int index) const override
    {
        YT_VERIFY(IsValidInvokerIndex(index));

        auto now = GetInstant();

        auto guard = Guard(InvokerQueueStatesLock_);

        const auto& queueState = InvokerQueueStates_[index];
        return queueState.GetInvokerStatistics(now);
    }

private:
    const IInvokerPtr UnderlyingInvoker_;

    std::vector<IInvokerPtr> Invokers_;

    class TInvokerQueueState
    {
    public:
        explicit TInvokerQueueState(
            TDuration halflife)
            : AverageTimeAggregator_(halflife)
        { }

        void OnActionEnqueued(TInstant now)
        {
            ActionEnqueueTimes_.push(now);
            ++EnqueuedActionCount_;
        }

        //! NB: We report action after execution and not after dequeue because
        //! we must take into account execution time for the following special case:
        //! We have enqueued just one action which freezes the invoker.
        //! If we ignore execution time, we will have a close to zero waiting time
        //! the next time we try to check if we should enqueue action.
        //! This will result in more actions stuck in queue than needed
        //! to determine whether or not invoker is frozen.
        void OnActionExecuted(TInstant now)
        {
            YT_VERIFY(!ActionEnqueueTimes_.empty());
            ++DequeuedActionCount_;

            auto totalWaitTime = now - ActionEnqueueTimes_.front();
            ActionEnqueueTimes_.pop();

            AverageTimeAggregator_.UpdateAt(now, totalWaitTime.MillisecondsFloat());
        }

        TInvokerStatistics GetInvokerStatistics(TInstant now) const
        {
            auto waitingActionCount = std::ssize(ActionEnqueueTimes_);

            return TInvokerStatistics{
                .EnqueuedActionCount = EnqueuedActionCount_,
                .DequeuedActionCount = DequeuedActionCount_,
                .WaitingActionCount = waitingActionCount,
                .TotalTimeEstimate = GetTotalTimeEstimate(now),
            };
        }

        void UpdateActionTimeRelevancyHalflife(TDuration newHalflife)
        {
            AverageTimeAggregator_.SetHalflife(newHalflife, /*resetOnNewHalflife*/ false);
        }

    private:
        TAdjustedExponentialMovingAverage AverageTimeAggregator_;
        TRingQueue<TInstant> ActionEnqueueTimes_;

        i64 EnqueuedActionCount_ = 0;
        i64 DequeuedActionCount_ = 0;

        TDuration GetTotalTimeEstimate(TInstant now) const
        {
            //! Invoker-accumulated average relevancy decays over time
            //! to account for the case when everything was fine
            //! and then invoker gets stuck for a very long time.
            auto maxWaitTime = GetMaxWaitTimeInQueue(now);
            auto totalWaitTimeMilliseconds = AverageTimeAggregator_.EstimateAverageWithNewValue(now, maxWaitTime.MillisecondsFloat());
            return TDuration::MilliSeconds(totalWaitTimeMilliseconds);
        }

        TDuration GetMaxWaitTimeInQueue(TInstant now) const
        {
            if (ActionEnqueueTimes_.empty()) {
                return TDuration::Zero();
            }

            return now - ActionEnqueueTimes_.front();
        }
    };

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, InvokerQueueStatesLock_);
    std::vector<TInvokerQueueState> InvokerQueueStates_;

    IFairShareCallbackQueuePtr Queue_;

    class TCpuTimeAccounter
    {
    public:
        TCpuTimeAccounter(int index, IFairShareCallbackQueue* queue)
            : Index_(index)
            , Queue_(queue)
            , ContextSwitchGuard_(
                /*out*/ [this] { Account(); },
                /*in*/ [] { })
        { }

        void Account()
        {
            if (Accounted_) {
                return;
            }
            Accounted_ = true;
            Queue_->AccountCpuTime(Index_, Timer_.GetElapsedCpuTime());
            Timer_.Stop();
        }

        ~TCpuTimeAccounter()
        {
            Account();
        }

    private:
        const int Index_;
        bool Accounted_ = false;
        IFairShareCallbackQueue* Queue_;
        TWallTimer Timer_;
        TContextSwitchGuard ContextSwitchGuard_;
    };

    class TInvoker
        : public TInvokerWrapper
    {
    public:
        TInvoker(IInvokerPtr underlyingInvoker_, int index, TWeakPtr<TFairShareInvokerPool> parent)
            : TInvokerWrapper(std::move(underlyingInvoker_))
            , Index_(index)
            , Parent_(std::move(parent))
        { }

        void Invoke(TClosure callback) override
        {
            if (auto strongParent = Parent_.Lock()) {
                strongParent->Enqueue(std::move(callback), Index_);
            }
        }

    private:
        const int Index_;
        const TWeakPtr<TFairShareInvokerPool> Parent_;
    };

    bool IsValidInvokerIndex(int index) const
    {
        return 0 <= index && index < std::ssize(Invokers_);
    }

    void Run()
    {
        TClosure callback;
        int bucketIndex = -1;
        YT_VERIFY(Queue_->TryDequeue(&callback, &bucketIndex));
        YT_VERIFY(IsValidInvokerIndex(bucketIndex));

        //! NB1: Finally causes us to count total time (wait + execution) in our statistics.
        //! This is done to compensate for the following situation:
        //! Consider the first task in the batch
        //! ("first" here means that invoker was not busy and the queue was empty during the enqueue call)
        //! It's wait time is always zero. Suppose execution time is T = 1 / EmaAlpha milliseconds
        //! Wait time for every task in the same batch after the first one would be at least T milliseconds.
        //! Unless aggregator completely disregards the first task in the batch,
        //! zero wait time can introduce a noticeable error to the average.
        //! If the rest of the batch executes instantly, actual average would be T milliseconds.
        //! Computed one would be T (1 - 1 / e) ~= 0.63T which is much less than what we would expect.

        //! NB2: We use Finally here instead of simply recording data after the execution is complete
        //! so that we dequeue task even if it threw an exception. Since we assume this invoker
        //! to be a wrapper around an arbitrary invoker which can be capable of catching exceptions
        //! and have some fallback strategy in case of a throwing callback we must ensure
        //! exception safety.
        auto cleanup = Finally([&] {
            auto now = GetInstant();
            auto guard = Guard(InvokerQueueStatesLock_);

            auto& queueState = InvokerQueueStates_[bucketIndex];
            queueState.OnActionExecuted(now);
        });

        {
            TCurrentInvokerGuard currentInvokerGuard(Invokers_[bucketIndex].Get());
            TCpuTimeAccounter cpuTimeAccounter(bucketIndex, Queue_.Get());
            callback();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TDiagnosableInvokerPoolPtr CreateFairShareInvokerPool(
    IInvokerPtr underlyingInvoker,
    int invokerCount,
    TFairShareCallbackQueueFactory callbackQueueFactory,
    TDuration actionTimeRelevancyHalflife)
{
    YT_VERIFY(0 < invokerCount && invokerCount < 100);
    return New<TFairShareInvokerPool>(
        std::move(underlyingInvoker),
        invokerCount,
        std::move(callbackQueueFactory),
        actionTimeRelevancyHalflife);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
