#include "fair_share_invoker_pool.h"
#include "profiling_helpers.h"

#include <yt/yt/core/actions/current_invoker.h>
#include <yt/yt/core/actions/invoker_detail.h>

#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/ring_queue.h>

#include <yt/yt/core/profiling/public.h>
#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/library/ytprof/api/api.h>

#include <library/cpp/yt/misc/port.h>

#include <library/cpp/yt/memory/weak_ptr.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>
#include <library/cpp/yt/threading/spin_lock.h>

#include <optional>
#include <utility>

namespace NYT::NConcurrency {

using namespace NProfiling;
using namespace NYTProf;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_THREAD_LOCAL(TCpuProfilerTagGuard, FairShareInvokerPoolProfilerTagGuard);

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

template <bool EnableProfiling>
class TFairShareInvokerPoolProfiler
{
public:
    using TObject = TFairShareInvokerPoolProfiler;

    class THandle
    {
    public:
        void ProfileEnqueue()
        { }

        void ProfileDequeue(TDuration /*waitTime*/)
        { }

        void ProfileExecutionFinish(TDuration /*execTime*/, TDuration /*totalTime*/)
        { }
    };

    static TObject Create(
        const TString& /*poolName*/,
        std::vector<TString> /*bucketNames*/,
        IRegistryImplPtr /*registry*/)
    {
        return {};
    }

    static THandle MakeHandle(const TObject& /*self*/, int /*handleIndex*/)
    {
        return THandle();
    }
};

template <>
class TFairShareInvokerPoolProfiler<true>
    : public TRefCounted
{
public:
    using TObject = TIntrusivePtr<TFairShareInvokerPoolProfiler>;

    class THandle
    {
    public:
        void ProfileEnqueue()
        {
            Profiler_->ProfileEnqueue(HandleIndex_);
        }

        void ProfileDequeue(TDuration waitTime)
        {
            Profiler_->ProfileDequeue(HandleIndex_, waitTime);
        }

        void ProfileExecutionFinish(TDuration execTime, TDuration totalTime)
        {
            Profiler_->ProfileExecutionFinish(HandleIndex_, execTime, totalTime);
        }

    private:
        friend class TFairShareInvokerPoolProfiler;

        TObject Profiler_;
        int HandleIndex_;

        THandle(TObject profiler, int handleIndex)
            : Profiler_(std::move(profiler))
            , HandleIndex_(handleIndex)
        { }
    };

    static TObject Create(
        const TString& poolName,
        std::vector<TString> bucketNames,
        IRegistryImplPtr registry)
    {
        return New<TFairShareInvokerPoolProfiler>(poolName, std::move(bucketNames), std::move(registry));
    }

    static THandle MakeHandle(const TObject& self, int handleIndex)
    {
        return THandle(self, handleIndex);
    }

private:
    friend class THandle;

    DECLARE_NEW_FRIEND();

    std::vector<TProfilerTagPtr> BucketProfilerTags_;

    struct TCounters
    {
        NProfiling::TCounter EnqueuedCounter;
        NProfiling::TCounter DequeuedCounter;
        NProfiling::TEventTimer WaitTimer;
        NProfiling::TEventTimer ExecTimer;
        NProfiling::TTimeCounter CumulativeTimeCounter;
        NProfiling::TEventTimer TotalTimer;
        std::atomic<int> ActiveCallbacks = 0;
    };

    using TCountersPtr = std::unique_ptr<TCounters>;

    std::vector<TCountersPtr> Counters_;

    TFairShareInvokerPoolProfiler(
        const TString& poolName,
        std::vector<TString> bucketNames,
        IRegistryImplPtr registry)
    {
        Counters_.reserve(std::ssize(bucketNames));
        BucketProfilerTags_.reserve(std::ssize(bucketNames));

        for (const auto& bucketName : bucketNames) {
            Counters_.push_back(CreateCounters(GetBucketTags(poolName, bucketName), registry));
            BucketProfilerTags_.push_back(New<TProfilerTag>("bucket", bucketName));
        }
    }

    TCountersPtr CreateCounters(const TTagSet& tagSet, const IRegistryImplPtr& registry) {
        auto profiler = TProfiler(registry, "/fair_share_invoker_pool").WithTags(tagSet).WithHot();

        auto counters = std::make_unique<TCounters>();
        counters->EnqueuedCounter = profiler.Counter("/enqueued");
        counters->DequeuedCounter = profiler.Counter("/dequeued");
        counters->WaitTimer = profiler.Timer("/time/wait");
        counters->ExecTimer = profiler.Timer("/time/exec");
        counters->CumulativeTimeCounter = profiler.TimeCounter("/time/cumulative");
        counters->TotalTimer = profiler.Timer("/time/total");

        profiler.AddFuncGauge(
            "/size",
            MakeStrong(this),
            [counters = counters.get()] {
                return counters->ActiveCallbacks.load(std::memory_order::relaxed);
            });

        return counters;
    }

    void ProfileEnqueue(int index)
    {
        auto& counters = Counters_[index];
        if (counters) {
            counters->ActiveCallbacks.fetch_add(1, std::memory_order::relaxed);
            counters->EnqueuedCounter.Increment();
        }
    }

    void ProfileDequeue(int index, TDuration waitTime)
    {
        auto& counters = Counters_[index];
        if (counters) {
            counters->DequeuedCounter.Increment();
            counters->WaitTimer.Record(waitTime);
        }

        FairShareInvokerPoolProfilerTagGuard() = TCpuProfilerTagGuard(BucketProfilerTags_[index]);
    }

    void ProfileExecutionFinish(int index, TDuration execTime, TDuration totalTime)
    {
        FairShareInvokerPoolProfilerTagGuard() = TCpuProfilerTagGuard{};

        auto& counters = Counters_[index];
        if (counters) {
            counters->ExecTimer.Record(execTime);
            counters->CumulativeTimeCounter.Add(execTime);
            counters->TotalTimer.Record(totalTime);
            counters->ActiveCallbacks.fetch_sub(1, std::memory_order::relaxed);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

template <bool EnableProfiling>
class TFairShareInvokerPool
    : public TDiagnosableInvokerPool
{
    using TPoolProfiler = TFairShareInvokerPoolProfiler<EnableProfiling>;
    using TPoolProfilerObject = typename TPoolProfiler::TObject;

public:
    TFairShareInvokerPool(
        IInvokerPtr underlyingInvoker,
        int invokerCount,
        TFairShareCallbackQueueFactory callbackQueueFactory,
        TDuration actionTimeRelevancyHalflife,
        const TString& poolName = "",
        std::vector<TString> bucketNames = {},
        IRegistryImplPtr registry = nullptr)
        : UnderlyingInvoker_(std::move(underlyingInvoker))
        , Queue_(callbackQueueFactory(invokerCount))
        , Profiler_(TPoolProfiler::Create(poolName, std::move(bucketNames), std::move(registry)))
    {
        Invokers_.reserve(invokerCount);
        InvokerQueueStates_.reserve(invokerCount);
        for (int index = 0; index < invokerCount; ++index) {
            Invokers_.push_back(New<TInvoker>(UnderlyingInvoker_, index, MakeWeak(this)));
            InvokerQueueStates_.emplace_back(actionTimeRelevancyHalflife, TPoolProfiler::MakeHandle(Profiler_, index));
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
        using THandle = typename TPoolProfiler::THandle;

    public:
        explicit TInvokerQueueState(
            TDuration halflife,
            THandle profilerHandle)
            : AverageTimeAggregator_(halflife)
            , ProfilerHandle_(std::move(profilerHandle))
        { }

        void OnActionEnqueued(TInstant now)
        {
            ActionEnqueueTimes_.push(now);
            ++EnqueuedActionCount_;
            ProfilerHandle_.ProfileEnqueue();
        }

        //! We do not remove any enqueue times now because we
        //! enjoy invariant ActionEnqueueTimes_.empty() iff
        //! no action running.
        void OnActionDequeued(TInstant now)
        {
            YT_VERIFY(!ActionEnqueueTimes_.empty());
            ++DequeuedActionCount_;

            auto waitTime = now - ActionEnqueueTimes_.front();
            ProfilerHandle_.ProfileDequeue(waitTime);
        }

        //! NB: We report action after execution and not after dequeue because
        //! we must take into account execution time for the following special case:
        //! We have enqueued just one action which freezes the invoker.
        //! If we ignore execution time, we will have a close to zero waiting time
        //! the next time we try to check if we should enqueue action.
        //! This will result in more actions stuck in queue than needed
        //! to determine whether or not invoker is frozen.
        void OnActionExecuted(TInstant now, TInstant dequeueTime)
        {
            YT_VERIFY(!ActionEnqueueTimes_.empty());
            ++ExecutedActionCount_;

            auto execTime = now - dequeueTime;
            auto totalTime = now - ActionEnqueueTimes_.front();
            ActionEnqueueTimes_.pop();

            AverageTimeAggregator_.UpdateAt(now, totalTime.MillisecondsFloat());
            ProfilerHandle_.ProfileExecutionFinish(execTime, totalTime);
        }

        TInvokerStatistics GetInvokerStatistics(TInstant now) const
        {
            auto waitingActionCount = std::ssize(ActionEnqueueTimes_);

            return TInvokerStatistics{
                .EnqueuedActionCount = EnqueuedActionCount_,
                .DequeuedActionCount = DequeuedActionCount_,
                .ExecutedActionCount = ExecutedActionCount_,
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
        i64 ExecutedActionCount_ = 0;

        YT_ATTRIBUTE_NO_UNIQUE_ADDRESS THandle ProfilerHandle_;

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

    YT_ATTRIBUTE_NO_UNIQUE_ADDRESS TPoolProfilerObject Profiler_;

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

        TInstant dequeueTime = GetInstant();

        if constexpr (EnableProfiling) {
            auto guard = Guard(InvokerQueueStatesLock_);
            auto& queueState = InvokerQueueStates_[bucketIndex];
            queueState.OnActionDequeued(dequeueTime);
        }

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
            queueState.OnActionExecuted(now, dequeueTime);
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
    return New<TFairShareInvokerPool</*EnableProfiling*/ false>>(
        std::move(underlyingInvoker),
        invokerCount,
        std::move(callbackQueueFactory),
        actionTimeRelevancyHalflife);
}

////////////////////////////////////////////////////////////////////////////////

TDiagnosableInvokerPoolPtr CreateProfiledFairShareInvokerPool(
    IInvokerPtr underlyingInvoker,
    TFairShareCallbackQueueFactory callbackQueueFactory,
    TDuration actionTimeRelevancyHalflife,
    const TString& poolName,
    std::vector<TString> bucketNames,
    IRegistryImplPtr registry)
{
    YT_VERIFY(0 < std::ssize(bucketNames) && std::ssize(bucketNames) < 100);

    return New<TFairShareInvokerPool</*EnableProfiling*/ true>>(
        std::move(underlyingInvoker),
        std::ssize(bucketNames),
        std::move(callbackQueueFactory),
        actionTimeRelevancyHalflife,
        poolName,
        std::move(bucketNames),
        std::move(registry));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
