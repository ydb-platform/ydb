#include "two_level_fair_share_thread_pool.h"
#include "private.h"
#include "invoker_queue.h"
#include "profiling_helpers.h"
#include "scheduler_thread.h"
#include "thread_pool_detail.h"

#include <yt/yt/core/actions/current_invoker.h>

#include <yt/yt/core/misc/heap.h>
#include <yt/yt/core/misc/ring_queue.h>

#include <yt/yt/core/profiling/tscp.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/memory/weak_ptr.h>

#include <util/generic/xrange.h>

#include <util/system/yield.h>

namespace NYT::NConcurrency {

using namespace NProfiling;

static constexpr auto& Logger = ConcurrencyLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

struct THeapItem;
class TTwoLevelFairShareQueue;

DECLARE_REFCOUNTED_STRUCT(TBucket)

struct TBucket
    : public IInvoker
{
    TBucket(size_t poolId, TFairShareThreadPoolTag tag, TWeakPtr<TTwoLevelFairShareQueue> parent)
        : PoolId(poolId)
        , Tag(std::move(tag))
        , Parent(std::move(parent))
    { }

    void RunCallback(const TClosure& callback)
    {
        TCurrentInvokerGuard currentInvokerGuard(this);
        callback();
    }

    void Invoke(TClosure callback) override;

    void Invoke(TMutableRange<TClosure> callbacks) override;

    void Drain()
    {
        Queue.clear();
    }

    NThreading::TThreadId GetThreadId() const override
    {
        return NThreading::InvalidThreadId;
    }

    bool CheckAffinity(const IInvokerPtr& invoker) const override
    {
        return invoker.Get() == this;
    }

    bool IsSerialized() const override
    {
        return false;
    }

    void RegisterWaitTimeObserver(TWaitTimeObserver /*waitTimeObserver*/) override
    { }

    ~TBucket();

    const size_t PoolId;
    const TFairShareThreadPoolTag Tag;
    TWeakPtr<TTwoLevelFairShareQueue> Parent;
    TRingQueue<TEnqueuedAction> Queue;
    THeapItem* HeapIterator = nullptr;
    NProfiling::TCpuDuration WaitTime = 0;

    TCpuDuration ExcessTime = 0;
    int CurrentExecutions = 0;
};

DEFINE_REFCOUNTED_TYPE(TBucket)

struct THeapItem
{
    TBucketPtr Bucket;

    THeapItem(const THeapItem&) = delete;
    THeapItem& operator=(const THeapItem&) = delete;

    explicit THeapItem(TBucketPtr bucket)
        : Bucket(std::move(bucket))
    {
        AdjustBackReference(this);
    }

    THeapItem(THeapItem&& other) noexcept
        : Bucket(std::move(other.Bucket))
    {
        AdjustBackReference(this);
    }

    THeapItem& operator=(THeapItem&& other) noexcept
    {
        Bucket = std::move(other.Bucket);
        AdjustBackReference(this);

        return *this;
    }

    void AdjustBackReference(THeapItem* iterator)
    {
        if (Bucket) {
            Bucket->HeapIterator = iterator;
        }
    }

    ~THeapItem()
    {
        if (Bucket) {
            Bucket->HeapIterator = nullptr;
        }
    }
};

bool operator < (const THeapItem& lhs, const THeapItem& rhs)
{
    return lhs.Bucket->ExcessTime < rhs.Bucket->ExcessTime;
}

////////////////////////////////////////////////////////////////////////////////

static constexpr auto LogDurationThreshold = TDuration::Seconds(1);

DECLARE_REFCOUNTED_TYPE(TTwoLevelFairShareQueue)

class TTwoLevelFairShareQueue
    : public TRefCounted
{
public:
    using TWaitTimeObserver = ITwoLevelFairShareThreadPool::TWaitTimeObserver;

    TTwoLevelFairShareQueue(
        TIntrusivePtr<NThreading::TEventCount> callbackEventCount,
        const TString& threadNamePrefix,
        IPoolWeightProviderPtr poolWeightProvider)
        : CallbackEventCount_(std::move(callbackEventCount))
        , ThreadNamePrefix_(threadNamePrefix)
        , Profiler_(TProfiler("/fair_share_queue")
            .WithHot())
        , PoolWeightProvider_(std::move(poolWeightProvider))
    { }

    ~TTwoLevelFairShareQueue()
    {
        Shutdown();
    }

    void Configure(int threadCount)
    {
        ThreadCount_.store(threadCount);
    }

    IInvokerPtr GetInvoker(const TString& poolName, const TFairShareThreadPoolTag& tag)
    {
        while (true) {
            auto guard = Guard(SpinLock_);

            auto poolIt = NameToPoolId_.find(poolName);
            if (poolIt == NameToPoolId_.end()) {
                auto newPoolId = GetLowestEmptyPoolId();

                auto profiler = Profiler_.WithTags(GetBucketTags(ThreadNamePrefix_, poolName));
                auto newPool = std::make_unique<TExecutionPool>(poolName, profiler);
                if (newPoolId >= IdToPool_.size()) {
                    IdToPool_.emplace_back();
                }
                IdToPool_[newPoolId] = std::move(newPool);
                poolIt = NameToPoolId_.emplace(poolName, newPoolId).first;
            }

            auto poolId = poolIt->second;
            const auto& pool = IdToPool_[poolId];
            if (PoolWeightProvider_) {
                pool->Weight = PoolWeightProvider_->GetWeight(poolName);
            }

            TBucketPtr bucket;
            auto bucketIt = pool->TagToBucket.find(tag);
            if (bucketIt == pool->TagToBucket.end()) {
                bucket = New<TBucket>(poolId, tag, MakeWeak(this));
                YT_VERIFY(pool->TagToBucket.emplace(tag, bucket.Get()).second);
                pool->BucketCounter.Update(pool->TagToBucket.size());
            } else {
                bucket = DangerousGetPtr<TBucket>(bucketIt->second);
                if (!bucket) {
                    // Bucket is already being destroyed; backoff and retry.
                    guard.Release();
                    ThreadYield();
                    continue;
                }
            }

            return bucket;
        }
    }

    void Invoke(TClosure callback, TBucket* bucket)
    {
        auto guard = Guard(SpinLock_);
        // See Shutdown method.
        if (Stopping_) {
            return;
        }
        const auto& pool = IdToPool_[bucket->PoolId];

        pool->SizeCounter.Record(++pool->Size);

        if (!bucket->HeapIterator) {
            // Otherwise ExcessTime will be recalculated in AccountCurrentlyExecutingBuckets.
            if (bucket->CurrentExecutions == 0 && !pool->Heap.empty()) {
                bucket->ExcessTime = pool->Heap.front().Bucket->ExcessTime;
            }

            pool->Heap.emplace_back(bucket);
            AdjustHeapBack(pool->Heap.begin(), pool->Heap.end());
            YT_VERIFY(bucket->HeapIterator);
        }

        YT_ASSERT(callback);

        TEnqueuedAction action;
        action.Finished = false;
        action.EnqueuedAt = GetCpuInstant();
        action.Callback = BIND(&TBucket::RunCallback, MakeStrong(bucket), std::move(callback));
        bucket->Queue.push(std::move(action));

        guard.Release();

        CallbackEventCount_->NotifyOne();
    }

    void RemoveBucket(TBucket* bucket)
    {
        auto guard = Guard(SpinLock_);

        auto& pool = IdToPool_[bucket->PoolId];

        auto it = pool->TagToBucket.find(bucket->Tag);
        YT_VERIFY(it != pool->TagToBucket.end());
        YT_VERIFY(it->second == bucket);
        pool->TagToBucket.erase(it);
        pool->BucketCounter.Update(pool->TagToBucket.size());

        if (pool->TagToBucket.empty()) {
            YT_VERIFY(NameToPoolId_.erase(pool->PoolName) == 1);
            pool.reset();
        }
    }

    void Shutdown()
    {
        auto guard = Guard(SpinLock_);
        // NB(arkady-e1ppa): We write/read value under spinlock
        // instead of atomic in order to ensure that in Invoke method
        // we either observe |false| and enqueue callback
        // which will be drained here or we observe |true|
        // and not enqueue callback at all.
        Stopping_ = true;

        for (const auto& pool : IdToPool_) {
            if (pool) {
                for (const auto& item : pool->Heap) {
                    item.Bucket->Drain();
                }
            }
        }
    }

    bool BeginExecute(TEnqueuedAction* action, int index)
    {
        auto& threadState = ThreadStates_[index];

        YT_ASSERT(!threadState.Bucket);
        YT_ASSERT(action && action->Finished);

        auto currentInstant = GetCpuInstant();

        TBucketPtr bucket;
        TWaitTimeObserver waitTimeObserver;

        {
            auto guard = Guard(SpinLock_);
            bucket = GetStarvingBucket(action);
            waitTimeObserver = WaitTimeObserver_;

            if (!bucket) {
                return false;
            }

            ++bucket->CurrentExecutions;

            threadState.Bucket = bucket;
            threadState.AccountedAt = currentInstant;

            action->StartedAt = currentInstant;
            bucket->WaitTime = action->StartedAt - action->EnqueuedAt;
        }

        if (waitTimeObserver) {
            waitTimeObserver(CpuDurationToDuration(action->StartedAt - action->EnqueuedAt));
        }

        YT_ASSERT(action && !action->Finished);

        {
            auto guard = Guard(SpinLock_);
            auto& pool = IdToPool_[bucket->PoolId];

            pool->WaitTimeCounter.Record(CpuDurationToDuration(bucket->WaitTime));
        }

        return true;
    }

    void EndExecute(TEnqueuedAction* action, int index)
    {
        auto& threadState = ThreadStates_[index];
        if (!threadState.Bucket) {
            return;
        }

        YT_ASSERT(action);

        if (action->Finished) {
            return;
        }

        auto currentInstant = GetCpuInstant();

        action->FinishedAt = currentInstant;

        auto timeFromStart = CpuDurationToDuration(action->FinishedAt - action->StartedAt);
        auto timeFromEnqueue = CpuDurationToDuration(action->FinishedAt - action->EnqueuedAt);

        {
            auto guard = Guard(SpinLock_);
            const auto& pool = IdToPool_[threadState.Bucket->PoolId];
            pool->SizeCounter.Record(--pool->Size);
            pool->ExecTimeCounter.Record(timeFromStart);
            pool->TotalTimeCounter.Record(timeFromEnqueue);
        }

        if (timeFromStart > LogDurationThreshold) {
            YT_LOG_DEBUG("Callback execution took too long (Wait: %v, Execution: %v, Total: %v)",
                CpuDurationToDuration(action->StartedAt - action->EnqueuedAt),
                timeFromStart,
                timeFromEnqueue);
        }

        auto waitTime = CpuDurationToDuration(action->StartedAt - action->EnqueuedAt);

        if (waitTime > LogDurationThreshold) {
            YT_LOG_DEBUG("Callback wait took too long (Wait: %v, Execution: %v, Total: %v)",
                waitTime,
                timeFromStart,
                timeFromEnqueue);
        }

        action->Finished = true;

        // Remove outside lock because of lock inside RemoveBucket.
        TBucketPtr bucket;
        {
            auto guard = Guard(SpinLock_);
            bucket = std::move(threadState.Bucket);

            UpdateExcessTime(bucket.Get(), currentInstant - threadState.AccountedAt);
            threadState.AccountedAt = currentInstant;

            YT_VERIFY(bucket->CurrentExecutions-- > 0);
        }
    }

    void RegisterWaitTimeObserver(TWaitTimeObserver waitTimeObserver)
    {
        auto guard = Guard(SpinLock_);
        WaitTimeObserver_ = waitTimeObserver;
    }

private:
    struct TThreadState
    {
        TCpuInstant AccountedAt = 0;
        TBucketPtr Bucket;
    };

    struct TExecutionPool
    {
        TExecutionPool(const TString& poolName, const TProfiler& profiler)
            : PoolName(poolName)
            , BucketCounter(profiler.Gauge("/buckets"))
            , SizeCounter(profiler.Summary("/size"))
            , WaitTimeCounter(profiler.Timer("/time/wait"))
            , ExecTimeCounter(profiler.Timer("/time/exec"))
            , TotalTimeCounter(profiler.Timer("/time/total"))
        { }

        TBucketPtr GetStarvingBucket(TEnqueuedAction* action)
        {
            if (!Heap.empty()) {
                auto bucket = Heap.front().Bucket;
                YT_VERIFY(!bucket->Queue.empty());
                *action = std::move(bucket->Queue.front());
                bucket->Queue.pop();

                if (bucket->Queue.empty()) {
                    ExtractHeap(Heap.begin(), Heap.end());
                    Heap.pop_back();
                }

                return bucket;
            }

            return nullptr;
        }

        const TString PoolName;

        TGauge BucketCounter;
        std::atomic<i64> Size = 0;
        NProfiling::TSummary SizeCounter;
        TEventTimer WaitTimeCounter;
        TEventTimer ExecTimeCounter;
        TEventTimer TotalTimeCounter;

        double Weight = 1.0;

        TCpuDuration ExcessTime = 0;
        std::vector<THeapItem> Heap;
        THashMap<TFairShareThreadPoolTag, TBucket*> TagToBucket;
    };

    const TIntrusivePtr<NThreading::TEventCount> CallbackEventCount_;
    const TString ThreadNamePrefix_;
    const TProfiler Profiler_;
    const IPoolWeightProviderPtr PoolWeightProvider_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    // NB: We set this flag to true so that whomever may spam tasks to queue
    // will stop doing so after the shutdown.
    bool Stopping_ = false;
    std::vector<std::unique_ptr<TExecutionPool>> IdToPool_;
    THashMap<TString, int> NameToPoolId_;

    std::atomic<int> ThreadCount_ = 0;
    std::array<TThreadState, TThreadPoolBase::MaxThreadCount> ThreadStates_;

    ITwoLevelFairShareThreadPool::TWaitTimeObserver WaitTimeObserver_;


    size_t GetLowestEmptyPoolId()
    {
        VERIFY_SPINLOCK_AFFINITY(SpinLock_);

        size_t id = 0;
        while (id < IdToPool_.size() && IdToPool_[id]) {
            ++id;
        }
        return id;
    }

    void AccountCurrentlyExecutingBuckets()
    {
        VERIFY_SPINLOCK_AFFINITY(SpinLock_);

        auto currentInstant = GetCpuInstant();
        auto threadCount = ThreadCount_.load();
        for (int index = 0; index < threadCount; ++index) {
            auto& threadState = ThreadStates_[index];
            if (!threadState.Bucket) {
                continue;
            }

            auto duration = currentInstant - threadState.AccountedAt;
            threadState.AccountedAt = currentInstant;

            UpdateExcessTime(threadState.Bucket.Get(), duration);
        }
    }

    void UpdateExcessTime(TBucket* bucket, TCpuDuration duration)
    {
        VERIFY_SPINLOCK_AFFINITY(SpinLock_);

        const auto& pool = IdToPool_[bucket->PoolId];

        pool->ExcessTime += duration / pool->Weight;
        bucket->ExcessTime += duration;

        auto positionInHeap = bucket->HeapIterator;
        if (!positionInHeap) {
            return;
        }

        size_t indexInHeap = positionInHeap - pool->Heap.data();
        YT_VERIFY(indexInHeap < pool->Heap.size());
        SiftDown(pool->Heap.begin(), pool->Heap.end(), pool->Heap.begin() + indexInHeap, std::less<>());
    }

    TBucketPtr GetStarvingBucket(TEnqueuedAction* action)
    {
        VERIFY_SPINLOCK_AFFINITY(SpinLock_);

        // For each currently evaluating buckets recalculate excess time.
        AccountCurrentlyExecutingBuckets();

        // Compute min excess over non-empty queues.
        auto minExcessTime = std::numeric_limits<NProfiling::TCpuDuration>::max();

        int minPoolIndex = -1;
        for (int index = 0; index < static_cast<int>(IdToPool_.size()); ++index) {
            const auto& pool = IdToPool_[index];
            if (pool && !pool->Heap.empty() && pool->ExcessTime < minExcessTime) {
                minExcessTime = pool->ExcessTime;
                minPoolIndex = index;
            }
        }

        YT_LOG_TRACE(
            "Buckets: %v",
            MakeFormattableView(
                xrange(size_t(0), IdToPool_.size()),
                [&] (auto* builder, auto index) {
                    const auto& pool = IdToPool_[index];
                    if (!pool) {
                        builder->AppendString("<null>");
                        return;
                    }
                    builder->AppendFormat("[%v %v ", index, pool->ExcessTime);
                    for (const auto& [tagId, rawBucket] : pool->TagToBucket) {
                        if (auto bucket = DangerousGetPtr<TBucket>(rawBucket)) {
                            auto excess = CpuDurationToDuration(bucket->ExcessTime).MilliSeconds();
                            builder->AppendFormat("(%v %v) ", tagId, excess);
                        } else {
                            builder->AppendFormat("(%v ?) ", tagId);
                        }
                    }
                    builder->AppendFormat("]");
                }));

        if (minPoolIndex >= 0) {
            // Reduce excesses (with truncation).
            auto delta = IdToPool_[minPoolIndex]->ExcessTime;
            for (const auto& pool : IdToPool_) {
                if (pool) {
                    pool->ExcessTime = std::max<NProfiling::TCpuDuration>(pool->ExcessTime - delta, 0);
                }
            }
            return IdToPool_[minPoolIndex]->GetStarvingBucket(action);
        }

        return nullptr;
    }
};

DEFINE_REFCOUNTED_TYPE(TTwoLevelFairShareQueue)

////////////////////////////////////////////////////////////////////////////////

void TBucket::Invoke(TClosure callback)
{
    if (auto parent = Parent.Lock()) {
        parent->Invoke(std::move(callback), this);
    }
}

void TBucket::Invoke(TMutableRange<TClosure> callbacks)
{
    if (auto parent = Parent.Lock()) {
        for (auto& callback : callbacks) {
            parent->Invoke(std::move(callback), this);
        }
    }
}

TBucket::~TBucket()
{
    if (auto parent = Parent.Lock()) {
        parent->RemoveBucket(this);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TFairShareThread
    : public TSchedulerThread
{
public:
    TFairShareThread(
        TTwoLevelFairShareQueuePtr queue,
        TIntrusivePtr<NThreading::TEventCount> callbackEventCount,
        const TString& threadGroupName,
        const TString& threadName,
        int index)
        : TSchedulerThread(
            std::move(callbackEventCount),
            threadGroupName,
            threadName)
        , Queue_(std::move(queue))
        , Index_(index)
    { }

protected:
    const TTwoLevelFairShareQueuePtr Queue_;
    const int Index_;

    TEnqueuedAction CurrentAction_;

    TClosure BeginExecute() override
    {
        return BeginExecuteImpl(Queue_->BeginExecute(&CurrentAction_, Index_), &CurrentAction_);
    }

    void EndExecute() override
    {
        Queue_->EndExecute(&CurrentAction_, Index_);
    }
};

DEFINE_REFCOUNTED_TYPE(TFairShareThread)

////////////////////////////////////////////////////////////////////////////////

class TTwoLevelFairShareThreadPool
    : public ITwoLevelFairShareThreadPool
    , public TThreadPoolBase
{
public:
    TTwoLevelFairShareThreadPool(
        int threadCount,
        const TString& threadNamePrefix,
        IPoolWeightProviderPtr poolWeightProvider)
        : TThreadPoolBase(threadNamePrefix)
        , Queue_(New<TTwoLevelFairShareQueue>(
            CallbackEventCount_,
            ThreadNamePrefix_,
            std::move(poolWeightProvider)))
    {
        Configure(threadCount);
        EnsureStarted();
    }

    ~TTwoLevelFairShareThreadPool()
    {
        Shutdown();
    }

    void Configure(int threadCount) override
    {
        TThreadPoolBase::Configure(threadCount);
    }

    void Configure(TDuration /*pollingPeriod*/) override
    { }

    int GetThreadCount() override
    {
        return TThreadPoolBase::GetThreadCount();
    }

    IInvokerPtr GetInvoker(
        const TString& poolName,
        const TFairShareThreadPoolTag& tag) override
    {
        EnsureStarted();
        return Queue_->GetInvoker(poolName, tag);
    }

    void Shutdown() override
    {
        TThreadPoolBase::Shutdown();
    }

    void RegisterWaitTimeObserver(TWaitTimeObserver waitTimeObserver) override
    {
        Queue_->RegisterWaitTimeObserver(std::move(waitTimeObserver));
    }

private:
    const TIntrusivePtr<NThreading::TEventCount> CallbackEventCount_ = New<NThreading::TEventCount>();
    const TTwoLevelFairShareQueuePtr Queue_;


    void DoShutdown() override
    {
        Queue_->Shutdown();
        TThreadPoolBase::DoShutdown();
    }

    void DoConfigure(int threadCount) override
    {
        Queue_->Configure(threadCount);
        TThreadPoolBase::DoConfigure(threadCount);
    }

    TSchedulerThreadPtr SpawnThread(int index) override
    {
        return New<TFairShareThread>(
            Queue_,
            CallbackEventCount_,
            ThreadNamePrefix_,
            MakeThreadName(index),
            index);
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

ITwoLevelFairShareThreadPoolPtr CreateTwoLevelFairShareThreadPool(
    int threadCount,
    const TString& threadNamePrefix,
    IPoolWeightProviderPtr poolWeightProvider)
{
    return New<TTwoLevelFairShareThreadPool>(
        threadCount,
        threadNamePrefix,
        std::move(poolWeightProvider));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
