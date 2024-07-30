#include "two_level_fair_share_thread_pool.h"
#include "new_fair_share_thread_pool.h"
#include "private.h"
#include "notify_manager.h"
#include "profiling_helpers.h"
#include "scheduler_thread.h"
#include "thread_pool_detail.h"

#include <yt/yt/core/actions/current_invoker.h>

#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/heap.h>
#include <yt/yt/core/misc/ring_queue.h>
#include <yt/yt/core/misc/mpsc_stack.h>
#include <yt/yt/core/misc/range_formatters.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/containers/intrusive_linked_list.h>

#include <library/cpp/yt/memory/public.h>

#include <library/cpp/yt/misc/tls.h>

#include <util/system/spinlock.h>

#include <util/generic/xrange.h>

namespace NYT::NConcurrency {

using namespace NProfiling;

YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "FairShareThreadPool");

////////////////////////////////////////////////////////////////////////////////

namespace {

DECLARE_REFCOUNTED_CLASS(TBucketMapping)
DECLARE_REFCOUNTED_CLASS(TTwoLevelFairShareQueue)
DECLARE_REFCOUNTED_CLASS(TBucket)

struct TExecutionPool;

// High 16 bits is thread index and 48 bits for thread pool ptr.
YT_DEFINE_THREAD_LOCAL(TPackedPtr, ThreadCookie, 0);

static constexpr auto LogDurationThreshold = TDuration::Seconds(1);

////////////////////////////////////////////////////////////////////////////////

template <class T>
class THeapItem
{
public:
    THeapItem(const THeapItem&) = delete;
    THeapItem& operator=(const THeapItem&) = delete;

    explicit THeapItem(T* ptr)
        : Ptr_(std::move(ptr))
    {
        AdjustBackReference();
    }

    ~THeapItem()
    {
        if (Ptr_) {
            YT_ASSERT(Ptr_->PositionInHeap_ == this);
            Ptr_->PositionInHeap_ = nullptr;
        }
    }

    T& operator* ()
    {
        return *Ptr_;
    }

    const T& operator*() const
    {
        return *Ptr_;
    }

    T* operator->()
    {
        return Ptr_;
    }

    const T* operator->() const
    {
        return Ptr_;
    }

    bool operator < (const THeapItem<T>& other) const
    {
        return *Ptr_ < *other;
    }

    THeapItem(THeapItem&& other) noexcept
        : Ptr_(std::move(other.Ptr_))
    {
        other.Ptr_ = nullptr;
        AdjustBackReference();
    }

    THeapItem& operator=(THeapItem&& other) noexcept
    {
        Ptr_ = std::move(other.Ptr_);
        other.Ptr_ = nullptr;
        AdjustBackReference();

        return *this;
    }

private:
    T* Ptr_;

    void AdjustBackReference()
    {
        if (Ptr_) {
            Ptr_->PositionInHeap_ = this;
        }
    }
};

template <class T>
class THeapItemBase
{
public:
    friend THeapItem<T>;

    THeapItem<T>* GetPositionInHeap() const
    {
        return PositionInHeap_;
    }

    ~THeapItemBase()
    {
        YT_ASSERT(!PositionInHeap_);
    }

private:
    THeapItem<T>* PositionInHeap_ = nullptr;
};

template <class T>
class TPriorityQueue
{
public:
    void Insert(T* object)
    {
        Items_.emplace_back(object);
        SiftUp(Items_.begin(), Items_.end(), Items_.end() - 1);
        YT_ASSERT(object->GetPositionInHeap());
    }

    void Extract(const T* object)
    {
        auto* positionInHeap = object->GetPositionInHeap();
        YT_ASSERT(Items_.data() <= positionInHeap && positionInHeap < Items_.data() + Items_.size());

        std::swap(*positionInHeap, Items_.back());
        SiftDown(
            Items_.data(),
            Items_.data() + std::ssize(Items_) - 1,
            positionInHeap);

        YT_ASSERT(Items_.back()->GetPositionInHeap() == object->GetPositionInHeap());
        Items_.pop_back();
    }

    void AdjustDown(const T* object)
    {
        auto* positionInHeap = object->GetPositionInHeap();
        YT_ASSERT(Items_.data() <= positionInHeap && positionInHeap < Items_.data() + Items_.size());
        SiftDown(
            Items_.data(),
            Items_.data() + std::ssize(Items_),
            positionInHeap);
    }

    T* GetFront()
    {
        YT_ASSERT(!Empty());
        return &*Items_.front();
    }

    bool Empty() const
    {
        return Items_.empty();
    }

    size_t GetSize() const
    {
        return Items_.size();
    }

    T& operator[] (size_t index)
    {
        return *Items_[index];
    }

    template <class F>
    void ForEach(F&& functor)
    {
        for (auto& item : Items_) {
            functor(&*item);
        }
    }

    void Clear()
    {
        Items_.clear();
    }

private:
    std::vector<THeapItem<T>> Items_;
};

////////////////////////////////////////////////////////////////////////////////

struct TAction
{
    TCpuInstant EnqueuedAt = 0;
    TCpuInstant StartedAt = 0;

    // Callback keeps raw ptr to bucket to minimize bucket ref count.
    TClosure Callback;
    TBucketPtr BucketHolder;

    TPackedPtr EnqueuedThreadCookie = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TEnqueuedTime
    : public THeapItemBase<TEnqueuedTime>
{
    TCpuInstant Value = 0;
};

bool operator < (const TEnqueuedTime& lhs, const TEnqueuedTime& rhs)
{
    return lhs.Value < rhs.Value;
}

////////////////////////////////////////////////////////////////////////////////

// Data for scheduling on the first level.
struct TExecutionPool final
    : public THeapItemBase<TExecutionPool>
{
    const TString PoolName;

    // Profiling sensors.
    const NProfiling::TSummary BucketCounter;
    const NProfiling::TSummary SizeCounter;
    const NProfiling::TCounter DequeuedCounter;
    const TEventTimer WaitTimeCounter;
    const TEventTimer ExecTimeCounter;
    const TEventTimer TotalTimeCounter;
    const NProfiling::TTimeCounter CumulativeTimeCounter;

    // Action count is used to decide whether to reset excess time or not.
    size_t ActionCountInQueue = 0;

    TCpuDuration NextUpdateWeightInstant = 0;
    double InverseWeight = 1.0;
    TCpuDuration ExcessTime = 0;

    TPriorityQueue<TBucket> ActiveBucketsHeap;
    TCpuDuration LastBucketExcessTime = 0;

    TIntrusiveLinkedListNode<TExecutionPool> LinkedListNode;
    // Execution pool is retained for some after last usage to flush profiling counters.
    TCpuInstant LastUsageTime = 0;

    TExecutionPool(TString poolName, const TProfiler& profiler)
        : PoolName(std::move(poolName))
        , BucketCounter(profiler.Summary("/buckets"))
        , SizeCounter(profiler.Summary("/size"))
        , DequeuedCounter(profiler.Counter("/dequeued"))
        , WaitTimeCounter(profiler.Timer("/time/wait"))
        , ExecTimeCounter(profiler.Timer("/time/exec"))
        , TotalTimeCounter(profiler.Timer("/time/total"))
        , CumulativeTimeCounter(profiler.TimeCounter("/time/cumulative"))
    { }
};

bool operator < (const TExecutionPool& lhs, const TExecutionPool& rhs)
{
    return lhs.ExcessTime < rhs.ExcessTime;
}

using TExecutionPoolPtr = ::NYT::TIntrusivePtr<TExecutionPool>;

////////////////////////////////////////////////////////////////////////////////

// Data for scheduling on the second level.
struct TBucketBase
{
    const TString BucketName;
    const TString PoolName;

    TRingQueue<TAction> ActionQueue;
    TExecutionPoolPtr Pool = nullptr;

    TCpuDuration ExcessTime = 0;

    TEnqueuedTime EnqueuedTime;

    TBucketBase(TString bucketName, TString poolName)
        : BucketName(std::move(bucketName))
        , PoolName(std::move(poolName))
    { }
};

bool operator < (const TBucketBase& lhs, const TBucketBase& rhs)
{
    return std::tie(lhs.ExcessTime, lhs.EnqueuedTime) < std::tie(rhs.ExcessTime, rhs.EnqueuedTime);
}

////////////////////////////////////////////////////////////////////////////////

class TBucket
    : public IInvoker
    , public THeapItemBase<TBucket>
    , public TBucketBase
{
public:
    TBucket(TString bucketName, TString poolName, TBucketMappingPtr parent)
        : TBucketBase(std::move(bucketName), std::move(poolName))
        , Parent_(std::move(parent))
    { }

    ~TBucket();

    void RunCallback(const TClosure& callback, TCpuInstant cpuInstant)
    {
        YT_LOG_TRACE("Executing callback (EnqueuedAt: %v)", cpuInstant);
        TCurrentInvokerGuard currentInvokerGuard(this);
        callback.Run();
    }

    bool IsSerialized() const override
    {
        return false;
    }

    void Invoke(TClosure callback) override;

    void Invoke(TMutableRange<TClosure> callbacks) override
    {
        for (auto& callback : callbacks) {
            Invoke(std::move(callback));
        }
    }

    NThreading::TThreadId GetThreadId() const override
    {
        return NThreading::InvalidThreadId;
    }

    bool CheckAffinity(const IInvokerPtr& invoker) const override
    {
        return invoker.Get() == this;
    }

    void RegisterWaitTimeObserver(TWaitTimeObserver /*waitTimeObserver*/) override
    { }

private:
    const TBucketMappingPtr Parent_;
};

DEFINE_REFCOUNTED_TYPE(TBucket)

////////////////////////////////////////////////////////////////////////////////

class TBucketMapping
    : public TRefCounted
{
public:
    struct TExecutionPoolToListNode
    {
        auto operator() (TExecutionPool* pool) const
        {
            return &pool->LinkedListNode;
        }
    };

    using TPoolQueue = TIntrusiveLinkedList<TExecutionPool, TExecutionPoolToListNode>;

    explicit TBucketMapping(TDuration poolRetentionTime)
        : PoolRetentionTime_(poolRetentionTime)
    { }

    ~TBucketMapping()
    {
        auto guard = Guard(MappingLock_);

        while (RetainPoolQueue_.GetSize() > 0) {
            auto* frontPool = RetainPoolQueue_.GetFront();

            auto poolIt = PoolMapping_.find(frontPool->PoolName);
            YT_ASSERT(poolIt != PoolMapping_.end() && poolIt->second == frontPool);
            PoolMapping_.erase(poolIt);

            RetainPoolQueue_.PopFront();
            NYT::DestroyRefCounted(frontPool);
        }
    }

    virtual TProfiler GetPoolProfiler(const TString& poolName) = 0;

    virtual void Invoke(TClosure callback, TBucket* bucket) = 0;

    // GetInvoker is protected by mapping lock (can be sharded).
    IInvokerPtr GetInvoker(const TString& poolName, const TString& bucketName)
    {
        // TODO(lukyan): Use reader guard and update it to writer if needed.
        auto guard = Guard(MappingLock_);

        auto [bucketIt, bucketInserted] = BucketMapping_.emplace(std::pair(poolName, bucketName), nullptr);

        auto bucket = bucketIt->second ? DangerousGetPtr(bucketIt->second) : nullptr;
        if (!bucket) {
            bucket = New<TBucket>(bucketName, poolName, MakeStrong(this));
            bucketIt->second = bucket.Get();
            bucket->Pool = GetOrRegisterPool(bucket->PoolName);
        }

        return bucket;
    }

    // GetInvoker is protected by mapping lock (can be sharded).
    void RemoveBucket(TBucket* bucket)
    {
        auto guard = Guard(MappingLock_);
        auto bucketIt = BucketMapping_.find(std::pair(bucket->PoolName, bucket->BucketName));

        if (bucketIt != BucketMapping_.end() && bucketIt->second == bucket) {
            BucketMapping_.erase(bucketIt);
        }

        // Detach under lock.
        auto* poolDangerousPtr = bucket->Pool.Release();

        // Do not want use NewWithDeleter and keep pointer to TTwoLevelFairShareQueue in each execution pool.
        if (NYT::GetRefCounter(poolDangerousPtr)->Unref(1)) {
            auto poolsToRemove = DetachPool(poolDangerousPtr);
            guard.Release();

            while (poolsToRemove.GetSize() > 0) {
                auto* frontPool = poolsToRemove.GetFront();
                poolsToRemove.PopFront();
                NYT::DestroyRefCounted(frontPool);
            }
        }
    }

    TExecutionPoolPtr GetOrRegisterPool(TString poolName)
    {
        VERIFY_SPINLOCK_AFFINITY(MappingLock_);

        auto [mappingIt, inserted] = PoolMapping_.emplace(poolName, nullptr);
        if (!inserted) {
            YT_ASSERT(mappingIt->second->PoolName == poolName);

            auto* pool = mappingIt->second;
            // If RetainPoolQueue_ contains only one element its LinkedListNode will be null.
            // Determine that pool is in RetainPoolQueue_ by checking its ref count.
            if (NYT::GetRefCounter(pool)->GetRefCount() == 0) {
                RetainPoolQueue_.Remove(pool);
                pool->LinkedListNode = {};

                YT_LOG_TRACE("Restoring pool (PoolName: %v)", pool->PoolName);
            }

            YT_LOG_TRACE("Reusing pool (PoolName: %v)", pool->PoolName);

            return pool;
        } else {
            YT_LOG_TRACE("Creating pool (PoolName: %v)", poolName);
            auto pool = New<TExecutionPool>(poolName, GetPoolProfiler(poolName));
            mappingIt->second = pool.Get();

            return pool;
        }
    }

    TPoolQueue DetachPool(TExecutionPool* pool)
    {
        VERIFY_SPINLOCK_AFFINITY(MappingLock_);

        YT_LOG_TRACE("Removing pool (PoolName: %v)", pool->PoolName);

        auto currentInstant = GetCpuInstant();
        pool->LastUsageTime = currentInstant;

        // Items in RetainPoolQueue_ are ordered by LastUsageTime.
        // When pool is used again it is removed from RetainPoolQueue_.
        RetainPoolQueue_.PushBack(pool);
        return ProceedRetainQueue(currentInstant);
    }

    TPoolQueue ProceedRetainQueue(TCpuInstant currentInstant)
    {
        VERIFY_SPINLOCK_AFFINITY(MappingLock_);

        YT_LOG_TRACE("ProceedRetainQueue (Size: %v)", RetainPoolQueue_.GetSize());

        TPoolQueue poolsToRemove;

        while (RetainPoolQueue_.GetSize() > 0) {
            auto* frontPool = RetainPoolQueue_.GetFront();

            auto lastUsageTime = frontPool->LastUsageTime;
            if (CpuDurationToDuration(currentInstant - lastUsageTime) < PoolRetentionTime_) {
                break;
            }

            YT_LOG_TRACE("Destroing pool (PoolName: %v)", frontPool->PoolName);

            auto poolIt = PoolMapping_.find(frontPool->PoolName);
            YT_ASSERT(poolIt != PoolMapping_.end() && poolIt->second == frontPool);
            PoolMapping_.erase(poolIt);

            RetainPoolQueue_.PopFront();
            poolsToRemove.PushBack(frontPool);
        }

        return poolsToRemove;
    }

    void MaybeProceedRetainQueue(TCpuInstant currentInstant)
    {
        if (!MappingLock_.TryAcquire()) {
            return;
        }

        auto finally = Finally([&] {
            MappingLock_.Release();
        });

        auto poolsToRemove = ProceedRetainQueue(currentInstant);
        MappingLock_.Release();
        finally.Release();

        while (poolsToRemove.GetSize() > 0) {
            auto* frontPool = poolsToRemove.GetFront();
            poolsToRemove.PopFront();
            NYT::DestroyRefCounted(frontPool);
        }
    }

private:
    const TDuration PoolRetentionTime_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, MappingLock_);
    THashMap<std::pair<TString, TString>, TBucket*> BucketMapping_;
    THashMap<TString, TExecutionPool*> PoolMapping_;

    TPoolQueue RetainPoolQueue_;
};

DEFINE_REFCOUNTED_TYPE(TBucketMapping)

////////////////////////////////////////////////////////////////////////////////

void TBucket::Invoke(TClosure callback)
{
    Parent_->Invoke(std::move(callback), this);
}

TBucket::~TBucket()
{
    Parent_->RemoveBucket(this);
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ERequest,
    (None)
    (EndExecute)
    (FetchNext)
);

class TTwoLevelFairShareQueue
    : protected TNotifyManager
    , public TBucketMapping
{
public:
    using TWaitTimeObserver = ITwoLevelFairShareThreadPool::TWaitTimeObserver;

    TTwoLevelFairShareQueue(
        TIntrusivePtr<NThreading::TEventCount> callbackEventCount,
        const TString& threadNamePrefix,
        const TNewTwoLevelFairShareThreadPoolOptions& options)
        : TNotifyManager(std::move(callbackEventCount), GetThreadTags(threadNamePrefix), options.PollingPeriod)
        , TBucketMapping(options.PoolRetentionTime)
        , ThreadNamePrefix_(threadNamePrefix)
        , Profiler_(TProfiler{"/fair_share_queue"}
            .WithHot())
        , CumulativeSchedulingTimeCounter_(Profiler_
            .WithTags(GetThreadTags(ThreadNamePrefix_))
            .TimeCounter("/time/scheduling_cumulative"))
        , PoolWeightProvider_(options.PoolWeightProvider)
        , VerboseLogging_(options.VerboseLogging)
    { }

    ~TTwoLevelFairShareQueue()
    {
        Shutdown();
    }

    void Configure(int threadCount)
    {
        ThreadCount_.store(threadCount);
    }

    void Configure(TDuration pollingPeriod)
    {
        TNotifyManager::Reconfigure(pollingPeriod);
    }

    // (arkady-e1ppa): Explanation of memory orders and fences around Stopped_:
    /*
        We have two concurrent actions: Invoke and Shutdown.
        For our logic Invoke method does 2 things:
        1) It pushes callback to the queue (we assume it be release RMW operation)
        In reality right now it is seq_cst, but it this can and should be changed in the future.
        2) Now, the second (and third) action is seq_cst fence and then seq_cst read of Stopped_.
        Shutdown also does two thigns:
        1) It does a seq_cst rmw in Stopped_ (effectively, just write)
        2) It drains the queue.
        In order to prevent losing callbacks in queue
        We need to make sure that either Invoke observes |true| in the read
        or Shutdown observes placed callback from said Invoke.
        We care about this, because if callback is stuck in queue it will
        remain there until process is killed. If said callback MUST be
        either executed or discarded (e.g. IPollable or Fiber) in order
        for program to finish correctly (an not get stuck), then such a leak
        can cause hung shutdown (and it used to do exactly that).

        Relevant parts of execution are written below
        (letter is the same in the code).
            T1(Invoke)                  T2(Shutdown)
        RMW^rel(Queue, 0, 1)   (a)   RMW^rlx(Stopping, false, true) (d)
        Fence^sc               (b)   Fence^sc                       (e)
        R^rlx(Stopping, false) (c)   RMW^acq(Queue, 0, 0)           (f)

        We suppose that callback is lost so we haven't
        seen 1 in (f) nor true in (c).
        Since (c) reads false it reads from ctor which precedes
        (d) in modification order. Thus we have (c) -cob-> (d)
        (coherence-ordered before, https://eel.is/c++draft/atomics.order#3.3).
        Likewise (f) must have read from some previous
        dequeue attempt (or ctor) which precedes (a) in modification order.
        Again, (f) -cob-> (a).
        Now, for fences we have (sb means sequenced-before):
        (b) -sb-> (c) -cob-> (d) -sb-> (e) => (b) -S-> (e)
        (see https://eel.is/c++draft/atomics.order#4.4).
        We also have
        (e) -sb-> (f) -cob-> (a) -sb-> (b) => (b) -S-> (e).
        Thus loop in S found contradicting the assumption
        that (f) doesn't read 1 and (c) doesn't read true.
    */

    // Invoke is lock free on a fast path (a.k.a. no shutdown).
    void Invoke(TClosure callback, TBucket* bucket) override
    {
        // We can't guarantee read of |true| in time anyway
        // So relaxed order is enough.
        if (Stopped_.load(std::memory_order::relaxed)) {
            Drain();
            return;
        }

        auto cpuInstant = GetCpuInstant();

        YT_LOG_TRACE("Invoking action (EnqueuedAt: %v, Invoker: %v)",
            cpuInstant,
            ThreadNamePrefix_);

        TAction action;
        action.EnqueuedAt = cpuInstant;
        // Callback keeps raw ptr to bucket to minimize bucket ref count.
        action.Callback = BIND(&TBucket::RunCallback, Unretained(bucket), std::move(callback), cpuInstant);
        action.BucketHolder = MakeStrong(bucket);
        action.EnqueuedThreadCookie = ThreadCookie();

        InvokeQueue_.Enqueue(std::move(action)); // <- (a)

        std::atomic_thread_fence(std::memory_order::seq_cst); // <- (b)
        if (Stopped_.load(std::memory_order::relaxed)) { // <- (c)
            // We have encountered stop right after we have enqueued
            // callback. There is a possibility that it is now stuck
            // in queue forever. We dequeue it ourselves.
            Drain();
        }

        NotifyFromInvoke(cpuInstant, ActiveThreads_.load() == 0);
    }

    void StopPrologue()
    {
        GetEventCount()->NotifyAll();
    }

    TClosure OnExecute(int index, bool fetchNext, std::function<bool()> isStopping)
    {
        while (true) {
            auto cookie = GetEventCount()->PrepareWait();

            auto hasAction = ThreadStates_[index].Action.BucketHolder;
            int activeThreadDelta = hasAction ? -1 : 0;

            auto callback = DoOnExecute(index, fetchNext);

            if (callback) {
                activeThreadDelta += 1;
            }

            YT_VERIFY(activeThreadDelta >= -1 && activeThreadDelta <= 1);

            if (activeThreadDelta != 0) {
                auto activeThreads = ActiveThreads_.fetch_add(activeThreadDelta);
                auto newActiveThreads = activeThreads + activeThreadDelta;
                YT_VERIFY(newActiveThreads >= 0 && newActiveThreads <= TThreadPoolBase::MaxThreadCount);
                activeThreadDelta = 0;
            }

            if (callback || isStopping()) {
                CancelWait();
                return callback;
            }

            YT_VERIFY(fetchNext);
            Wait(cookie, isStopping);
        }
    }

    void Shutdown()
    {
        if (Stopped_.exchange(true, std::memory_order::relaxed)) { // <- (d)
            return;
        }
        std::atomic_thread_fence(std::memory_order::seq_cst); // <- (e)

        Drain(); // <- (f)
    }

    void Drain()
    {
        auto guard = Guard(MainLock_);

        WaitHeap_.Clear();

        std::vector<TBucket*> buckets;
        ActivePoolsHeap_.ForEach([&] (auto* pool) {
            pool->ActiveBucketsHeap.ForEach([&] (auto* bucket) {
                buckets.push_back(bucket);
            });
            pool->ActiveBucketsHeap.Clear();
        });
        ActivePoolsHeap_.Clear();

        // Actions hold strong references to buckets.
        // Buckets' ActionQueue must be cleared before destroying actions.
        std::vector<TAction> actions;
        for (auto* bucket : buckets) {
            while (!bucket->ActionQueue.empty()) {
                actions.push_back(std::move(bucket->ActionQueue.front()));
                bucket->ActionQueue.pop();
            }
        }

        InvokeQueue_.DequeueAll();

        guard.Release();
        actions.clear();

    }

    void RegisterWaitTimeObserver(TWaitTimeObserver waitTimeObserver)
    {
        WaitTimeObserver_ = waitTimeObserver;
        auto alreadyInitialized = IsWaitTimeObserverSet_.exchange(true);

        // Multiple observers are forbidden.
        YT_VERIFY(!alreadyInitialized);
    }

private:
    struct TThreadState
    {
        std::atomic<ERequest> Request = ERequest::None;

        TCpuInstant AccountedAt = 0;
        TAction Action;

        // Used to store bucket ref under lock to destroy it outside.
        TBucketPtr BucketToUnref;
        int LastActionsInQueue;
        TDuration TimeFromStart;
        TDuration TimeFromEnqueue;
    };

    static_assert(sizeof(TThreadState) >= CacheLineSize);

    const TString ThreadNamePrefix_;
    const TProfiler Profiler_;
    const NProfiling::TTimeCounter CumulativeSchedulingTimeCounter_;
    const IPoolWeightProviderPtr PoolWeightProvider_;
    const bool VerboseLogging_;

    std::atomic<bool> Stopped_ = false;
    TMpscStack<TAction> InvokeQueue_;
    char Padding0_[CacheLineSize - sizeof(TMpscStack<TAction>)];

    // Use simple non adaptive spinlock without complex wait strategies.
    ::TSpinLock MainLock_;
    char Padding1_[CacheLineSize - sizeof(::TSpinLock)];

    std::array<TThreadState, TThreadPoolBase::MaxThreadCount> ThreadStates_;

    TPriorityQueue<TExecutionPool> ActivePoolsHeap_;
    TCpuDuration LastPoolExcessTime_ = 0;
    TPriorityQueue<TEnqueuedTime> WaitHeap_;

    // Buffer to keep actions during distribution to threads.
    std::array<TAction, TThreadPoolBase::MaxThreadCount> OtherActions_;
    std::atomic<int> ThreadCount_ = 0;
    std::atomic<int> ActiveThreads_ = 0;

    std::atomic<bool> IsWaitTimeObserverSet_;
    TWaitTimeObserver WaitTimeObserver_;

    TProfiler GetPoolProfiler(const TString& poolName) override
    {
        return Profiler_.WithTags(GetBucketTags(ThreadNamePrefix_, poolName));
    }

    Y_NO_INLINE void ConsumeInvokeQueue()
    {
        VERIFY_SPINLOCK_AFFINITY(MainLock_);

        Y_UNUSED(Padding0_);
        Y_UNUSED(Padding1_);

        InvokeQueue_.DequeueAll(true, [&] (auto& action) {
            auto* bucket = action.BucketHolder.Get();

            auto* pool = bucket->Pool.Get();

            YT_VERIFY(!pool->LinkedListNode.Next && !pool->LinkedListNode.Prev);

            if (!pool->GetPositionInHeap()) {
                // ExcessTime can be greater than last pool excess time
                // if the pool is "currently executed" and reschedules action.
                if (pool->ExcessTime < LastPoolExcessTime_) {
                    // Use last pool excess time to schedule new pool
                    // after earlier scheduled pools (and not yet executed) in queue.

                    YT_LOG_DEBUG_IF(VerboseLogging_, "Initial pool excess time (Name: %v, ExcessTime: %v -> %v)",
                        pool->PoolName,
                        pool->ExcessTime,
                        LastPoolExcessTime_);

                    pool->ExcessTime = LastPoolExcessTime_;
                }

                ActivePoolsHeap_.Insert(pool);
            }

            ++pool->ActionCountInQueue;

            auto enqueuedAt = action.EnqueuedAt;

            bool wasEmpty = bucket->ActionQueue.empty();
            bucket->ActionQueue.push(std::move(action));

            if (wasEmpty) {
                bucket->EnqueuedTime.Value = enqueuedAt;
            }

            YT_ASSERT(wasEmpty == !bucket->GetPositionInHeap());

            if (!bucket->GetPositionInHeap()) {
                // ExcessTime can be greater than last bucket excess time
                // if the bucket is "currently executed" and reschedules action.
                if (bucket->ExcessTime < pool->LastBucketExcessTime) {
                    // Use last bucket excess time to schedule new bucket
                    // after earlier scheduled buckets (and not yet executed) in queue.

                    YT_LOG_DEBUG_IF(VerboseLogging_, "Initial bucket excess time (Name: %v, ExcessTime: %v -> %v)",
                        bucket->BucketName,
                        bucket->ExcessTime,
                        pool->LastBucketExcessTime);

                    bucket->ExcessTime = pool->LastBucketExcessTime;
                }

                pool->ActiveBucketsHeap.Insert(bucket);
                pool->BucketCounter.Record(pool->ActiveBucketsHeap.GetSize());
                WaitHeap_.Insert(&bucket->EnqueuedTime);
            }
        });
    }

    void ServeBeginExecute(TThreadState* threadState, TCpuInstant currentInstant, TAction action)
    {
        VERIFY_SPINLOCK_AFFINITY(MainLock_);

        YT_ASSERT(!threadState->Action.Callback);
        YT_ASSERT(!threadState->Action.BucketHolder);

        action.StartedAt = currentInstant;

        threadState->AccountedAt = currentInstant;
        threadState->Action = std::move(action);
    }

    void ServeEndExecute(TThreadState* threadState, TCpuInstant /*cpuInstant*/)
    {
        VERIFY_SPINLOCK_AFFINITY(MainLock_);

        auto action = std::move(threadState->Action);
        YT_ASSERT(!threadState->Action.Callback);
        YT_ASSERT(!action.Callback);

        if (!action.BucketHolder) {
            // There was no action in begin execute.
            return;
        }

        // Try not to change bucket ref count under lock.
        auto bucket = std::move(action.BucketHolder);

        auto& pool = *bucket->Pool;
        YT_ASSERT(pool.PoolName == bucket->PoolName);

        // LastActionsInQueue is used to update SizeCounter outside lock.
        threadState->LastActionsInQueue = --pool.ActionCountInQueue;

        // Do not destroy bucket pointer under lock. Move it in thread state in other place and
        // destroy in corresponding thread after combiner.
        threadState->BucketToUnref = std::move(bucket);
    }

    Y_NO_INLINE void UpdateExcessTime(TBucket* bucket, TCpuDuration duration, TCpuInstant currentInstant)
    {
        VERIFY_SPINLOCK_AFFINITY(MainLock_);

        auto* pool = bucket->Pool.Get();

        if (PoolWeightProvider_ && pool->NextUpdateWeightInstant < currentInstant) {
            pool->NextUpdateWeightInstant = currentInstant + DurationToCpuDuration(TDuration::Seconds(1));
            pool->InverseWeight = 1.0 / PoolWeightProvider_->GetWeight(pool->PoolName);
        }

        YT_LOG_DEBUG_IF(VerboseLogging_, "Increment excess time (BucketName: %v, PoolName: %v, ExcessTime: %v -> %v)",
            bucket->BucketName,
            bucket->PoolName,
            bucket->ExcessTime,
            bucket->ExcessTime + duration);

        pool->ExcessTime += duration * pool->InverseWeight;
        bucket->ExcessTime += duration;

        if (auto* positionInHeap = pool->GetPositionInHeap()) {
            ActivePoolsHeap_.AdjustDown(pool);
        }

        if (auto* positionInHeap = bucket->GetPositionInHeap()) {
            pool->ActiveBucketsHeap.AdjustDown(bucket);
        }

        // No need to update wait heap.
        YT_ASSERT(!bucket->EnqueuedTime.GetPositionInHeap() == !bucket->GetPositionInHeap());
    }

    Y_NO_INLINE bool GetStarvingBucket(TAction* action)
    {
        VERIFY_SPINLOCK_AFFINITY(MainLock_);

        YT_LOG_DEBUG_IF(
            VerboseLogging_,
            "Buckets: %v",
            MakeFormattableView(
                xrange(size_t(0), ActivePoolsHeap_.GetSize()),
                [&] (auto* builder, auto index) {
                    auto& pool = ActivePoolsHeap_[index];
                    builder->AppendFormat("%v [", CpuDurationToDuration(pool.ExcessTime));

                    for (size_t bucketIndex = 0; bucketIndex < pool.ActiveBucketsHeap.GetSize(); ++bucketIndex) {
                        const auto& bucket = pool.ActiveBucketsHeap[bucketIndex];

                        builder->AppendFormat("%Qv:%v/%v ",
                            bucket.BucketName,
                            CpuDurationToDuration(bucket.ExcessTime),
                            bucket.ActionQueue.front().EnqueuedAt);
                    }
                    builder->AppendFormat("]");
                }));

        if (ActivePoolsHeap_.Empty()) {
            return false;
        }

        auto* pool = ActivePoolsHeap_.GetFront();
        LastPoolExcessTime_ = pool->ExcessTime;

        auto* bucket = pool->ActiveBucketsHeap.GetFront();
        pool->LastBucketExcessTime = bucket->ExcessTime;

        YT_ASSERT(!bucket->ActionQueue.empty());
        *action = std::move(bucket->ActionQueue.front());
        bucket->ActionQueue.pop();

        YT_ASSERT(bucket == action->BucketHolder);

        if (bucket->ActionQueue.empty()) {
            bucket->EnqueuedTime.Value = std::numeric_limits<TCpuInstant>::max();

            WaitHeap_.Extract(&bucket->EnqueuedTime);

            pool->ActiveBucketsHeap.Extract(bucket);
            pool->BucketCounter.Record(pool->ActiveBucketsHeap.GetSize());

            if (pool->ActiveBucketsHeap.Empty()) {
                ActivePoolsHeap_.Extract(pool);
            }
        } else {
            bucket->EnqueuedTime.Value = bucket->ActionQueue.front().EnqueuedAt;
            WaitHeap_.AdjustDown(&bucket->EnqueuedTime);
        }

        return true;
    }

    Y_NO_INLINE std::tuple<int, int> ServeCombinedRequests(TCpuInstant currentInstant, int currentThreadIndex)
    {
        VERIFY_SPINLOCK_AFFINITY(MainLock_);

        auto threadCount = ThreadCount_.load();
        // Thread pool size can be reconfigures during serving requests.
        threadCount = std::max(threadCount, currentThreadIndex + 1);

        // Saved thread requests. They must be saved before consuming invoke queue.
        std::array<bool, TThreadPoolBase::MaxThreadCount> threadRequests{false};
        std::array<int, TThreadPoolBase::MaxThreadCount> threadIds;
        int requestCount = 0;

        YT_LOG_TRACE("Updating excess time");

        // Recalculate excess time for all currently evaluating or evaluated recently (end execute) buckets
        for (int threadIndex = 0; threadIndex < threadCount; ++threadIndex) {
            auto& threadState = ThreadStates_[threadIndex];

            // TODO(lukyan): Can skip (for threads without requests) or throttle UpdateExcessTime if it happens frequently.
            // For each currently evaluating buckets recalculate excess time.
            if (auto* bucket = threadState.Action.BucketHolder.Get()) {

                // TODO(lukyan): Update last excess time for pool without active buckets.
                UpdateExcessTime(bucket, currentInstant - threadState.AccountedAt, currentInstant);
                threadState.AccountedAt = currentInstant;
            }

            auto request = threadState.Request.load(std::memory_order::acquire);
            if (request != ERequest::None) {
                ServeEndExecute(&threadState, currentInstant);

                if (request == ERequest::FetchNext) {
                    // Save requests before ConsumeInvokeQueue. Otherwise some thread can schedule action
                    // but action can not be fetched (schedule and fetch happens after ConsumeInvokeQueue).
                    threadRequests[threadIndex] = true;
                    threadIds[requestCount++] = threadIndex;
                } else {
                    threadState.Request.store(ERequest::None, std::memory_order::release);
                }
            }
        }

        YT_LOG_TRACE("Consuming invoke queue");

        ConsumeInvokeQueue();

        int fetchedActions = 0;
        int otherActionCount = 0;

        // Schedule actions to desired threads.
        while (fetchedActions < requestCount) {
            TAction action;

            if (!GetStarvingBucket(&action)) {
                break;
            }

            ++fetchedActions;

            int threadIndex = -1;

            auto unpackedCookie = TTaggedPtr<TTwoLevelFairShareQueue>::Unpack(action.EnqueuedThreadCookie);
            // TODO(lukyan): Check also wait time. If it is too high, no matter where to schedule.
            if (unpackedCookie.Ptr == this) {
                threadIndex = unpackedCookie.Tag;
            }

            if (threadIndex != -1 && threadRequests[threadIndex]) {
                ServeBeginExecute(&ThreadStates_[threadIndex], currentInstant, std::move(action));
                threadRequests[threadIndex] = false;
                ThreadStates_[threadIndex].Request.store(ERequest::None, std::memory_order::release);
            } else {
                OtherActions_[otherActionCount++] = std::move(action);
            }
        }

        // Schedule other actions.
        for (int threadIndex : MakeRange(threadIds.data(), requestCount)) {
            if (threadRequests[threadIndex]) {
                if (otherActionCount > 0) {
                    ServeBeginExecute(&ThreadStates_[threadIndex], currentInstant, std::move(OtherActions_[--otherActionCount]));
                }

                ThreadStates_[threadIndex].Request.store(ERequest::None, std::memory_order::release);
            }
        }

        return {requestCount, fetchedActions};
    }

    TCpuInstant GetMinEnqueuedAt()
    {
        VERIFY_SPINLOCK_AFFINITY(MainLock_);

        return WaitHeap_.Empty()
            ? std::numeric_limits<TCpuInstant>::max()
            : WaitHeap_.GetFront()->Value;
    }

    TClosure DoOnExecute(int index, bool fetchNext)
    {
        auto cpuInstant = GetCpuInstant();
        auto& threadState = ThreadStates_[index];

        const auto& oldAction = threadState.Action;
        if (oldAction.BucketHolder) {
            auto waitTime = CpuDurationToDuration(oldAction.StartedAt - oldAction.EnqueuedAt);
            auto timeFromStart = CpuDurationToDuration(cpuInstant - oldAction.StartedAt);
            auto timeFromEnqueue = CpuDurationToDuration(cpuInstant - oldAction.EnqueuedAt);

            threadState.TimeFromStart = timeFromStart;
            threadState.TimeFromEnqueue = timeFromEnqueue;

            if (timeFromStart > LogDurationThreshold) {
                YT_LOG_DEBUG("Callback execution took too long (Wait: %v, Execution: %v, Total: %v)",
                    waitTime,
                    timeFromStart,
                    timeFromEnqueue);
            }

            if (waitTime > LogDurationThreshold) {
                YT_LOG_DEBUG("Callback wait took too long (Wait: %v, Execution: %v, Total: %v)",
                    waitTime,
                    timeFromStart,
                    timeFromEnqueue);
            }
        }

        auto finally = Finally([&] {
            auto bucketToUndef = std::move(threadState.BucketToUnref);
            if (bucketToUndef) {
                auto* pool = bucketToUndef->Pool.Get();
                pool->SizeCounter.Record(threadState.LastActionsInQueue);
                pool->DequeuedCounter.Increment(1);
                pool->ExecTimeCounter.Record(threadState.TimeFromStart);
                pool->TotalTimeCounter.Record(threadState.TimeFromEnqueue);
                pool->CumulativeTimeCounter.Add(threadState.TimeFromStart);
                bucketToUndef.Reset();
            }

            const auto& action = threadState.Action;
            if (action.BucketHolder) {
                auto waitTime = CpuDurationToDuration(action.StartedAt - action.EnqueuedAt);
                action.BucketHolder->Pool->WaitTimeCounter.Record(waitTime);
                ReportWaitTime(waitTime);
            }

            CumulativeSchedulingTimeCounter_.Add(CpuDurationToDuration(GetCpuInstant() - cpuInstant));

            if (!fetchNext) {
                MaybeProceedRetainQueue(cpuInstant);
            }
        });

        auto& request = threadState.Request;
        YT_VERIFY(request == ERequest::None);
        request.store(fetchNext ? ERequest::FetchNext : ERequest::EndExecute);

        if (MainLock_.IsLocked() || !MainLock_.TryAcquire()) {
            // Locked here.
            while (true) {
                SpinLockPause();

                if (request.load(std::memory_order::acquire) == ERequest::None) {
                    return std::move(threadState.Action.Callback);
                } else if (!MainLock_.IsLocked() && MainLock_.TryAcquire()) {
                    break;
                }
            }
        }

        ResetMinEnqueuedAt();

        YT_LOG_TRACE("Started serving requests");
        auto [requests, fetchedActions] = ServeCombinedRequests(cpuInstant, index);

        // Evaluate notify condition here, but call NotifyAfterFetch outside lock.
        auto newMinEnqueuedAt = GetMinEnqueuedAt();
        MainLock_.Release();

        auto endInstant = GetCpuInstant();
        YT_LOG_TRACE("Finished serving requests (Duration: %v, Requests: %v, FetchCount: %v, MinEnqueuedAt: %v)",
            CpuDurationToDuration(endInstant - cpuInstant),
            requests,
            fetchedActions,
            CpuInstantToInstant(newMinEnqueuedAt));

        NotifyAfterFetch(endInstant, newMinEnqueuedAt);

        return std::move(threadState.Action.Callback);
    }

    void ReportWaitTime(TDuration waitTime)
    {
        if (IsWaitTimeObserverSet_.load()) {
            WaitTimeObserver_(waitTime);
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TTwoLevelFairShareQueue)

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

    void OnStart() override
    {
        ThreadCookie() = TTaggedPtr(Queue_.Get(), static_cast<ui16>(Index_)).Pack();
    }

    void StopPrologue() override
    {
        Queue_->StopPrologue();
    }

    TClosure OnExecute() override
    {
        bool fetchNext = !TSchedulerThread::IsStopping() || TSchedulerThread::GracefulStop_;

        return Queue_->OnExecute(Index_, fetchNext, [&] {
            return TSchedulerThread::IsStopping();
        });
    }

    TClosure BeginExecute() override
    {
        Y_UNREACHABLE();
    }

    void EndExecute() override
    {
        Y_UNREACHABLE();
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
        const TNewTwoLevelFairShareThreadPoolOptions& options)
        : TThreadPoolBase(threadNamePrefix)
        , Queue_(New<TTwoLevelFairShareQueue>(
            CallbackEventCount_,
            ThreadNamePrefix_,
            options))
    {
        Configure(threadCount);
    }

    ~TTwoLevelFairShareThreadPool()
    {
        Shutdown();
    }

    void Configure(int threadCount) override
    {
        TThreadPoolBase::Configure(threadCount);
    }

    void Configure(TDuration pollingPeriod) override
    {
        Queue_->Configure(pollingPeriod);
    }

    IInvokerPtr GetInvoker(
        const TString& poolName,
        const TFairShareThreadPoolTag& bucketName) override
    {
        EnsureStarted();
        return Queue_->GetInvoker(poolName, bucketName);
    }

    void Shutdown() override
    {
        TThreadPoolBase::Shutdown();
    }

    int GetThreadCount() override
    {
        return TThreadPoolBase::GetThreadCount();
    }

    void RegisterWaitTimeObserver(TWaitTimeObserver waitTimeObserver) override
    {
        Queue_->RegisterWaitTimeObserver(waitTimeObserver);
    }

private:
    const TIntrusivePtr<NThreading::TEventCount> CallbackEventCount_ = New<NThreading::TEventCount>();
    const TTwoLevelFairShareQueuePtr Queue_;

    void DoShutdown() override
    {
        Queue_->Shutdown();
        TThreadPoolBase::DoShutdown();
        Queue_->Drain();
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

ITwoLevelFairShareThreadPoolPtr CreateNewTwoLevelFairShareThreadPool(
    int threadCount,
    const TString& threadNamePrefix,
    const TNewTwoLevelFairShareThreadPoolOptions& options)
{
    return New<TTwoLevelFairShareThreadPool>(
        threadCount,
        threadNamePrefix,
        options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
