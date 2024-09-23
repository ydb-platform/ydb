#include "fair_share_thread_pool.h"

#include "private.h"
#include "invoker_queue.h"
#include "profiling_helpers.h"
#include "scheduler_thread.h"
#include "thread_pool_detail.h"

#include <yt/yt/core/actions/current_invoker.h>

#include <yt/yt/core/misc/heap.h>
#include <yt/yt/core/misc/ring_queue.h>

#include <yt/yt/core/profiling/tscp.h>

#include <library/cpp/yt/memory/weak_ptr.h>

#include <util/generic/xrange.h>

namespace NYT::NConcurrency {

using namespace NProfiling;

static constexpr auto& Logger = ConcurrencyLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

struct THeapItem;
class TFairShareQueue;

DECLARE_REFCOUNTED_CLASS(TBucket)

class TBucket
    : public IInvoker
{
public:
    TBucket(TFairShareThreadPoolTag tag, TWeakPtr<TFairShareQueue> parent)
        : Tag(std::move(tag))
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

    TFairShareThreadPoolTag Tag;
    TWeakPtr<TFairShareQueue> Parent;
    TRingQueue<TEnqueuedAction> Queue;
    THeapItem* HeapIterator = nullptr;
    i64 WaitTime = 0;

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

DECLARE_REFCOUNTED_TYPE(TFairShareQueue)

class TFairShareQueue
    : public TRefCounted
{
public:
    TFairShareQueue(
        TIntrusivePtr<NThreading::TEventCount> callbackEventCount,
        const TTagSet& tags)
        : CallbackEventCount_(std::move(callbackEventCount))
    {
        auto profiler = TProfiler{"/fair_share_queue"}.WithHot().WithTags(tags);
        BucketCounter_ = profiler.Summary("/buckets");
        SizeCounter_ = profiler.Summary("/size");
        WaitTimeCounter_ = profiler.Timer("/time/wait");
        ExecTimeCounter_ = profiler.Timer("/time/exec");
        TotalTimeCounter_ = profiler.Timer("/time/total");
    }

    ~TFairShareQueue()
    {
        Shutdown();
    }

    void Configure(int threadCount)
    {
        ThreadCount_.store(threadCount);
    }

    IInvokerPtr GetInvoker(const TFairShareThreadPoolTag& tag)
    {
        auto guard = Guard(TagMappingSpinLock_);

        auto inserted = TagToBucket_.emplace(tag, nullptr).first;
        auto invoker = inserted->second.Lock();

        if (!invoker) {
            invoker = New<TBucket>(tag, MakeWeak(this));
            inserted->second = invoker;
        }

        BucketCounter_.Record(TagToBucket_.size());
        return invoker;
    }

    void Invoke(TClosure callback, TBucket* bucket)
    {
        auto guard = Guard(SpinLock_);
        // See Shutdown.
        if (Stopping_) {
            return;
        }

        QueueSize_.fetch_add(1, std::memory_order::relaxed);

        if (!bucket->HeapIterator) {
            // Otherwise ExcessTime will be recalculated in AccountCurrentlyExecutingBuckets.
            if (bucket->CurrentExecutions == 0 && !Heap_.empty()) {
                bucket->ExcessTime = Heap_.front().Bucket->ExcessTime;
            }

            Heap_.emplace_back(bucket);
            AdjustHeapBack(Heap_.begin(), Heap_.end());
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
        auto guard = Guard(TagMappingSpinLock_);
        auto it = TagToBucket_.find(bucket->Tag);

        if (it != TagToBucket_.end() && it->second.IsExpired()) {
            TagToBucket_.erase(it);
        }

        BucketCounter_.Record(TagToBucket_.size());
    }

    void Shutdown()
    {
        auto guard = Guard(SpinLock_);
        // We want to make sure that calls to
        // Shutdown and Invoke are "atomic" with respect
        // to each other. We need this so that
        // there are no tasks left in the queue after
        // the first call to Shutdown has finished.
        // Here we achieve that by accessing
        // both buckets and Stopping flag under
        // SpinLock in either method.
        // See two_level_fair_share_thread_pool.cpp
        // for lock-free version with a more detailed
        // explanation why Stopping flag logic provides
        // the desired guarantee.
        Stopping_ = true;
        for (const auto& item : Heap_) {
            item.Bucket->Drain();
        }
    }

    bool BeginExecute(TEnqueuedAction* action, int index)
    {
        auto& threadState = ThreadStates_[index];

        YT_ASSERT(!threadState.Bucket);

        YT_ASSERT(action && action->Finished);

        auto tscp = NProfiling::TTscp::Get();

        TBucketPtr bucket;
        {
            auto guard = Guard(SpinLock_);
            bucket = GetStarvingBucket(action, tscp);

            if (!bucket) {
                return false;
            }

            ++bucket->CurrentExecutions;

            threadState.Bucket = bucket;
            threadState.AccountedAt = tscp.Instant;

            action->StartedAt = tscp.Instant;
            bucket->WaitTime = action->StartedAt - action->EnqueuedAt;
        }

        YT_ASSERT(action && !action->Finished);

        WaitTimeCounter_.Record(CpuDurationToDuration(bucket->WaitTime));
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

        auto tscp = NProfiling::TTscp::Get();

        action->FinishedAt = tscp.Instant;

        int queueSize = QueueSize_.fetch_sub(1, std::memory_order::relaxed) - 1;
        SizeCounter_.Record(queueSize);

        auto timeFromStart = CpuDurationToDuration(action->FinishedAt - action->StartedAt);
        auto timeFromEnqueue = CpuDurationToDuration(action->FinishedAt - action->EnqueuedAt);
        ExecTimeCounter_.Record(timeFromStart);
        TotalTimeCounter_.Record(timeFromEnqueue);

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

            UpdateExcessTime(bucket.Get(), tscp.Instant - threadState.AccountedAt);

            YT_VERIFY(bucket->CurrentExecutions-- > 0);
        }
    }

private:
    struct TThreadState
    {
        TCpuInstant AccountedAt = 0;
        TBucketPtr Bucket;
    };

    const TIntrusivePtr<NThreading::TEventCount> CallbackEventCount_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    bool Stopping_ = false;
    std::vector<THeapItem> Heap_;

    std::atomic<int> ThreadCount_ = 0;
    std::array<TThreadState, TThreadPoolBase::MaxThreadCount> ThreadStates_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, TagMappingSpinLock_);
    THashMap<TFairShareThreadPoolTag, TWeakPtr<TBucket>> TagToBucket_;

    std::atomic<int> QueueSize_ = 0;

    NProfiling::TSummary BucketCounter_;
    NProfiling::TSummary SizeCounter_;
    TEventTimer WaitTimeCounter_;
    TEventTimer ExecTimeCounter_;
    TEventTimer TotalTimeCounter_;


    void AccountCurrentlyExecutingBuckets(NProfiling::TTscp tscp)
    {
        int threadCount = ThreadCount_.load();
        for (int index = 0; index < threadCount; ++index) {
            auto& threadState = ThreadStates_[index];
            if (!threadState.Bucket) {
                continue;
            }

            auto duration = tscp.Instant - threadState.AccountedAt;
            threadState.AccountedAt = tscp.Instant;

            UpdateExcessTime(threadState.Bucket.Get(), duration);
        }
    }

    void UpdateExcessTime(TBucket* bucket, TCpuDuration duration)
    {
        bucket->ExcessTime += duration;

        auto positionInHeap = bucket->HeapIterator;
        if (!positionInHeap) {
            return;
        }

        size_t indexInHeap = positionInHeap - Heap_.data();
        YT_VERIFY(indexInHeap < Heap_.size());
        SiftDown(Heap_.begin(), Heap_.end(), Heap_.begin() + indexInHeap, std::less<>());
    }

    TBucketPtr GetStarvingBucket(TEnqueuedAction* action, NProfiling::TTscp tscp)
    {
        // For each currently evaluating buckets recalculate excess time.
        AccountCurrentlyExecutingBuckets(tscp);

        #ifdef YT_ENABLE_TRACE_LOGGING
        if (Logger().IsLevelEnabled(NLogging::ELogLevel::Trace)) {
            auto guard = Guard(TagMappingSpinLock_);
            YT_LOG_TRACE("Buckets: [%v]",
                MakeFormattableView(
                    TagToBucket_,
                    [] (auto* builder, const auto& tagToBucket) {
                        if (auto item = tagToBucket.second.Lock()) {
                            auto excess = CpuDurationToDuration(tagToBucket.second.Lock()->ExcessTime).MilliSeconds();
                            builder->AppendFormat("(%v %v)", tagToBucket.first, excess);
                        } else {
                            builder->AppendFormat("(%v *)", tagToBucket.first);
                        }
                    }));
        }
        #endif

        if (Heap_.empty()) {
            return nullptr;
        }

        auto bucket = Heap_.front().Bucket;
        YT_VERIFY(!bucket->Queue.empty());
        *action = std::move(bucket->Queue.front());
        bucket->Queue.pop();

        if (bucket->Queue.empty()) {
            ExtractHeap(Heap_.begin(), Heap_.end());
            Heap_.pop_back();
        }

        return bucket;
    }
};

DEFINE_REFCOUNTED_TYPE(TFairShareQueue)

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
        TFairShareQueuePtr queue,
        TIntrusivePtr<NThreading::TEventCount> callbackEventCount,
        const TString& threadGroupName,
        const TString& threadName,
        NThreading::EThreadPriority threadPriority,
        int index)
        : TSchedulerThread(
            std::move(callbackEventCount),
            threadGroupName,
            threadName,
            NThreading::TThreadOptions{
                .ThreadPriority = threadPriority,
            })
        , Queue_(std::move(queue))
        , Index_(index)
    { }

protected:
    const TFairShareQueuePtr Queue_;
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

class TFairShareThreadPool
    : public IFairShareThreadPool
    , public TThreadPoolBase
{
public:
    TFairShareThreadPool(
        int threadCount,
        const TString& threadNamePrefix)
        : TThreadPoolBase(threadNamePrefix)
        , Queue_(New<TFairShareQueue>(
            CallbackEventCount_,
            GetThreadTags(ThreadNamePrefix_)))
    {
        Configure(threadCount);
        EnsureStarted();
    }

    ~TFairShareThreadPool()
    {
        Shutdown();
    }

    void Configure(int threadCount) override
    {
        TThreadPoolBase::Configure(threadCount);
    }

    IInvokerPtr GetInvoker(const TFairShareThreadPoolTag& tag) override
    {
        EnsureStarted();
        return Queue_->GetInvoker(tag);
    }

    void Shutdown() override
    {
        TThreadPoolBase::Shutdown();
    }

private:
    const TIntrusivePtr<NThreading::TEventCount> CallbackEventCount_ = New<NThreading::TEventCount>();
    const TFairShareQueuePtr Queue_;


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
            NThreading::EThreadPriority::Normal,
            index);
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

IFairShareThreadPoolPtr CreateFairShareThreadPool(
    int threadCount,
    const TString& threadNamePrefix)
{
    return New<TFairShareThreadPool>(
        threadCount,
        threadNamePrefix);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
