#pragma once

#include "private.h"

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/library/ytprof/api/api.h>

#include <yt/yt/core/concurrency/moody_camel_concurrent_queue.h>

#include <yt/yt/core/actions/invoker.h>

#include <yt/yt/core/profiling/public.h>

#include <yt/yt/core/misc/mpsc_queue.h>

#include <library/cpp/yt/threading/event_count.h>

#include <atomic>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

struct TEnqueuedAction
{
    bool Finished = true;
    NProfiling::TCpuInstant EnqueuedAt = 0;
    NProfiling::TCpuInstant StartedAt = 0;
    NProfiling::TCpuInstant FinishedAt = 0;
    TClosure Callback;
    int ProfilingTag = 0;
    NYTProf::TProfilerTagPtr ProfilerTag;
};

////////////////////////////////////////////////////////////////////////////////

class TMpmcQueueImpl
{
public:
    using TConsumerToken = std::array<moodycamel::ConsumerToken, 2>;

    void Enqueue(TEnqueuedAction&& action);
    void Enqueue(TMutableRange<TEnqueuedAction> actions);
    bool TryDequeue(TEnqueuedAction* action, TConsumerToken* token = nullptr);

    void DrainProducer();
    void DrainConsumer();

    TConsumerToken MakeConsumerToken();

    bool IsEmpty() const;

    bool HasSingleConsumer() const;

private:
    using TBucket = moodycamel::ConcurrentQueue<TEnqueuedAction>;
    std::array<TBucket, 2> Buckets_;

    alignas(CacheLineSize) std::atomic<int> Size_ = 0;

    // Bit 0: producer bucket index
    // Bit 1: consumer bucket index
    // Bits 2..63: epoch
    //
    // Transitions:
    //              *---------------------------*
    //              |                           |
    //             \|/                          |
    // 0=00 (produce to B0, consume from B0)<---*---*
    //              |                           |   |
    //              | epoch changes             |   |
    //             \|/                          |   |
    // 1=01 (produce to B1, consume from B0)    |   | dequeue spin limit exceeded
    //              |                           |   |
    //              | B0 is exhausted           |   |
    //             \|/                          |   |
    // 3=11 (produce to B1, consume from B1)<---*---*
    //              |                           |
    //              | epoch changes             |
    //             \|/                          |
    // 2=10 (produce to B0, consume from B1)    |
    //              |                           |
    //              | B1 is exhausted           |
    //              |                           |
    //              *---------------------------*
    alignas(CacheLineSize) std::atomic<ui64> BucketSelector_ = 0;

    template <class T>
    void DoEnqueue(TCpuInstant instant, T&& func);
    void EnqueueTo(TBucket* bucket, TEnqueuedAction&& action);
    void EnqueueTo(TBucket* bucket, TMutableRange<TEnqueuedAction> actions);

    static ui64 EpochFromInstant(TCpuInstant instant);
};

////////////////////////////////////////////////////////////////////////////////

class TMpscQueueImpl
{
public:
    using TConsumerToken = std::monostate;

    void Enqueue(TEnqueuedAction&& action);
    void Enqueue(TMutableRange<TEnqueuedAction> actions);
    bool TryDequeue(TEnqueuedAction* action, TConsumerToken* token = nullptr);

    void DrainProducer();
    void DrainConsumer();

    TConsumerToken MakeConsumerToken();

    bool IsEmpty() const;

    bool HasSingleConsumer() const;

private:
    TMpscQueue<TEnqueuedAction> Queue_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TQueueImpl>
class TInvokerQueue
    : public IInvoker
{
public:
    TInvokerQueue(
        TIntrusivePtr<NThreading::TEventCount> callbackEventCount,
        const NProfiling::TTagSet& counterTagSet,
        NProfiling::IRegistryImplPtr registry = nullptr);

    TInvokerQueue(
        TIntrusivePtr<NThreading::TEventCount> callbackEventCount,
        const std::vector<NProfiling::TTagSet>& counterTagSets,
        const std::vector<NYTProf::TProfilerTagPtr>& profilerTags,
        NProfiling::IRegistryImplPtr registry = nullptr);

    void SetThreadId(NThreading::TThreadId threadId);

    void Invoke(TClosure callback) override;

    void Invoke(TMutableRange<TClosure> callbacks) override;

    void Invoke(
        TClosure callback,
        NProfiling::TTagId profilingTag,
        NYTProf::TProfilerTagPtr profilerTag);

    TEnqueuedAction MakeAction(
        TClosure callback,
        NProfiling::TTagId profilingTag,
        NYTProf::TProfilerTagPtr profilerTag,
        TCpuInstant cpuInstant);

    TCpuInstant EnqueueCallback(
        TClosure callback,
        NProfiling::TTagId profilingTag,
        NYTProf::TProfilerTagPtr profilerTag);

    TCpuInstant EnqueueCallbacks(
        TMutableRange<TClosure> callbacks,
        NProfiling::TTagId profilingTag = 0,
        NYTProf::TProfilerTagPtr profilerTag = nullptr);

    NThreading::TThreadId GetThreadId() const override;
    bool CheckAffinity(const IInvokerPtr& invoker) const override;
    bool IsSerialized() const override;

    // NB(arkady-e1ppa): Trying to call graceful shutdown
    // concurrently with someone calling Invoke
    // may end up making shutdown not graceful
    // as double-checking in Invoke would drain
    // the queue. If want a truly graceful
    // Shutdown (e.g. until you run out of callbacks)
    // just drain the queue without shutting it down.
    void Shutdown(bool graceful = false);

    // NB(arkady-e1ppa): Calling shutdown is not
    // enough to prevent leaks of callbacks
    // as there might be some callbacks left in
    // local queue of MPSC queue if shutdown
    // was not graceful.
    void OnConsumerFinished();

    bool BeginExecute(TEnqueuedAction* action, typename TQueueImpl::TConsumerToken* token = nullptr);
    void EndExecute(TEnqueuedAction* action);

    typename TQueueImpl::TConsumerToken MakeConsumerToken();

    bool IsEmpty() const;
    bool IsRunning() const;

    IInvoker* GetProfilingTagSettingInvoker(int profilingTag);

    void RegisterWaitTimeObserver(TWaitTimeObserver waitTimeObserver) override;

private:
    const TIntrusivePtr<NThreading::TEventCount> CallbackEventCount_;

    TQueueImpl QueueImpl_;

    NThreading::TThreadId ThreadId_ = NThreading::InvalidThreadId;
    std::atomic<bool> Running_ = true;
    std::atomic<bool> Stopping_ = false;
    std::atomic<bool> Graceful_ = false;

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

    std::vector<IInvokerPtr> ProfilingTagSettingInvokers_;

    std::atomic<bool> IsWaitTimeObserverSet_;
    TWaitTimeObserver WaitTimeObserver_;

    TCountersPtr CreateCounters(const NProfiling::TTagSet& tagSet, NProfiling::IRegistryImplPtr registry);

    void TryDrainProducer(bool force = false);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
