#include "invoker_queue.h"
#include "private.h"

#include <yt/yt/core/actions/invoker_detail.h>
#include <yt/yt/core/actions/current_invoker.h>

#include <yt/yt/core/profiling/tscp.h>

#include <library/cpp/yt/misc/tls.h>

namespace NYT::NConcurrency {

using namespace NProfiling;
using namespace NYTProf;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = ConcurrencyLogger;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_THREAD_LOCAL(TCpuProfilerTagGuard, CpuProfilerTagGuard);

////////////////////////////////////////////////////////////////////////////////

void TMpmcQueueImpl::Enqueue(TEnqueuedAction&& action)
{
    DoEnqueue(
        action.EnqueuedAt,
        [&] (TBucket* bucket) {
            EnqueueTo(bucket, std::move(action));
        });
}

void TMpmcQueueImpl::Enqueue(TMutableRange<TEnqueuedAction> actions)
{
    if (Y_UNLIKELY(actions.empty())) {
        return;
    }

    DoEnqueue(
        actions[0].EnqueuedAt,
        [&] (TBucket* bucket) {
            EnqueueTo(bucket, actions);
        });
}

template <class T>
void TMpmcQueueImpl::DoEnqueue(TCpuInstant instant, T&& func)
{
    auto currentSelector = BucketSelector_.load(std::memory_order::acquire);
    auto currentBucketIndex = currentSelector & 1;
    auto& bucket = Buckets_[currentBucketIndex];
    func(&bucket);
    auto currentSelectorState = currentSelector & 3;
    if (currentSelectorState == 0 || currentSelectorState == 3) {
        auto currentEpoch = currentSelector >> 2;
        auto newEpoch = EpochFromInstant(instant);
        if (newEpoch != currentEpoch) {
            auto newSelectorState = (currentSelectorState == 0 ? 1UL : 2UL);
            auto newSelector = newSelectorState | (newEpoch << 2);
            BucketSelector_.compare_exchange_weak(currentSelector, newSelector);
        }
    }
}

Y_FORCE_INLINE void TMpmcQueueImpl::EnqueueTo(TBucket* bucket, TEnqueuedAction&& action)
{
    YT_VERIFY(bucket->enqueue(std::move(action)));
    Size_.fetch_add(1, std::memory_order::release);
}

Y_FORCE_INLINE void TMpmcQueueImpl::EnqueueTo(TBucket* bucket, TMutableRange<TEnqueuedAction> actions)
{
    auto size = std::ssize(actions);
    YT_VERIFY(bucket->enqueue_bulk(std::make_move_iterator(actions.Begin()), size));
    Size_.fetch_add(size, std::memory_order::release);
}

bool TMpmcQueueImpl::TryDequeue(TEnqueuedAction* action, TConsumerToken* token)
{
    if (Size_.load() <= 0) {
        return false;
    }

    // Fast path.
    if (Size_.fetch_sub(1) <= 0) {
        Size_.fetch_add(1);

        // Slow path.
        auto queueSize = Size_.load();
        while (queueSize > 0 && !Size_.compare_exchange_weak(queueSize, queueSize - 1));

        if (queueSize <= 0) {
            return false;
        }
    }

    constexpr int MaxSpinCount = 100;
    for (int spinCount = 0; ; spinCount++) {
        auto currentSelector = BucketSelector_.load(std::memory_order::acquire);
        auto currentBucketIndex = (currentSelector & 3) >> 1;
        auto& bucket = Buckets_[currentBucketIndex];
        bool result = token
            ? bucket.try_dequeue((*token)[currentBucketIndex], *action)
            : bucket.try_dequeue(*action);
        if (result) {
            break;
        }
        auto currentSelectorState = currentSelector & 3;
        if (currentSelectorState == 1 || currentSelectorState == 2 || spinCount > MaxSpinCount) {
            auto newSelectorState = currentSelectorState <= 1 ? 3UL : 0UL;
            auto newEpoch = EpochFromInstant(GetApproximateCpuInstant());
            auto newSelector = newSelectorState | (newEpoch << 2);
            BucketSelector_.compare_exchange_weak(currentSelector, newSelector);
            spinCount = 0;
        }
    }

    return true;
}

void TMpmcQueueImpl::DrainProducer()
{
    auto queueSize = Size_.load();
    // Must use CAS to prevent modifying Size_ if it is negative.
    while (queueSize > 0 && !Size_.compare_exchange_weak(queueSize, 0));

    TEnqueuedAction action;
    while (queueSize-- > 0) {
        [&] {
            while (true) {
                for (auto& bucket : Buckets_) {
                    if (bucket.try_dequeue(action)) {
                        return;
                    }
                }
            }
        }();
    }
}

void TMpmcQueueImpl::DrainConsumer()
{
    DrainProducer();
}

TMpmcQueueImpl::TConsumerToken TMpmcQueueImpl::MakeConsumerToken()
{
    return {
        moodycamel::ConsumerToken(Buckets_[0]),
        moodycamel::ConsumerToken(Buckets_[1]),
    };
}

bool TMpmcQueueImpl::IsEmpty() const
{
    return Size_.load() <= 0;
}

bool TMpmcQueueImpl::HasSingleConsumer() const
{
    return false;
}

ui64 TMpmcQueueImpl::EpochFromInstant(TCpuInstant instant)
{
    // ~1 ms for 1 GHz clock.
    return instant >> 20;
}

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE void TMpscQueueImpl::Enqueue(TEnqueuedAction&& action)
{
    Queue_.Enqueue(std::move(action));
}

Y_FORCE_INLINE void TMpscQueueImpl::Enqueue(TMutableRange<TEnqueuedAction> actions)
{
    for (auto& action : actions) {
        Enqueue(std::move(action));
    }
}

Y_FORCE_INLINE bool TMpscQueueImpl::TryDequeue(TEnqueuedAction* action, TConsumerToken* /*token*/)
{
    return Queue_.TryDequeue(action);
}

void TMpscQueueImpl::DrainProducer()
{
    Queue_.DrainProducer();
}

void TMpscQueueImpl::DrainConsumer()
{
    Queue_.DrainConsumer();
}

TMpscQueueImpl::TConsumerToken TMpscQueueImpl::MakeConsumerToken()
{
    return {};
}

bool TMpscQueueImpl::IsEmpty() const
{
    return Queue_.IsEmpty();
}

bool TMpscQueueImpl::HasSingleConsumer() const
{
    return true;
}

////////////////////////////////////////////////////////////////////////////////

template <class TQueueImpl>
class TProfilingTagSettingInvoker
    : public IInvoker
{
public:
    TProfilingTagSettingInvoker(
        TWeakPtr<TInvokerQueue<TQueueImpl>> queue,
        NProfiling::TTagId profilingTag,
        TProfilerTagPtr profilerTag)
        : Queue_(std::move(queue))
        , ProfilingTag_(profilingTag)
        , ProfilerTag_(std::move(profilerTag))
    { }

    void Invoke(TClosure callback) override
    {
        if (auto queue = Queue_.Lock()) {
            queue->Invoke(std::move(callback), ProfilingTag_, ProfilerTag_);
        }
    }

    void Invoke(TMutableRange<TClosure> callbacks) override
    {
        if (auto queue = Queue_.Lock()) {
            for (auto& callback : callbacks) {
                queue->Invoke(std::move(callback), ProfilingTag_, ProfilerTag_);
            }
        }
    }

    TThreadId GetThreadId() const override
    {
        if (auto queue = Queue_.Lock()) {
            return queue->GetThreadId();
        } else {
            return {};
        }
    }

    bool CheckAffinity(const IInvokerPtr& invoker) const override
    {
        return invoker.Get() == this;
    }

    bool IsSerialized() const override
    {
        if (auto queue = Queue_.Lock()) {
            return queue->IsSerialized();
        } else {
            return true;
        }
    }

    void RegisterWaitTimeObserver(TWaitTimeObserver waitTimeObserver) override
    {
        if (auto queue = Queue_.Lock()) {
            queue->RegisterWaitTimeObserver(waitTimeObserver);
        }
    }

private:
    const TWeakPtr<TInvokerQueue<TQueueImpl>> Queue_;
    const int ProfilingTag_;
    const TProfilerTagPtr ProfilerTag_;
};

////////////////////////////////////////////////////////////////////////////////

// (arkady-e1ppa): Memory orders explanation around Shutdown:
/*
    There are two guarantees we want to enforce:
    1) If we read Running_ |false| we want to observe the correct value
    of Graceful_. Otherwise we may discard tasks even when graceful
    shutdown was requested.
    2) We must ensure that no callback will be left in the queue. That is if
    shutdown is not graceful, there must be no execution of
    concurrent Enqueue and Shutdown such that (c) (or (c'))
    reads |true| from Running_ and (f) doesn't observe
    action placed by (a) (or (a')). Note: in case of
    graceful shutdown we expect that caller manually drains the queue
    when they feel like it. This is sufficient because queue will either
    be drained by another producer or enqueue requests will stop and
    caller's method of draining the queue (e.g. stopping thread which polls
    the queue and ask it to drain the queue) would process the queue normally.

    Let's deal with 1) first since it is very easy. Consider relevant
    part of EnqueueCallback call (some calls are inlined):
    if (!Running_.load(std::memory_order::relaxed)) { <- (a0)
        std::atomic_thread_fence(std::memory_order::acquire); <- (b0)
        if (!Graceful_.load(std::memory_order::relaxed)) { <- (c0)
            Queue_.DrainProducer();
        }

        return GetCpuInstant();
    }

    Relevant part of Shutdown:
    if (graceful) {
        Graceful_.store(true, std::memory_order::relaxed); <- (d0)
    }

    Running_.store(false, std::memory_order::release); <- (e0)

    Suppose we read false in a0. Then we must has (e0) -rf-> (a0)
    rf is reads-from. Then we have (a0) -sb-> (b0)
    and b0 is acquire-fence, thus (e0) -rf-> (a0) -sb-> (b0)
    and so (e0) -SW-> (b0) (SW is synchronizes with) implying
    (e0) -SIHB-> (b0) (simply happens before). By transitivity
    (d0) (if it happened) is SIHB (b0) and thus (c0) too.
    (d0) -SIHB-> (c0) and (d0) being the last modification
    in modification order implies that (c0) must be reading
    from (d0) and thus (c0) always reads the correct value.

    Now, let's deal with 2). Suppose otherwise. In this case
    (c) reads |true| and (f) doesn't observe result of (a).
    We shall model (a) as release RMW (currently it is
    a seq_cst rmw, but this is suboptimal) and (f)
    as acquire RMW (again currently seq_cst but can be
    optimized). Then we have execution
        T1(Enqueue)                 T2(Shutdown)
    RMW^rel(Queue_, 0, 1)   (a)  W^rlx(Running_, false) (d)
    Fence^sc                (b)  Fence^sc               (e)
    R^rlx(Running_, true)   (c)  RMW^acq(Queue_, 0, 0)  (f)

    Here RMW^rel(Queue_, 0, 1) means an rmw op on Queue_ state
    which reads 0 (empty queue) and writes 1 (some callback address).

    Since (f) doesn't read result of (a) then it must read value of
    another modification (either ctor or another dequeue) which
    preceedes (a) in modification order. Thus we have (f) -cob-> (a)
    (cob is coherence-ordered before, https://eel.is/c++draft/atomics.order#3.3).
    For fences we have (e) -sb-> (f) (sequenced before) and (a) -sb-> (b).
    Thus (e) -sb-> (f) -cob-> (a) -sb-> (b) => (e) -S-> (b)
    (Here S is total order on sequentially consistent events,
    see https://eel.is/c++draft/atomics.order#4.4).
    Like-wise, (c) -cob-> (d) because it must be reading from
    another modification prior to (d) in modification order.
    (b) -sb-> (c) -cob-> (d) -sb-> (e) => (b) -S-> (e)
    and we have a loop in S, contracting the assumption.

    NB(arkady-e1ppa): We do force-drain after (c) and (c')
    because otherwise we could be an Enqueue happening
    concurrently to a processing thread draining the queue
    and leave our callback in queue forever.
*/

////////////////////////////////////////////////////////////////////////////////

template <class TQueueImpl>
TInvokerQueue<TQueueImpl>::TInvokerQueue(
    TIntrusivePtr<NThreading::TEventCount> callbackEventCount,
    const TTagSet& counterTagSet,
    NProfiling::IRegistryImplPtr registry)
    : CallbackEventCount_(std::move(callbackEventCount))
{
    Counters_.push_back(CreateCounters(counterTagSet, std::move(registry)));
}

template <class TQueueImpl>
TInvokerQueue<TQueueImpl>::TInvokerQueue(
    TIntrusivePtr<NThreading::TEventCount> callbackEventCount,
    const std::vector<TTagSet>& counterTagSets,
    const std::vector<NYTProf::TProfilerTagPtr>& profilerTags,
    NProfiling::IRegistryImplPtr registry)
    : CallbackEventCount_(std::move(callbackEventCount))
{
    YT_VERIFY(counterTagSets.size() == profilerTags.size());

    Counters_.reserve(counterTagSets.size());
    for (const auto& tagSet : counterTagSets) {
        Counters_.push_back(CreateCounters(tagSet, registry));
    }

    ProfilingTagSettingInvokers_.reserve(Counters_.size());
    for (int index = 0; index < std::ssize(Counters_); ++index) {
        ProfilingTagSettingInvokers_.push_back(
            New<TProfilingTagSettingInvoker<TQueueImpl>>(MakeWeak(this), index, profilerTags[index]));
    }
}

template <class TQueueImpl>
void TInvokerQueue<TQueueImpl>::SetThreadId(TThreadId threadId)
{
    ThreadId_ = threadId;
}

template <class TQueueImpl>
void TInvokerQueue<TQueueImpl>::Invoke(TClosure callback)
{
    YT_ASSERT(Counters_.size() == 1);
    Invoke(std::move(callback), /*profilingTag*/ 0, /*profilerTag*/ nullptr);
}

template <class TQueueImpl>
void TInvokerQueue<TQueueImpl>::Invoke(TMutableRange<TClosure> callbacks)
{
    EnqueueCallbacks(callbacks);
    CallbackEventCount_->NotifyMany(std::ssize(callbacks));
}

template <class TQueueImpl>
void TInvokerQueue<TQueueImpl>::Invoke(
    TClosure callback,
    NProfiling::TTagId profilingTag,
    TProfilerTagPtr profilerTag)
{
    EnqueueCallback(std::move(callback), profilingTag, profilerTag);
    CallbackEventCount_->NotifyOne();
}

template <class TQueueImpl>
TEnqueuedAction TInvokerQueue<TQueueImpl>::MakeAction(
    TClosure callback,
    NProfiling::TTagId profilingTag,
    TProfilerTagPtr profilerTag,
    TCpuInstant cpuInstant)
{
    YT_ASSERT(callback);
    YT_ASSERT(profilingTag >= 0 && profilingTag < std::ssize(Counters_));

    YT_LOG_TRACE("Callback enqueued (Callback: %v, ProfilingTag: %v)",
        callback.GetHandle(),
        profilingTag);

    return {
        .Finished = false,
        .EnqueuedAt = cpuInstant,
        .Callback = std::move(callback),
        .ProfilingTag = profilingTag,
        .ProfilerTag = std::move(profilerTag)
    };
}

template <class TQueueImpl>
TCpuInstant TInvokerQueue<TQueueImpl>::EnqueueCallback(
    TClosure callback,
    NProfiling::TTagId profilingTag,
    TProfilerTagPtr profilerTag)
{
    // NB: We are likely to never read false here
    // so we do relaxed load but acquire fence in
    // IF branch.
    if (!Running_.load(std::memory_order::relaxed)) {
        std::atomic_thread_fence(std::memory_order::acquire);
        TryDrainProducer();
        YT_LOG_TRACE(
            "Queue had been shut down, incoming action ignored (Callback: %v)",
            callback.GetHandle());
        return GetCpuInstant();
    }

    auto cpuInstant = GetCpuInstant();

    auto action = MakeAction(std::move(callback), profilingTag, std::move(profilerTag), cpuInstant);

    if (Counters_[profilingTag]) {
        Counters_[profilingTag]->ActiveCallbacks += 1;
        Counters_[profilingTag]->EnqueuedCounter.Increment();
    }

    QueueImpl_.Enqueue(std::move(action)); // <- (a)

    std::atomic_thread_fence(std::memory_order::seq_cst); // <- (b)
    if (!Running_.load(std::memory_order::relaxed)) { // <- (c)
        TryDrainProducer(/*force*/ true);
        YT_LOG_TRACE(
            "Queue had been shut down concurrently, incoming action ignored (Callback: %v)",
            callback.GetHandle());
    }

    return cpuInstant;
}

template <class TQueueImpl>
TCpuInstant TInvokerQueue<TQueueImpl>::EnqueueCallbacks(
    TMutableRange<TClosure> callbacks,
    NProfiling::TTagId profilingTag,
    TProfilerTagPtr profilerTag)
{
    auto cpuInstant = GetCpuInstant();

    if (!Running_.load(std::memory_order::relaxed)) {
        std::atomic_thread_fence(std::memory_order::acquire);
        TryDrainProducer();
        YT_LOG_TRACE(
            "Queue had been shut down, incoming actions ignored");
        return cpuInstant;
    }

    std::vector<TEnqueuedAction> actions;
    actions.reserve(callbacks.size());

    for (auto& callback : callbacks) {
        actions.push_back(MakeAction(std::move(callback), profilingTag, profilerTag, cpuInstant));
    }

    if (Counters_[profilingTag]) {
        Counters_[profilingTag]->ActiveCallbacks += std::ssize(actions);
        Counters_[profilingTag]->EnqueuedCounter.Increment(std::ssize(actions));
    }

    QueueImpl_.Enqueue(actions); // <- (a')

    std::atomic_thread_fence(std::memory_order::seq_cst); // <- (b')
    if (!Running_.load(std::memory_order::relaxed)) { // <- (c')
        TryDrainProducer(/*force*/ true);
        YT_LOG_TRACE(
            "Queue had been shut down concurrently, incoming actions ignored");
        return cpuInstant;
    }

    return cpuInstant;
}

template <class TQueueImpl>
TThreadId TInvokerQueue<TQueueImpl>::GetThreadId() const
{
    return ThreadId_;
}

template <class TQueueImpl>
bool TInvokerQueue<TQueueImpl>::CheckAffinity(const IInvokerPtr& invoker) const
{
    return invoker.Get() == this;
}

template <class TQueueImpl>
bool TInvokerQueue<TQueueImpl>::IsSerialized() const
{
    return QueueImpl_.HasSingleConsumer();
}

template <class TQueueImpl>
void TInvokerQueue<TQueueImpl>::Shutdown(bool graceful)
{
    // This is done to protect both Graceful_
    // and Running_ to be modified independently.
    if (Stopping_.exchange(true, std::memory_order::relaxed)) {
        return;
    }

    if (graceful) {
        Graceful_.store(true, std::memory_order::relaxed);
    }

    Running_.store(false, std::memory_order::release); // <- (d)

    std::atomic_thread_fence(std::memory_order::seq_cst); // <- (e)

    if (!graceful) {
        // NB: There may still be tasks in
        // local part of the Queue in case
        // of single consumer.
        // One must drain it after stopping
        // consumer.
        QueueImpl_.DrainProducer(); // <- (f)
    }
}

template <class TQueueImpl>
void TInvokerQueue<TQueueImpl>::OnConsumerFinished()
{
    QueueImpl_.DrainProducer();
    QueueImpl_.DrainConsumer();
}

template <class TQueueImpl>
bool TInvokerQueue<TQueueImpl>::BeginExecute(TEnqueuedAction* action, typename TQueueImpl::TConsumerToken* token)
{
    YT_ASSERT(action && action->Finished);

    if (!QueueImpl_.TryDequeue(action, token)) {
        return false;
    }

    auto cpuInstant = GetCpuInstant();

    action->StartedAt = cpuInstant;

    auto waitTime = CpuDurationToDuration(action->StartedAt - action->EnqueuedAt);

    if (IsWaitTimeObserverSet_.load()) {
        WaitTimeObserver_(waitTime);
    }

    if (Counters_[action->ProfilingTag]) {
        Counters_[action->ProfilingTag]->DequeuedCounter.Increment();
        Counters_[action->ProfilingTag]->WaitTimer.Record(waitTime);
    }

    if (const auto& profilerTag = action->ProfilerTag) {
        CpuProfilerTagGuard() = TCpuProfilerTagGuard(profilerTag);
    } else {
        CpuProfilerTagGuard() = {};
    }

    SetCurrentInvoker(GetProfilingTagSettingInvoker(action->ProfilingTag));

    return true;
}

template <class TQueueImpl>
void TInvokerQueue<TQueueImpl>::EndExecute(TEnqueuedAction* action)
{
    CpuProfilerTagGuard() = TCpuProfilerTagGuard{};
    SetCurrentInvoker(nullptr);

    YT_ASSERT(action);

    if (action->Finished) {
        return;
    }

    auto cpuInstant = GetCpuInstant();
    action->FinishedAt = cpuInstant;
    action->Finished = true;

    auto timeFromStart = CpuDurationToDuration(action->FinishedAt - action->StartedAt);
    auto timeFromEnqueue = CpuDurationToDuration(action->FinishedAt - action->EnqueuedAt);

    if (Counters_[action->ProfilingTag]) {
        Counters_[action->ProfilingTag]->ExecTimer.Record(timeFromStart);
        Counters_[action->ProfilingTag]->CumulativeTimeCounter.Add(timeFromStart);
        Counters_[action->ProfilingTag]->TotalTimer.Record(timeFromEnqueue);
        Counters_[action->ProfilingTag]->ActiveCallbacks -= 1;
    }
}

template <class TQueueImpl>
typename TQueueImpl::TConsumerToken TInvokerQueue<TQueueImpl>::MakeConsumerToken()
{
    return QueueImpl_.MakeConsumerToken();
}

template <class TQueueImpl>
bool TInvokerQueue<TQueueImpl>::IsEmpty() const
{
    return QueueImpl_.IsEmpty();
}

template <class TQueueImpl>
bool TInvokerQueue<TQueueImpl>::IsRunning() const
{
    return Running_.load(std::memory_order::relaxed);
}

template <class TQueueImpl>
IInvoker* TInvokerQueue<TQueueImpl>::GetProfilingTagSettingInvoker(int profilingTag)
{
    if (ProfilingTagSettingInvokers_.empty()) {
        // Fast path.
        YT_ASSERT(profilingTag == 0);
        return this;
    } else {
        YT_ASSERT(0 <= profilingTag && profilingTag < std::ssize(Counters_));
        return ProfilingTagSettingInvokers_[profilingTag].Get();
    }
}

template <class TQueueImpl>
void TInvokerQueue<TQueueImpl>::RegisterWaitTimeObserver(TWaitTimeObserver waitTimeObserver)
{
    WaitTimeObserver_ = waitTimeObserver;
    auto alreadyInitialized = IsWaitTimeObserverSet_.exchange(true);

    // Multiple observers are forbidden.
    YT_VERIFY(!alreadyInitialized);
}

template <class TQueueImpl>
typename TInvokerQueue<TQueueImpl>::TCountersPtr TInvokerQueue<TQueueImpl>::CreateCounters(const TTagSet& tagSet, NProfiling::IRegistryImplPtr registry)
{
    auto profiler = TProfiler(registry, "/action_queue").WithTags(tagSet).WithHot();

    auto counters = std::make_unique<TCounters>();
    counters->EnqueuedCounter = profiler.Counter("/enqueued");
    counters->DequeuedCounter = profiler.Counter("/dequeued");
    counters->WaitTimer = profiler.Timer("/time/wait");
    counters->ExecTimer = profiler.Timer("/time/exec");
    counters->CumulativeTimeCounter = profiler.TimeCounter("/time/cumulative");
    counters->TotalTimer = profiler.Timer("/time/total");

    profiler.AddFuncGauge("/size", MakeStrong(this), [counters = counters.get()] {
        return counters->ActiveCallbacks.load();
    });

    return counters;
}

template <class TQueueImpl>
void TInvokerQueue<TQueueImpl>::TryDrainProducer(bool force)
{
    if (force || !Graceful_.load(std::memory_order::relaxed)) {
        QueueImpl_.DrainProducer();
    }
}

////////////////////////////////////////////////////////////////////////////////

template class TInvokerQueue<TMpmcQueueImpl>;
template class TInvokerQueue<TMpscQueueImpl>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
