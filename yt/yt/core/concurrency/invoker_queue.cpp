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

static const auto& Logger = ConcurrencyLogger;

////////////////////////////////////////////////////////////////////////////////

constinit YT_THREAD_LOCAL(TCpuProfilerTagGuard) CpuProfilerTagGuard;

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
    if (!Running_.load(std::memory_order::relaxed)) {
        DrainProducer();
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

    QueueImpl_.Enqueue(std::move(action));
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
        DrainProducer();
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

    QueueImpl_.Enqueue(actions);
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
void TInvokerQueue<TQueueImpl>::Shutdown()
{
    Running_.store(false, std::memory_order::relaxed);
}

template <class TQueueImpl>
void TInvokerQueue<TQueueImpl>::DrainProducer()
{
    YT_VERIFY(!Running_.load(std::memory_order::relaxed));

    QueueImpl_.DrainProducer();
}

template <class TQueueImpl>
void TInvokerQueue<TQueueImpl>::DrainConsumer()
{
    YT_VERIFY(!Running_.load(std::memory_order::relaxed));

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
        GetTlsRef(CpuProfilerTagGuard) = TCpuProfilerTagGuard(profilerTag);
    } else {
        GetTlsRef(CpuProfilerTagGuard) = {};
    }

    SetCurrentInvoker(GetProfilingTagSettingInvoker(action->ProfilingTag));

    return true;
}

template <class TQueueImpl>
void TInvokerQueue<TQueueImpl>::EndExecute(TEnqueuedAction* action)
{
    GetTlsRef(CpuProfilerTagGuard) = TCpuProfilerTagGuard{};
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

////////////////////////////////////////////////////////////////////////////////

template class TInvokerQueue<TMpmcQueueImpl>;
template class TInvokerQueue<TMpscQueueImpl>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
