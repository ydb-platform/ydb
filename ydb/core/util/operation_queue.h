#pragma once

#include "circular_queue.h"
#include "intrusive_heap.h"
#include "token_bucket.h"

#include <ydb/core/base/defs.h>

#include <ydb/library/actors/core/monotonic.h>

#include <util/datetime/base.h>
#include <util/generic/algorithm.h>
#include <util/generic/vector.h>
#include <util/generic/xrange.h>
#include <util/random/shuffle.h>
#include <util/string/join.h>
#include <util/system/compiler.h>

namespace NKikimr {
namespace NOperationQueue {

enum class EStartStatus {
    EOperationRunning,
    EOperationRetry,
    EOperationRemove,
};

class ITimer {
public:
    // asks to call TOperationQueue::Wakeup()
    virtual void SetWakeupTimer(TDuration delta) = 0;

    virtual TMonotonic Now() = 0;
};

template <typename T>
class IStarter {
public:
    virtual ~IStarter() = default;

    virtual EStartStatus StartOperation(const T& item) = 0;

    // in many cases just for metrics/logging, because
    // queue is able to restart operation itself
    virtual void OnTimeout(const T& item) = 0;
};

struct TConfig {
    // after timeout item is considered as done
    TDuration Timeout = TDuration::Zero();

    // if every item in queue failed to start, then
    // retry after this amount of time
    TDuration WakeupInterval = TDuration::Seconds(1);

    // Do not wakeup too often
    TDuration MinWakeupInterval = TDuration::Zero();

    // done and timeouted items are enqueued again
    bool IsCircular = false;

    // shuffle added items on Start()
    bool ShuffleOnStart = false;

    TDuration RoundInterval;

    ui32 InflightLimit = 1;
    double MaxRate = 0.0; // max rate operations/s

    // In case of circular queue start done operation
    // again only after this interval of time
    TDuration MinOperationRepeatDelay = TDuration::Minutes(0);

    TConfig() = default;

    TConfig(TDuration timeout, bool isCircular, ui32 inflight)
        : Timeout(timeout)
        , IsCircular(isCircular)
        , InflightLimit(inflight)
    { }
};

// Rich wrapper over TIntrusiveHeap, so that
// it can be used as queue in TOperationQueue.
template <typename T, class TCompare = TLess<T>>
class TQueueWithPriority {
private:
    struct THeapItem {
        T Item;
        size_t HeapIndex = -1;

        explicit THeapItem(const T& item)
            : Item(item)
        {}

        explicit THeapItem(T&& item)
            : Item(std::move(item))
        {}

        bool operator ==(const THeapItem& rhs) const {
            return Item == rhs.Item;
        }

        explicit operator size_t() const {
            return size_t(Item);
        }

        struct THeapIndex {
            size_t& operator ()(THeapItem& item) const {
                return item.HeapIndex;
            }
        };

        struct THeapItemCompare {
            bool operator()(const THeapItem& lhs, const THeapItem& rhs) const {
                return TCompare()(lhs.Item, rhs.Item);
            }
        };
    };

    using TItemsSet = THashSet<THeapItem>;

    TItemsSet Items;
    TIntrusiveHeap<THeapItem, typename THeapItem::THeapIndex, typename THeapItem::THeapItemCompare> Heap;

public:
    TQueueWithPriority() = default;

    bool Contains(const T& item) const {
        THeapItem heapItem(item);
        return Items.find(heapItem) != Items.end();
    }

    template<typename T2>
    bool Enqueue(T2&& item) {
        THeapItem heapItem(std::forward<T2>(item));
        typename TItemsSet::insert_ctx insert_ctx;
        auto it = Items.find(heapItem, insert_ctx);
        if (it != Items.end()) {
            // seems to be ok to simply ignore
            return false;
        }

        auto indexIt = Items.emplace_direct(insert_ctx, std::move(heapItem));
        Heap.Add(const_cast<THeapItem*>(&*indexIt));

        return true;
    }

    bool Remove(const T& item) {
        THeapItem heapItem(item);

        auto it = Items.find(heapItem);
        if (it == Items.end())
            return false;

        // note the order of Heap->Items
        Heap.Remove(const_cast<THeapItem*>(&*it));
        Items.erase(it);
        return true;
    }

    bool UpdateIfFound(const T& item) {
        THeapItem heapItem(item);
        auto it = Items.find(heapItem);
        if (it == Items.end())
            return false;

        auto& ref = const_cast<THeapItem&>(*it);
        ref.Item = item;
        Heap.Update(&ref);
        return true;
    }

    void Clear() {
        Heap.Clear();
        Items.clear();
    }

    const T& Front() const {
        return Heap.Top()->Item;
    }

    void PopFront() {
        Remove(Front());
    }

    bool Empty() const {
        return Heap.Empty();
    }

    size_t Size() const {
        return Heap.Size();
    }
};

template <typename T, typename TQueue>
class TOperationQueue {
public:

    struct TItemWithTs {
        T Item;
        TMonotonic Timestamp;

        explicit TItemWithTs(const T& item)
            : Item(item)
        { }

        TItemWithTs(const T& item, TMonotonic s)
            : Item(item)
            , Timestamp(s)
        { }

        TItemWithTs(T&& item, TMonotonic s)
            : Item(std::move(item))
            , Timestamp(s)
        { }

        TItemWithTs(const TItemWithTs&) = default;

        bool operator ==(const TItemWithTs& rhs) const {
            return Item == rhs.Item;
        }

        TItemWithTs& operator =(const TItemWithTs& rhs) {
            Item = rhs.Item;
            if (rhs.Timestamp) {
                // avoid clearing ts, little bit hacky:
                // in UpdateIfFound() we don't want to copy ts,
                // but in other places we want.
                // Though in UpdateIfFound rhs.Timestamp should
                // be always missing while in other cases it
                // always presents
                Timestamp = rhs.Timestamp;
            }
            return *this;
        }

        size_t Hash() const {
            return THash<T>()(Item);
        }

        explicit operator size_t() const {
            return Hash();
        }
    };

    using IStarter = IStarter<T>;
    using TConfig = ::NKikimr::NOperationQueue::TConfig;

private:
    using TRunningItems = TFifoQueue<TItemWithTs>;
    using TWaitingItems = TFifoQueue<TItemWithTs>;

private:
    TConfig Config;
    IStarter& Starter;
    ITimer& Timer;

    // used only with ShuffleOnStart
    TVector<T> ItemsToShuffle;

    TQueue ReadyQueue;
    TRunningItems RunningItems;
    TWaitingItems WaitingItems;

    TTokenBucketBase<TMonotonic> TokenBucket;
    bool HasRateLimit = false;

    TMonotonic NextWakeup;
    bool Running = false;
    bool WasRunning = false;

    // operations / s
    double Rate = 0;

public:
    TOperationQueue(const TConfig& config,
                    IStarter& starter,
                    ITimer& timer)
        : Config(config)
        , Starter(starter)
        , Timer(timer)
    {
        UpdateConfig(config);
    }

    template <typename TReadyQueueConfig>
    TOperationQueue(const TConfig& config,
                    const TReadyQueueConfig& queueConfig,
                    IStarter& starter,
                    ITimer& timer)
        : Config(config)
        , Starter(starter)
        , Timer(timer)
    {
        UpdateConfig(config, queueConfig);
    }

    template <typename TReadyQueueConfig>
    void UpdateConfig(const TConfig& config, const TReadyQueueConfig& queueConfig) {
        UpdateConfig(config);
        ReadyQueue.UpdateConfig(queueConfig);
    }

    void UpdateConfig(const TConfig& config) {
        if (&Config != &config)
            Config = config;

        UpdateRate();
    }

    void UpdateRate() {
        if (!Config.MaxRate && !Config.RoundInterval) {
            HasRateLimit = false;
            Rate = 0;
            TokenBucket.SetUnlimited();
            return;
        }

        Rate = Config.MaxRate;
        if (Config.RoundInterval && TotalQueueSize() > 0) {
            double rateByInterval = TotalQueueSize() / (double)Config.RoundInterval.Seconds();
            if (Config.MaxRate)
                rateByInterval = Min(rateByInterval, Config.MaxRate);
            Rate = rateByInterval;
        }

        HasRateLimit = false;
        if (Rate) {
            // by default token bucket is unlimitted, so
            // configure only when rate is limited
            TokenBucket.SetCapacity(Config.InflightLimit);
            TokenBucket.SetRate(Rate);
            TokenBucket.Fill(Timer.Now());
            HasRateLimit = true;
        }
    }

    // Items enqueued before Start() will start
    void Start();

    // Can be used to pause, note that running items continue to run and
    // need either OnDone() or timeout via Wakeup()
    void Stop();

    // returns true when item enqueued and false if it was already in queue.
    //
    // Note that when ShuffleOnStart is set, then it always returns true and
    // duplicates are eliminated only when queue starts
    bool Enqueue(const T& item);

    // returns true if item was in queue and removed, should
    // not be called before Start() with ShuffleOnStart, because
    // it will be O(N).
    //
    // note, that in case of circular queue it is possible
    // to remove item which was already started: it's up
    // to caller to stop it if needed.
    bool Remove(const T& item);

    // updates operation either running/waiting/ready by
    // copying item. Used in priority queues to modify
    // priority
    bool Update(const T& item);

    // done or failed doesn't matter, if item was running then
    // returns duration between start and done
    // Note that in case of circular queue item overwrites running
    // item, which is useful, when new priority needed
    TDuration OnDone(const T& item);

    // must be called by Starter on response to ITimer::SetWakeupTimer()
    void Wakeup();

    // Consider the case when there are running items, user clears the queue and:
    // 1. Adds items with same ID and queue starts them
    // 2. Calls OnDone() because previously added items done, but not new ones
    void Clear();

    // note that it is size of both ready and waiting queue, not running
    size_t Size() const { return ReadyQueue.Size() + WaitingItems.Size() + ItemsToShuffle.size(); }
    bool Empty() const { return ReadyQueue.Empty() && WaitingItems.Empty() && ItemsToShuffle.empty(); }

    size_t RunningSize() const { return RunningItems.Size(); }
    bool RunningEmpty() const { return RunningItems.Empty(); }

    size_t WaitingSize() const { return WaitingItems.Size(); }
    bool WaitingEmpty() const { return WaitingItems.Empty(); }

    size_t TotalQueueSize() const {
        if (Config.IsCircular)
            return Size() + RunningSize();
        return Size();
    }

    double GetRate() const { return Rate; }

    const TQueue& GetReadyQueue() const { return ReadyQueue; }

    // copies items, should be used in tests only
    TVector<T> GetQueue() const { return ReadyQueue.GetQueue(); }
    TVector<TItemWithTs> GetRunning() const { return RunningItems.GetQueue(); }
    TVector<TItemWithTs> GetWaiting() const { return WaitingItems.GetQueue(); }

    void Dump(IOutputStream& out) const;

private:
    template <typename T2>
    bool EnqueueNoStart(T2&& item);

    template <typename T2>
    void ReEnqueueNoStart(T2&& item);

    void CheckTimeoutOperations();
    void CheckWaitingOperations();
    void StartOperations();
    void ScheduleWakeup();
};

template <typename T, typename TQueue>
void TOperationQueue<T, TQueue>::Start() {
    if (Running)
        return;

    if (Config.ShuffleOnStart && !ItemsToShuffle.empty()) {
        ShuffleRange(ItemsToShuffle);
        for (auto& item: ItemsToShuffle) {
            EnqueueNoStart(std::move(item));
        }
        TVector<T>().swap(ItemsToShuffle);
    }

    Running = true;
    WasRunning = true;
    StartOperations();
}

template <typename T, typename TQueue>
void TOperationQueue<T, TQueue>::Stop() {
    Running = false;
}

template <typename T, typename TQueue>
bool TOperationQueue<T, TQueue>::Enqueue(const T& item) {
    // note that there is no reason to reshuffle on start/stop
    // also waiting/timeouted operations enqueued vid EnqueueNoStart
    // thus don't use shuffling
    if (!WasRunning && Config.ShuffleOnStart) {
        ItemsToShuffle.push_back(item);
        return true; // we don't check if item is unique
    }

    if (EnqueueNoStart(item)) {
        StartOperations();
        return true;
    }
    return false;
}

template <typename T, typename TQueue>
template <typename T2>
bool TOperationQueue<T, TQueue>::EnqueueNoStart(T2&& item) {
    bool res = ReadyQueue.Enqueue(std::forward<T2>(item));
    if (res)
        UpdateRate();

    return res;
}

template <typename T, typename TQueue>
template <typename T2>
void TOperationQueue<T, TQueue>::ReEnqueueNoStart(T2&& item) {
    auto now = Timer.Now();
    if (Config.MinOperationRepeatDelay) {
        TItemWithTs runningItem(std::forward<T2>(item), now);
        WaitingItems.PushBack(runningItem);
    } else {
        EnqueueNoStart(std::forward<T2>(item));
    }
}

template <typename T, typename TQueue>
bool TOperationQueue<T, TQueue>::Remove(const T& item) {
    bool removed = ReadyQueue.Remove(item);

    if (RunningItems.Remove(TItemWithTs(item))) {
        StartOperations();
    }

    removed |= WaitingItems.Remove(TItemWithTs(item));

    if (ItemsToShuffle) {
        auto it = Find(ItemsToShuffle.begin(), ItemsToShuffle.end(), item);
        if (it != ItemsToShuffle.end()) {
            ItemsToShuffle.erase(it);
            return true;
        }
    }

    if (removed)
        UpdateRate();

    return removed;
}

template <typename T, typename TQueue>
bool TOperationQueue<T, TQueue>::Update(const T& item) {
    // note that no ts and below we will keep original ts
    TItemWithTs tsItem(item);

    return ReadyQueue.UpdateIfFound(item) ||
        WaitingItems.UpdateIfFound(tsItem) ||
        RunningItems.UpdateIfFound(tsItem);
}

template <typename T, typename TQueue>
TDuration TOperationQueue<T, TQueue>::OnDone(const T& item) {
    TItemWithTs runningItem(item);
    TDuration d;
    if (RunningItems.CopyAndRemove(runningItem)) {
        auto now = Timer.Now();
        d = now - runningItem.Timestamp;
        if (Config.IsCircular) {
            ReEnqueueNoStart(item);
        }
        StartOperations();
    }

    return d;
}

template <typename T, typename TQueue>
void TOperationQueue<T, TQueue>::Wakeup() {
    NextWakeup = {};
    StartOperations();
    if (!NextWakeup)
        ScheduleWakeup();
}

template <typename T, typename TQueue>
void TOperationQueue<T, TQueue>::Clear() {
    ReadyQueue.Clear();
    RunningItems.Clear();
    WaitingItems.Clear();
    ItemsToShuffle.clear();
}

template <typename T, typename TQueue>
void TOperationQueue<T, TQueue>::CheckTimeoutOperations() {
    if (!Config.Timeout)
        return;

    if (RunningItems.Empty())
        return;

    auto now = Timer.Now();
    while (!RunningItems.Empty()) {
        const auto& item = RunningItems.Front();
        if (item.Timestamp + Config.Timeout <= now) {
            auto movedItem = std::move(item.Item);

            // we want to pop before calling OnTimeout, because
            // it might want to enqueue in case of non-circular
            // queue
            RunningItems.PopFront();

            // note that OnTimeout() can enqueue item back
            // in case of non-circular queue
            Starter.OnTimeout(movedItem);

            if (Config.IsCircular)
                ReEnqueueNoStart(std::move(movedItem));
            continue;
        }
        break;
    }
}

template <typename T, typename TQueue>
void TOperationQueue<T, TQueue>::CheckWaitingOperations() {
    if (WaitingItems.Empty())
        return;

    auto now = Timer.Now();
    while (!WaitingItems.Empty()) {
        const auto& item = WaitingItems.Front();
        if (item.Timestamp + Config.MinOperationRepeatDelay > now)
            break;
        EnqueueNoStart(std::move(item.Item));
        WaitingItems.PopFront();
    }
}

template <typename T, typename TQueue>
void TOperationQueue<T, TQueue>::StartOperations() {
    CheckTimeoutOperations();
    CheckWaitingOperations();

    if (!Running)
        return;

    if ((ReadyQueue.Empty() && WaitingItems.Empty()) || RunningItems.Size() == Config.InflightLimit)
        return;

    Y_ABORT_UNLESS(RunningItems.Size() < Config.InflightLimit);

    auto now = Timer.Now();
    TokenBucket.Fill(now);
    auto maxTries = ReadyQueue.Size();
    for (size_t tries = 0; RunningItems.Size() != Config.InflightLimit && tries < maxTries && TokenBucket.Available() >= 0; ++tries) {
        auto item = ReadyQueue.Front();
        auto status = Starter.StartOperation(item);
        ReadyQueue.PopFront();
        switch (status) {
        case EStartStatus::EOperationRunning:
            TokenBucket.Take(1);
            RunningItems.PushBack(TItemWithTs(std::move(item), now));
            break;
        case EStartStatus::EOperationRetry:
            ReadyQueue.Enqueue(std::move(item));
            break;
        case EStartStatus::EOperationRemove:
            break;
        }
    }

    ScheduleWakeup();
}

template <typename T, typename TQueue>
void TOperationQueue<T, TQueue>::ScheduleWakeup() {
    if (RunningItems.Empty() && !Running)
        return;

    auto now = Timer.Now();
    auto wakeup = TMonotonic::Max();

    if (RunningItems.Empty() && !ReadyQueue.Empty()) {
        if (TokenBucket.Available() <= 0) {
            // we didn't start anything because of RPS limit
            wakeup = now + TokenBucket.NextAvailableDelay();
        } else if (!NextWakeup) {
            // special case when we failed to start anything
            wakeup = now + Config.WakeupInterval;
        }
    } else {
        // note, that by design we should have remove timeouted items,
        // thus assume that timeout is in future
        if (Config.Timeout && !RunningItems.Empty()) {
            const auto& item = RunningItems.Front();
            wakeup = Min(wakeup, item.Timestamp + Config.Timeout);
        }

        if (!WaitingItems.Empty()) {
            const auto& item = WaitingItems.Front();
            wakeup = Min(wakeup, item.Timestamp + Config.MinOperationRepeatDelay);
        }

        // neither timeout will happen or there any waiting items.
        // in this case, queue will be triggered by enqueue operation.
        if (wakeup == TMonotonic::Max())
            return;

        // no sense to wakeup earlier that rate limit allows
        if (HasRateLimit) {
            wakeup = Max(wakeup, now + TokenBucket.NextAvailableDelay());
        }
    }

    // don't wakeup too often (as well as don't wakeup in past)
    wakeup = Max(wakeup, now + Config.MinWakeupInterval);

    if (!NextWakeup) {
        NextWakeup = wakeup;
        Timer.SetWakeupTimer(wakeup - now);
        return;
    }

    if (NextWakeup > wakeup) {
        auto delta = NextWakeup - wakeup;
        if (!Config.MinWakeupInterval || delta > Config.MinWakeupInterval) {
            NextWakeup = wakeup;
            Timer.SetWakeupTimer(wakeup - now);
        }
    }
}

template <typename T, typename TQueue>
void TOperationQueue<T, TQueue>::Dump(IOutputStream& out) const {
    out << "{ ReadyQueue #" << ReadyQueue.Size()
        << " {" << JoinSeq(", ", GetQueue()) << "}, "
        << "Running #" << RunningItems.Size()
        << " {" << JoinSeq(", ", GetRunning()) << "}, "
        << "Waiting #" << WaitingItems.Size()
        << " {" << JoinSeq(", ", GetWaiting()) << "}}";
}

} // NOperationQueue
} // NKikimr
