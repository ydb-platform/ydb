#pragma once

#include "schemeshard_impl.h"

namespace NKikimr {
namespace NSchemeShard {

template<typename TEvent>
EStatsQueueStatus TStatsQueue<TEvent>::Add(TStatsId statsId, TEventPtr ev) {
    typename TStatsMap::insert_ctx insertCtx;
    auto it = Map.find(statsId, insertCtx);
    if (it == Map.end()) {
        Queue.emplace_back(ev, statsId);
        Map.emplace_direct(insertCtx, statsId, &Queue.back());
    } else {
        // already in queue, just update
        it->second->Ev = ev;
    }

    WriteQueueSizeMetric();

    return Status();
}

template<typename TEvent>
TDuration TStatsQueue<TEvent>::Age() const {
    if (Empty()) {
        return TDuration::Zero();
    }
    const auto& oldestItem = Queue.front();
    return AppData()->MonotonicTimeProvider->Now() - oldestItem.Ts;
}

template<typename TEvent>
bool TStatsQueue<TEvent>::BatchingEnabled() const {
    return SS->StatsMaxBatchSize > 0;
}

template<typename TEvent>
TDuration TStatsQueue<TEvent>::BatchTimeout() const {
     return SS->StatsBatchTimeout; 
}

template<typename TEvent>
TDuration TStatsQueue<TEvent>::Delay() const {
    return BatchTimeout() - Age();
}

template<typename TEvent>
bool TStatsQueue<TEvent>::Empty() const {
     return Queue.empty(); 
}

template<typename TEvent>
ui32 TStatsQueue<TEvent>::MaxBatchSize() const {
     return std::max<ui32>(SS->StatsMaxBatchSize, 1); 
}

template<typename TEvent>
TDuration TStatsQueue<TEvent>::MaxExecuteTime() const { 
    return SS->StatsMaxExecuteTime; 
}

template<typename TEvent>
TStatsQueueItem<TEvent> TStatsQueue<TEvent>::Next() {
        auto item = Queue.front();
        Queue.pop_front();

        Map.erase(item.Id);

        return item;
}

template<typename TEvent>
EStatsQueueStatus TStatsQueue<TEvent>::Status() const {
    if (Empty()) {
        return NOT_READY;
    }

    if (!BatchingEnabled()) {
        return READY;
    }

    if (Size() >= MaxBatchSize()) {
        return READY;
    }

    if (!BatchTimeout() || Age() >= BatchTimeout()) {
        return READY;
    }

    return NOT_READY;
}

template<typename TEvent>
size_t TStatsQueue<TEvent>::Size() const { 
    return Queue.size(); 
}

template<typename TEvent>
void TStatsQueue<TEvent>::WriteLatencyMetric(const TItem& item) {
    auto timeInQueue = AppData()->MonotonicTimeProvider->Now() - item.Ts;
    SS->TabletCounters->Percentile()[LatencyCounter].IncrementFor(timeInQueue.MicroSeconds());
}

template<typename TEvent>
void TStatsQueue<TEvent>::WriteQueueSizeMetric() {
    SS->TabletCounters->Simple()[QueueSizeCounter].Set(Size());
}


template<typename TEvent>
bool TTxStoreStats<TEvent>::Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) {
    PersistStatsPending = false;

    if (Queue.Empty()) {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "TTxStoreStats::Execute empty");
        return true;
    }

    TMonotonic start = TMonotonic::Now();
    const ui32 maxBatchSize = Queue.MaxBatchSize();
    ui32 batchSize = 0;
    for (; batchSize < maxBatchSize && !Queue.Empty(); ++batchSize) {
        auto item = Queue.Next();
        Queue.WriteLatencyMetric(item);
        if (!PersistSingleStats(item.PathId(), item, txc, ctx)) {
            break;
        }

        if ((TMonotonic::Now() - start) >= Queue.MaxExecuteTime()) {
            break;
        }
    }

    Self->TabletCounters->Cumulative()[Queue.WrittenCounter].Increment(batchSize);

    if (READY == Queue.Status()) {
        ScheduleNextBatch(ctx);
    }

    return true;
}

} // NSchemeShard
} // NKikimr
