#pragma once

#include "schemeshard.h"
#include "schemeshard_private.h"
#include "schemeshard_tx_infly.h"
#include "schemeshard__operation.h"

#include <ydb/core/tablet_flat/tablet_flat_executed.h>

namespace NKikimr {
namespace NSchemeShard {

struct TStatsId {
    TPathId PathId;
    TTabletId Datashard;

    TStatsId(const TPathId& pathId, const TTabletId datashard = TTabletId(0))
        : PathId(pathId)
        , Datashard(datashard)
    {
    }

    bool operator==(const TStatsId& rhs) const {
        return PathId == rhs.PathId && Datashard == rhs.Datashard;
    }

    struct THash {
        inline size_t operator()(const TStatsId& obj) const {
            return MultiHash(obj.PathId.Hash(), obj.Datashard);
        }
    };
};

template<typename TEvPeriodicStats>
struct TStatsQueueItem {
    typename TEvPeriodicStats::TPtr Ev;
    TStatsId Id;
    TMonotonic Ts;

    TStatsQueueItem(typename TEvPeriodicStats::TPtr ev, const TStatsId& id)
            : Ev(ev)
            , Id(id)
            , Ts(AppData()->MonotonicTimeProvider->Now())
    {}

    TPathId PathId() {
        return Id.PathId;
    }
};

enum EStatsQueueStatus {
    READY,
    NOT_READY
};

template<typename TEvent>
class TStatsQueue {
public:
    using TEventPtr = typename TEvent::TPtr;
    using TItem = TStatsQueueItem<TEvent>;
    using TStatsMap = THashMap<TStatsId, TItem*, TStatsId::THash>;
    using TStatsQ = TStatsQueue<TEvent>;

    TStatsQueue(TSchemeShard* ss, ESimpleCounters queueSizeCounter, ECumulativeCounters writtenCounter, EPercentileCounters latencyCounter)
        : QueueSizeCounter(queueSizeCounter)
        , WrittenCounter(writtenCounter)
        , LatencyCounter(latencyCounter)
        , SS(ss)
    {}

    EStatsQueueStatus Add(TStatsId statsId, TEventPtr ev);
    TItem Next();

    TDuration Age() const;
    TDuration Delay() const;

    EStatsQueueStatus Status() const;
    bool Empty() const;
    size_t Size() const;

    bool BatchingEnabled() const;
    TDuration BatchTimeout() const;
    ui32 MaxBatchSize() const;
    TDuration MaxExecuteTime() const;

    void WriteLatencyMetric(const TItem& item);
    void WriteQueueSizeMetric();

    const ESimpleCounters QueueSizeCounter;
    const ECumulativeCounters WrittenCounter;
    const EPercentileCounters LatencyCounter;

private:

    TSchemeShard* SS;

    TStatsMap Map;
    TDeque<TItem> Queue;
};


template<typename TEvent>
class TTxStoreStats: public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
    using TStatsQ = TStatsQueue<TEvent>;
    using TItem = TStatsQueueItem<TEvent>;

protected:
    TStatsQ& Queue;
    bool& PersistStatsPending;

public:
    TTxStoreStats(TSchemeShard* ss, TStatsQ& queue, bool& persistStatsPending)
        : TBase(ss)
        , Queue(queue)
        , PersistStatsPending(persistStatsPending)
    {
    }

    virtual ~TTxStoreStats() = default;

    TTxType GetTxType() const override {
        return TXTYPE_STORE_PARTITION_STATS;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;

    // returns true to continue batching
    virtual bool PersistSingleStats(const TPathId& pathId, const TItem& item, TTransactionContext& txc, const TActorContext& ctx) = 0;

    virtual void ScheduleNextBatch(const TActorContext& ctx) = 0;
};

} // NSchemeShard
} // NKikimr
