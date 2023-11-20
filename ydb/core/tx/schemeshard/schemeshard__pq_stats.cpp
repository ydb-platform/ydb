#include "schemeshard_impl.h"
#include "schemeshard__stats_impl.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/cputime.h>
#include <ydb/core/protos/sys_view.pb.h>

namespace NKikimr {
namespace NSchemeShard {

class TTxStoreTopicStats: public TTxStoreStats<TEvPersQueue::TEvPeriodicTopicStats> {
    TSideEffects MergeOpSideEffects;

public:
    TTxStoreTopicStats(TSchemeShard* ss, TStatsQueue<TEvPersQueue::TEvPeriodicTopicStats>& queue, bool& persistStatsPending)
        : TTxStoreStats(ss, queue, persistStatsPending)
    {
    }

    virtual ~TTxStoreTopicStats() = default;

    void Complete(const TActorContext& ) override;

    // returns true to continue batching
    bool PersistSingleStats(const TPathId& pathId, const TStatsQueue<TEvPersQueue::TEvPeriodicTopicStats>::TItem& item, TTransactionContext& txc, const TActorContext& ctx) override;
    void ScheduleNextBatch(const TActorContext& ctx) override;
};


bool TTxStoreTopicStats::PersistSingleStats(const TPathId& pathId, const TStatsQueueItem<TEvPersQueue::TEvPeriodicTopicStats>& item, TTransactionContext& txc, const TActorContext& ctx) {
    const auto& rec = item.Ev->Get()->Record;

    TTopicStats newStats;
    newStats.SeqNo = TMessageSeqNo(rec.GetGeneration(), rec.GetRound());
    newStats.DataSize = rec.GetDataSize();
    newStats.UsedReserveSize = rec.GetUsedReserveSize();

    if (newStats.DataSize < newStats.UsedReserveSize) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
               "Got wrong periodic topic stats at partition " << pathId
                        << ". DataSize must be greater than or equal to UsedReserveSize but "
                        << " DataSize " << rec.GetDataSize()
                        << " UsedReserveSize " << rec.GetUsedReserveSize());
        return true;
    }

    const auto it = Self->Topics.find(pathId);
    if (it == Self->Topics.end()) {
        return true;
    }

    auto& topic = it->second;
    auto& oldStats = topic->Stats;

    if (newStats.SeqNo <= oldStats.SeqNo) {
        // Ignore outdated message
        return true;
    }

    auto subDomainInfo = Self->ResolveDomainInfo(pathId);
    subDomainInfo->AggrDiskSpaceUsage(newStats, oldStats);

    oldStats = newStats;

    NIceDb::TNiceDb db(txc.DB);

    Self->PersistPersQueueGroupStats(db, pathId, newStats);
    Self->ChangeDiskSpaceTopicsTotalBytes(subDomainInfo->GetPQAccountStorage());

    if (subDomainInfo->CheckDiskSpaceQuotas(Self)) {
        auto subDomainId = Self->ResolvePathIdForDomain(pathId);
        Self->PersistSubDomainState(db, subDomainId, *subDomainInfo);

        // Publish is done in a separate transaction, so we may call this directly
        TDeque<TPathId> toPublish;
        toPublish.push_back(subDomainId);
        Self->PublishToSchemeBoard(TTxId(), std::move(toPublish), ctx);
    }

    return true;
}

void TTxStoreTopicStats::Complete(const TActorContext&) {
    Queue.WriteQueueSizeMetric();
}

void TTxStoreTopicStats::ScheduleNextBatch(const TActorContext& ctx) {
    Self->ExecuteTopicStatsBatch(ctx);
}


void TSchemeShard::Handle(TEvPersQueue::TEvPeriodicTopicStats::TPtr& ev, const TActorContext& ctx) {
    const auto& rec = ev->Get()->Record;

    const TPathId pathId = TPathId(TabletID(), rec.GetPathId());

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
               "Got periodic topic stats at partition " << pathId
                                                        << " DataSize " << rec.GetDataSize()
                                                        << " UsedReserveSize " << rec.GetUsedReserveSize());

    TStatsId statsId(pathId);
    switch(TopicStatsQueue.Add(statsId, ev.Release())) {
        case READY:
            ExecuteTopicStatsBatch(ctx);
            break;

        case NOT_READY:
            ScheduleTopicStatsBatch(ctx);
            break;

        default:
          Y_ABORT("Unknown batch status");
    }
}

void TSchemeShard::Handle(TEvPrivate::TEvPersistTopicStats::TPtr&, const TActorContext& ctx) {
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
           "Started TEvPersistStats at tablet " << TabletID() << ", queue size# " << TopicStatsQueue.Size());

    TopicStatsBatchScheduled = false;
    ExecuteTopicStatsBatch(ctx);
}

void TSchemeShard::ExecuteTopicStatsBatch(const TActorContext& ctx) {
    if (!TopicPersistStatsPending && !TopicStatsQueue.Empty()) {
        LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "Will execute TTxStoreStats, queue# " << TopicStatsQueue.Size());

        TopicPersistStatsPending = true;
        EnqueueExecute(new TTxStoreTopicStats(this, TopicStatsQueue, TopicPersistStatsPending));

        ScheduleTopicStatsBatch(ctx);
    }
}

void TSchemeShard::ScheduleTopicStatsBatch(const TActorContext& ctx) {
    if (!TopicStatsBatchScheduled && !TopicStatsQueue.Empty()) {
        TDuration delay = TopicStatsQueue.Delay();
        LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Will delay TTxStoreTopicStats on# " << delay << ", queue# " << TopicStatsQueue.Size());

        ctx.Schedule(delay, new TEvPrivate::TEvPersistTopicStats());
        TopicStatsBatchScheduled = true;
    }
}

} // NSchemeShard
} // NKikimr
