#include "schemeshard_impl.h"
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/cputime.h>
#include <ydb/core/protos/sys_view.pb.h>

namespace NKikimr {
namespace NSchemeShard {

template <typename T>
static ui64 GetThroughput(const T& c) {
    ui64 acc = 0;
    for (const auto& v : c)
        acc += v.GetThroughput();
    return acc;
}

template <typename T>
static ui64 GetIops(const T& c) {
    ui64 acc = 0;
    for (const auto& v : c)
        acc += v.GetIops();
    return acc;
}

void TSchemeShard::Handle(NSysView::TEvSysView::TEvGetPartitionStats::TPtr& ev, const TActorContext& ctx) {
    if (SysPartitionStatsCollector) {
        ctx.Send(ev->Forward(SysPartitionStatsCollector));
    }
}

auto TSchemeShard::BuildStatsForCollector(TPathId pathId, TShardIdx shardIdx, TTabletId datashardId,
    TMaybe<ui32> nodeId, TMaybe<ui64> startTime, const TPartitionStats& stats)
{
    auto ev = MakeHolder<NSysView::TEvSysView::TEvSendPartitionStats>(
        GetDomainKey(pathId), pathId, std::make_pair(ui64(shardIdx.GetOwnerId()), ui64(shardIdx.GetLocalId())));

    auto& sysStats = ev->Stats;
    sysStats.SetDataSize(stats.DataSize);
    sysStats.SetRowCount(stats.RowCount);
    sysStats.SetIndexSize(stats.IndexSize);
    sysStats.SetCPUCores(std::min(stats.GetCurrentRawCpuUsage() / 1000000., 1.0));
    sysStats.SetTabletId(ui64(datashardId));
    sysStats.SetAccessTime(stats.LastAccessTime.MilliSeconds());
    sysStats.SetUpdateTime(stats.LastUpdateTime.MilliSeconds());
    sysStats.SetInFlightTxCount(stats.InFlightTxCount);
    sysStats.SetRowUpdates(stats.RowUpdates);
    sysStats.SetRowDeletes(stats.RowDeletes);
    sysStats.SetRowReads(stats.RowReads);
    sysStats.SetRangeReads(stats.RangeReads);
    sysStats.SetRangeReadRows(stats.RangeReadRows);
    sysStats.SetImmediateTxCompleted(stats.ImmediateTxCompleted);
    sysStats.SetPlannedTxCompleted(stats.PlannedTxCompleted);
    sysStats.SetTxRejectedByOverload(stats.TxRejectedByOverload);
    sysStats.SetTxRejectedBySpace(stats.TxRejectedBySpace);

    if (nodeId) {
        sysStats.SetNodeId(*nodeId);
    }
    if (startTime) {
        sysStats.SetStartTime(*startTime);
    }

    return ev;
}

class TTxStorePartitionStats: public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
    TSideEffects MergeOpSideEffects;

    struct TMessage {
        TActorId Actor;
        THolder<IEventBase> Event;

        TMessage(const TActorId& actor, IEventBase* event)
            : Actor(actor)
            , Event(event)
        {}
    };

    TVector<TMessage> PendingMessages;

public:
    TTxStorePartitionStats(TSelf* self)
        : TBase(self)
    {
    }

    virtual ~TTxStorePartitionStats() = default;

    TTxType GetTxType() const override {
        return TXTYPE_STORE_PARTITION_STATS;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;

    // returns true to continue batching
    bool PersistSingleStats(TTransactionContext& txc, const TActorContext& ctx);
};

THolder<TProposeRequest> MergeRequest(
    TSchemeShard* ss, TTxId& txId, TPathId& pathId, const TVector<TShardIdx>& shardsToMerge)
{
    auto request = MakeHolder<TProposeRequest>(ui64(txId), ui64(ss->SelfTabletId()));
    auto& record = request->Record;

    TPath tablePath = TPath::Init(pathId, ss);

    auto& propose = *record.AddTransaction();
    propose.SetFailOnExist(false);
    propose.SetOperationType(NKikimrSchemeOp::ESchemeOpSplitMergeTablePartitions);
    propose.SetInternal(true);

    propose.SetWorkingDir(tablePath.Parent().PathString());

    auto& merge = *propose.MutableSplitMergeTablePartitions();
    merge.SetTablePath(tablePath.PathString());
    merge.SetSchemeshardId(ss->TabletID());

    for (auto shardIdx : shardsToMerge) {
        auto tabletId = ss->ShardInfos.at(shardIdx).TabletID;
        merge.AddSourceTabletId(ui64(tabletId));
    }

    return std::move(request);
}

bool TTxStorePartitionStats::Execute(TTransactionContext& txc, const TActorContext& ctx) {
    Self->PersistStatsPending = false;

    if (Self->StatsQueue.empty())
        return true;

    NCpuTime::TCpuTimer timer;

    const ui32 maxBatchSize = Self->StatsMaxBatchSize ? Self->StatsMaxBatchSize : 1;
    ui32 batchSize = 0;
    while (batchSize < maxBatchSize && !Self->StatsQueue.empty()) {
        ++batchSize;
        if (!PersistSingleStats(txc, ctx))
            break;

        if (timer.GetTime() >= Self->StatsMaxExecuteTime)
            break;
    }

    Self->TabletCounters->Cumulative()[COUNTER_STATS_WRITTEN].Increment(batchSize);

    bool isBatchingDisabled = Self->StatsMaxBatchSize == 0;
    if (isBatchingDisabled) {
        // there will be per stat transaction, don't need to schedule additional one
        return true;
    }

    if (!Self->StatsQueue.empty()) {
        Self->ScheduleStatsBatch(ctx);
    }

    return true;
}

bool TTxStorePartitionStats::PersistSingleStats(TTransactionContext& txc, const TActorContext& ctx) {
    auto item = Self->StatsQueue.front();
    Self->StatsQueue.pop_front();

    auto timeInQueue = AppData()->MonotonicTimeProvider->Now() - item.Ts;
    Self->TabletCounters->Percentile()[COUNTER_STATS_BATCH_LATENCY].IncrementFor(timeInQueue.MicroSeconds());

    const auto& rec = item.Ev->Get()->Record;
    auto datashardId = TTabletId(rec.GetDatashardId());
    const TPathId& pathId = item.PathId;

    TSchemeShard::TStatsId statsId(pathId, datashardId);
    Self->StatsMap.erase(statsId);

    const auto& tableStats = rec.GetTableStats();
    const auto& tabletMetrics = rec.GetTabletMetrics();
    ui64 dataSize = tableStats.GetDataSize();
    ui64 rowCount = tableStats.GetRowCount();

    bool isDataShard = Self->Tables.contains(pathId);
    bool isOlapStore = Self->OlapStores.contains(pathId);
    if (!isDataShard && !isOlapStore) {
        LOG_ERROR_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Unexpected stats from shard " << datashardId);
        return true;
    }

    if (!Self->TabletIdToShardIdx.contains(datashardId)) {
        LOG_ERROR_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "No shardIdx for shard " << datashardId);
        return true;
    }

    TShardIdx shardIdx = Self->TabletIdToShardIdx[datashardId];

    TPartitionStats newStats;
    newStats.SeqNo = TMessageSeqNo(rec.GetGeneration(), rec.GetRound());

    newStats.RowCount = tableStats.GetRowCount();
    newStats.DataSize = tableStats.GetDataSize();
    newStats.IndexSize = tableStats.GetIndexSize();
    newStats.LastAccessTime = TInstant::MilliSeconds(tableStats.GetLastAccessTime());
    newStats.LastUpdateTime = TInstant::MilliSeconds(tableStats.GetLastUpdateTime());

    newStats.ImmediateTxCompleted = tableStats.GetImmediateTxCompleted();
    newStats.PlannedTxCompleted = tableStats.GetPlannedTxCompleted();
    newStats.TxRejectedByOverload = tableStats.GetTxRejectedByOverload();
    newStats.TxRejectedBySpace = tableStats.GetTxRejectedBySpace();
    newStats.TxCompleteLag = TDuration::MilliSeconds(tableStats.GetTxCompleteLagMsec());
    newStats.InFlightTxCount = tableStats.GetInFlightTxCount();

    newStats.RowUpdates = tableStats.GetRowUpdates();
    newStats.RowDeletes = tableStats.GetRowDeletes();
    newStats.RowReads = tableStats.GetRowReads();
    newStats.RangeReads = tableStats.GetRangeReads();
    newStats.RangeReadRows = tableStats.GetRangeReadRows();

    TInstant now = AppData(ctx)->TimeProvider->Now();
    newStats.SetCurrentRawCpuUsage(tabletMetrics.GetCPU(), now);
    newStats.Memory = tabletMetrics.GetMemory();
    newStats.Network = tabletMetrics.GetNetwork();
    newStats.Storage = tabletMetrics.GetStorage();
    newStats.ReadThroughput = GetThroughput(tabletMetrics.GetGroupReadThroughput());
    newStats.WriteThroughput = GetThroughput(tabletMetrics.GetGroupWriteThroughput());
    newStats.ReadIops = GetIops(tabletMetrics.GetGroupReadIops());
    newStats.WriteIops = GetIops(tabletMetrics.GetGroupWriteIops());
    newStats.PartCount = tableStats.GetPartCount();
    newStats.SearchHeight = tableStats.GetSearchHeight();
    newStats.FullCompactionTs = tableStats.GetLastFullCompactionTs();
    newStats.MemDataSize = tableStats.GetInMemSize();
    newStats.StartTime = TInstant::MilliSeconds(rec.GetStartTime());
    newStats.HasLoanedData = tableStats.GetHasLoanedParts();
    for (ui64 tabletId : rec.GetUserTablePartOwners()) {
        newStats.PartOwners.insert(TTabletId(tabletId));
        if (tabletId != rec.GetDatashardId()) {
            newStats.HasBorrowedData = true;
        }
    }
    for (ui64 tabletId : rec.GetSysTablesPartOwners()) {
        newStats.PartOwners.insert(TTabletId(tabletId));
    }
    newStats.ShardState = rec.GetShardState();

    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Add stats from shard " << datashardId << ", pathId " << pathId.LocalPathId
                << ": RowCount " << newStats.RowCount << ", DataSize " << newStats.DataSize);

    NIceDb::TNiceDb db(txc.DB);

    TTableInfo::TPtr table;
    TPartitionStats oldAggrStats;
    TPartitionStats newAggrStats;
    bool updateSubdomainInfo = false;

    if (isDataShard) {
        table = Self->Tables[pathId];
        oldAggrStats = table->GetStats().Aggregated;
        table->UpdateShardStats(shardIdx, newStats);

        if (!table->IsBackup) {
            Self->UpdateBackgroundCompaction(shardIdx, newStats);
            Self->UpdateShardMetrics(shardIdx, newStats);
        }

        if (!newStats.HasBorrowedData) {
            Self->RemoveBorrowedCompaction(shardIdx);
        }

        if (!table->IsBackup && !table->IsShardsStatsDetached()) {
            newAggrStats = table->GetStats().Aggregated;
            updateSubdomainInfo = true;
        }

        Self->PersistTablePartitionStats(db, pathId, shardIdx, table);
    } else if (isOlapStore) {
        TOlapStoreInfo::TPtr olapStore = Self->OlapStores[pathId];
        oldAggrStats = olapStore->GetStats().Aggregated;
        olapStore->UpdateShardStats(shardIdx, newStats);
        newAggrStats = olapStore->GetStats().Aggregated;
        updateSubdomainInfo = true;
    }

    if (updateSubdomainInfo) {
        auto subDomainId = Self->ResolveDomainId(pathId);
        auto subDomainInfo = Self->ResolveDomainInfo(pathId);
        subDomainInfo->AggrDiskSpaceUsage(Self, newAggrStats, oldAggrStats);
        if (subDomainInfo->CheckDiskSpaceQuotas(Self)) {
            Self->PersistSubDomainState(db, subDomainId, *subDomainInfo);
            // Publish is done in a separate transaction, so we may call this directly
            TDeque<TPathId> toPublish;
            toPublish.push_back(subDomainId);
            Self->PublishToSchemeBoard(TTxId(), std::move(toPublish), ctx);
        }
    }

    if (AppData(ctx)->FeatureFlags.GetEnableSystemViews()) {
        TMaybe<ui32> nodeId;
        if (rec.HasNodeId()) {
            nodeId = rec.GetNodeId();
        }
        TMaybe<ui64> startTime;
        if (rec.HasStartTime()) {
            startTime = rec.GetStartTime();
        }

        if (Self->SysPartitionStatsCollector) {
            PendingMessages.emplace_back(
                Self->SysPartitionStatsCollector,
                Self->BuildStatsForCollector(pathId, shardIdx, datashardId, nodeId, startTime, newStats).Release());
        }
    }

    if (isOlapStore) {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "Aggregated stats for pathId " << pathId.LocalPathId
                    << ": RowCount " << newAggrStats.RowCount << ", DataSize " << newAggrStats.DataSize);
        return true;
    }

    const auto& shardToPartition = table->GetShard2PartitionIdx();
    if (table->IsTTLEnabled() && shardToPartition.contains(shardIdx)) {
        const ui64 partitionIdx = shardToPartition.at(shardIdx);
        const auto& partitions = table->GetPartitions();

        Y_VERIFY(partitionIdx < partitions.size());
        auto& shardInfo = partitions.at(partitionIdx);
        auto& lag = shardInfo.LastCondEraseLag;

        if (lag) {
            Self->TabletCounters->Percentile()[COUNTER_NUM_SHARDS_BY_TTL_LAG].DecrementFor(lag->Seconds());
        } else {
            Y_VERIFY_DEBUG(false);
        }

        const auto now = ctx.Now();
        if (now >= shardInfo.LastCondErase) {
            lag = now - shardInfo.LastCondErase;
        } else {
            lag = TDuration::Zero();
        }

        Self->TabletCounters->Percentile()[COUNTER_NUM_SHARDS_BY_TTL_LAG].IncrementFor(lag->Seconds());
    }

    const auto forceShardSplitSettings = Self->SplitSettings.GetForceShardSplitSettings();
    TVector<TShardIdx> shardsToMerge;
    if (table->CheckCanMergePartitions(Self->SplitSettings, forceShardSplitSettings, shardIdx, shardsToMerge)) {
        TTxId txId = Self->GetCachedTxId(ctx);

        if (!txId) {
            LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                       "Do not request merge op"
                        << ", reason: no cached tx ids for internal operation"
                        << ", shardIdx: " << shardIdx
                        << ", size of merge: " << shardsToMerge.size());
            return true;
        }

        auto request = MergeRequest(Self, txId, Self->ShardInfos[shardIdx].PathId, shardsToMerge);

        TMemoryChanges memChanges;
        TStorageChanges dbChanges;
        TOperationContext context{Self, txc, ctx, MergeOpSideEffects, memChanges, dbChanges};

        auto response = Self->IgniteOperation(*request, context);

        dbChanges.Apply(Self, txc, ctx);
        MergeOpSideEffects.ApplyOnExecute(Self, txc, ctx);

        return false;
    }

    if (rec.GetShardState() != NKikimrTxDataShard::Ready) {
        return true;
    }

    ui64 dataSizeResolution = 0; // Datashard will use default resolution
    ui64 rowCountResolution = 0; // Datashard will use default resolution
    bool collectKeySample = false;
    if (table->ShouldSplitBySize(dataSize, forceShardSplitSettings)) {
        // We would like to split by size and do this no matter how many partitions there are
    } else if (table->GetPartitions().size() >= table->GetMaxPartitionsCount()) {
        // We cannot split as there are max partitions already
        return true;
    } else if (table->CheckFastSplitForPartition(Self->SplitSettings, shardIdx, dataSize, rowCount)) {
        dataSizeResolution = Max<ui64>(dataSize / 100, 100*1024);
        rowCountResolution = Max<ui64>(rowCount / 100, 1000);
        collectKeySample = true;
    } else if (table->CheckSplitByLoad(Self->SplitSettings, shardIdx, dataSize, rowCount)) {
        collectKeySample = true;
    } else {
        return true;
    }

    {
        constexpr ui64 deltaShards = 2;
        TPathElement::TPtr path = Self->PathsById.at(pathId);
        TSubDomainInfo::TPtr domainInfo = Self->ResolveDomainInfo(pathId);

        if (domainInfo->GetShardsInside() + deltaShards > domainInfo->GetSchemeLimits().MaxShards) {
            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                         "Do not request full stats from datashard"
                             << ", datashard: " << datashardId
                             << ", reason: shards count has reached maximum value in the domain"
                             << ", shards limit for domain: " << domainInfo->GetSchemeLimits().MaxShards
                             << ", shards count inside domain: " << domainInfo->GetShardsInside()
                             << ", intention to create new shards: " << deltaShards);
            return true;
        }

        if (path->GetShardsInside() + deltaShards > domainInfo->GetSchemeLimits().MaxShardsInPath) {
            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                         "Do not request full stats from datashard"
                             << ", datashard: " << datashardId
                             << ", reason: shards count has reached maximum value in the path"
                             << ", shards limit for path: " << domainInfo->GetSchemeLimits().MaxShardsInPath
                             << ", shards count inside path: " << path->GetShardsInside()
                             << ", intention to create new shards: " << deltaShards);
            return true;
        }
    }

    if (newStats.HasBorrowedData) {
        // We don't want to split shards that have borrow parts
        // We must ask them to compact first
        Self->EnqueueBorrowedCompaction(shardIdx);
        return true;
    }

    // Request histograms from the datashard
    LOG_DEBUG(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
             "Requesting full stats from datashard %" PRIu64, rec.GetDatashardId());
    PendingMessages.emplace_back(
        item.Ev->Sender,
        new TEvDataShard::TEvGetTableStats(
            pathId.LocalPathId,
            dataSizeResolution,
            rowCountResolution,
            collectKeySample));

    return true;
}

void TTxStorePartitionStats::Complete(const TActorContext& ctx) {
    MergeOpSideEffects.ApplyOnComplete(Self, ctx);

    for (auto& m: PendingMessages) {
        Y_VERIFY(m.Event);
        ctx.Send(m.Actor, m.Event.Release());
    }

    Self->TabletCounters->Simple()[COUNTER_STATS_QUEUE_SIZE].Set(Self->StatsQueue.size());
}

void TSchemeShard::Handle(TEvDataShard::TEvPeriodicTableStats::TPtr& ev, const TActorContext& ctx) {
    const auto& rec = ev->Get()->Record;

    auto datashardId = TTabletId(rec.GetDatashardId());
    const auto& tableStats = rec.GetTableStats();
    const auto& tabletMetrics = rec.GetTabletMetrics();
    ui64 dataSize = tableStats.GetDataSize();
    ui64 rowCount = tableStats.GetRowCount();

    TPathId pathId = rec.HasTableOwnerId()
            ? TPathId(TOwnerId(rec.GetTableOwnerId()), TLocalPathId(rec.GetTableLocalId()))
            : MakeLocalId(TLocalPathId(rec.GetTableLocalId()));

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
               "Got periodic table stats at tablet " << TabletID()
                                                     << " from shard " << datashardId
                                                     << " pathId " << pathId
                                                     << " state '" << DatashardStateName(rec.GetShardState()) << "'"
                                                     << " dataSize " << dataSize
                                                     << " rowCount " << rowCount
                                                     << " cpuUsage " << tabletMetrics.GetCPU()/10000.0);

    TStatsId statsId(pathId, datashardId);
    TStatsMap::insert_ctx insertCtx;
    auto it = StatsMap.find(statsId, insertCtx);
    if (it == StatsMap.end()) {
        StatsQueue.emplace_back(ev.Release(), pathId);
        StatsMap.emplace_direct(insertCtx, statsId, &StatsQueue.back());
    } else {
        // already in queue, just update
        it->second->Ev = ev.Release();
    }

    TabletCounters->Simple()[COUNTER_STATS_QUEUE_SIZE].Set(StatsQueue.size());
    ScheduleStatsBatch(ctx);
}

void TSchemeShard::ScheduleStatsBatch(const TActorContext& ctx) {
    if (StatsQueue.empty())
        return;

    bool isBatchingDisabled = StatsMaxBatchSize == 0;
    if (isBatchingDisabled) {
        PersistStatsPending = true;
        Execute(new TTxStorePartitionStats(this), ctx);
        LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Will execute TTxStorePartitionStats without batch");
        return;
    }

    if (PersistStatsPending)
        return;

    if (StatsQueue.size() >= StatsMaxBatchSize || !StatsBatchTimeout) {
        // note that we don't care if already scheduled
        PersistStatsPending = true;
        Execute(new TTxStorePartitionStats(this), ctx);
        LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "Will execute TTxStorePartitionStats, queue# " << StatsQueue.size());
        return;
    }

    const auto& oldestItem = StatsQueue.front();
    auto age = AppData()->MonotonicTimeProvider->Now() - oldestItem.Ts;
    if (age >= StatsBatchTimeout) {
        PersistStatsPending = true;
        Execute(new TTxStorePartitionStats(this), ctx);
        LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "Will execute TTxStorePartitionStats because of age, queue# " << StatsQueue.size());
        return;
    }

    if (StatsBatchScheduled)
        return;

    auto delay = StatsBatchTimeout - age;
    LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Will delay TTxStorePartitionStats on# " << delay << ", queue# " << StatsQueue.size());

    ctx.Schedule(delay, new TEvPrivate::TEvPersistStats());
    StatsBatchScheduled = true;
}

void TSchemeShard::Handle(TEvPrivate::TEvPersistStats::TPtr&, const TActorContext& ctx) {
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
               "Started TEvPersistStats at tablet " << TabletID() << ", queue size# " << StatsQueue.size());

    StatsBatchScheduled = false;

    if (PersistStatsPending) {
        return;
    }

    if (StatsQueue.empty()) {
        return;
    }

    PersistStatsPending = true;
    Execute(new TTxStorePartitionStats(this), ctx);
}

}}
