#include "schemeshard_impl.h"
#include "schemeshard__stats_impl.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/cputime.h>
#include <ydb/core/protos/sys_view.pb.h>

namespace {

THashMap<ui32, TString> MapChannelsToStoragePoolKinds(const NActors::TActorContext& ctx,
                                                      const NKikimr::TStoragePools& pools,
                                                      const NKikimr::TChannelsBindings& bindings
) {
    THashMap<TString, TString> nameToKindMap(pools.size());
    for (const auto& pool : pools) {
        nameToKindMap.emplace(pool.GetName(), pool.GetKind());
    }
    THashMap<ui32, TString> channelsMapping(bindings.size());
    for (ui32 channel = 0u; channel < bindings.size(); ++channel) {
        if (const auto* poolKind = nameToKindMap.FindPtr(bindings[channel].GetStoragePoolName())) {
            channelsMapping.emplace(channel, *poolKind);
        } else {
            LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "MapChannelsToStoragePoolKinds: the subdomain has no info about the storage pool named "
                            << bindings[channel].GetStoragePoolName()
            );
        }
    }
    return channelsMapping;
}

}

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
    ctx.Send(ev->Forward(SysPartitionStatsCollector));
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
    sysStats.SetByKeyFilterSize(stats.ByKeyFilterSize);
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

class TTxStoreTableStats: public TTxStoreStats<TEvDataShard::TEvPeriodicTableStats> {
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
    TTxStoreTableStats(TSchemeShard* ss, TStatsQueue<TEvDataShard::TEvPeriodicTableStats>& queue, bool& persistStatsPending)
        : TTxStoreStats(ss, queue, persistStatsPending)
    {
    }

    virtual ~TTxStoreTableStats() = default;

    void Complete(const TActorContext& ctx) override;

    // returns true to continue batching
    bool PersistSingleStats(const TPathId& pathId, const TStatsQueue<TEvDataShard::TEvPeriodicTableStats>::TItem& item, TTransactionContext& txc, const TActorContext& ctx) override;
    void ScheduleNextBatch(const TActorContext& ctx) override;

    template <typename T>
    TPartitionStats PrepareStats(const TActorContext& ctx, const T& rec, const THashMap<ui32, TString>& channelsMapping = {}) const;
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

template <typename T>
TPartitionStats TTxStoreTableStats::PrepareStats(const TActorContext& ctx,
                                                 const T& rec,
                                                 const THashMap<ui32, TString>& channelsMapping
) const {
    const auto& tableStats = rec.GetTableStats();
    const auto& tabletMetrics = rec.GetTabletMetrics();

    TPartitionStats newStats;
    newStats.SeqNo = TMessageSeqNo(rec.GetGeneration(), rec.GetRound());

    newStats.RowCount = tableStats.GetRowCount();
    newStats.DataSize = tableStats.GetDataSize();
    newStats.IndexSize = tableStats.GetIndexSize();
    newStats.ByKeyFilterSize = tableStats.GetByKeyFilterSize();
    newStats.LastAccessTime = TInstant::MilliSeconds(tableStats.GetLastAccessTime());
    newStats.LastUpdateTime = TInstant::MilliSeconds(tableStats.GetLastUpdateTime());
    for (const auto& channelStats : tableStats.GetChannels()) {
        if (const auto* poolKind = channelsMapping.FindPtr(channelStats.GetChannel())) {
            auto& [dataSize, indexSize] = newStats.StoragePoolsStats[*poolKind];
            dataSize += channelStats.GetDataSize();
            indexSize += channelStats.GetIndexSize();
        } else {
            LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "PrepareStats: SchemeShard has no info on DataShard "
                            << rec.GetDatashardId() << " channel " << channelStats.GetChannel() << " binding"
            );
        }
    }

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

    return newStats;
}

bool TTxStoreTableStats::PersistSingleStats(const TPathId& pathId,
                                            const TStatsQueueItem<TEvDataShard::TEvPeriodicTableStats>& item,
                                            NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) {
    const auto& rec = item.Ev->Get()->Record;
    const auto datashardId = TTabletId(rec.GetDatashardId());

    const auto& tableStats = rec.GetTableStats();
    ui64 dataSize = tableStats.GetDataSize();
    ui64 rowCount = tableStats.GetRowCount();

    const bool isDataShard = Self->Tables.contains(pathId);
    const bool isOlapStore = Self->OlapStores.contains(pathId);
    const bool isColumnTable = Self->ColumnTables.contains(pathId);

    if (!isDataShard && !isOlapStore && !isColumnTable) {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Unexpected stats from shard " << datashardId);
        return true;
    }

    if (!Self->TabletIdToShardIdx.contains(datashardId)) {
        LOG_ERROR_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "No shardIdx for shard " << datashardId);
        return true;
    }

    TShardIdx shardIdx = Self->TabletIdToShardIdx[datashardId];
    const auto* shardInfo = Self->ShardInfos.FindPtr(shardIdx);
    if (!shardInfo) {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "No ShardInfo by shardIdx " << shardIdx << " of shard " << datashardId;
        );
        return true;
    }

    auto subDomainInfo = Self->ResolveDomainInfo(pathId);
    const auto channelsMapping = MapChannelsToStoragePoolKinds(ctx,
                                                               subDomainInfo->EffectiveStoragePools(),
                                                               shardInfo->BindedChannels);

    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TTxStoreTableStats.PersistSingleStats: main stats from"
                    << " datashardId(TabletID)=" << datashardId << " maps to shardIdx: " << shardIdx
                    << ", pathId: " << pathId << ", pathId map=" << Self->PathsById[pathId]->Name
                    << ", is column=" << isColumnTable << ", is olap=" << isOlapStore);

    const TPartitionStats newStats = PrepareStats(ctx, rec, channelsMapping);

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
               "Add stats from shard with datashardId(TabletID)=" << datashardId 
                    << ", pathId " << pathId.LocalPathId
                    << ": RowCount " << newStats.RowCount 
                    << ", DataSize " << newStats.DataSize
                    << (newStats.HasBorrowedData ? ", with borrowed parts" : ""));

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
        } else if (Self->EnableBorrowedSplitCompaction && rec.GetIsDstSplit()) {
            // note that we want to compact only shards originating
            // from split/merge and not shards created via copytable
            Self->EnqueueBorrowedCompaction(shardIdx);
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

        const auto tables = rec.GetTables();
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "OLAP store contains " << tables.size() << " tables.");

        for (const auto& table : tables) {
            const TPartitionStats newTableStats = PrepareStats(ctx, table);

            const TPathId tablePathId = TPathId(TOwnerId(pathId.OwnerId), TLocalPathId(table.GetTableLocalId()));

            if (Self->ColumnTables.contains(tablePathId)) {
                LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                            "add stats for exists table with pathId=" << tablePathId);

                auto columnTable = Self->ColumnTables.TakeVerified(tablePathId);
                columnTable->UpdateTableStats(shardIdx, tablePathId, newTableStats);
            } else {
                LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                           "failed add stats for table with pathId=" << tablePathId);
            }
        }

    } else if (isColumnTable) {
        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "PersistSingleStats: ColumnTable rec.GetColumnTables() size=" << rec.GetTables().size());

        auto columnTable = Self->ColumnTables.TakeVerified(pathId);
        oldAggrStats = columnTable->GetStats().Aggregated;
        columnTable->UpdateShardStats(shardIdx, newStats);
        newAggrStats = columnTable->GetStats().Aggregated;
        updateSubdomainInfo = true;
    }

    if (updateSubdomainInfo) {
        auto subDomainId = Self->ResolvePathIdForDomain(pathId);
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

        PendingMessages.emplace_back(
            Self->SysPartitionStatsCollector,
            Self->BuildStatsForCollector(pathId, shardIdx, datashardId, nodeId, startTime, newStats).Release());
    }

    if (isOlapStore || isColumnTable) {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "Aggregated stats for pathId " << pathId.LocalPathId
                    << ": RowCount " << newAggrStats.RowCount << ", DataSize " << newAggrStats.DataSize);
        return true;
    }

    const auto& shardToPartition = table->GetShard2PartitionIdx();
    if (table->IsTTLEnabled() && shardToPartition.contains(shardIdx)) {
        const ui64 partitionIdx = shardToPartition.at(shardIdx);
        const auto& partitions = table->GetPartitions();

        Y_ABORT_UNLESS(partitionIdx < partitions.size());
        auto& shardInfo = partitions.at(partitionIdx);
        auto& lag = shardInfo.LastCondEraseLag;

        if (lag) {
            Self->TabletCounters->Percentile()[COUNTER_NUM_SHARDS_BY_TTL_LAG].DecrementFor(lag->Seconds());
        }

        const auto now = ctx.Now();
        if (now >= shardInfo.LastCondErase) {
            lag = now - shardInfo.LastCondErase;
        } else {
            lag = TDuration::Zero();
        }

        Self->TabletCounters->Percentile()[COUNTER_NUM_SHARDS_BY_TTL_LAG].IncrementFor(lag->Seconds());
    }

    const TTableInfo* mainTableForIndex = Self->GetMainTableForIndex(pathId);

    const auto forceShardSplitSettings = Self->SplitSettings.GetForceShardSplitSettings();
    TVector<TShardIdx> shardsToMerge;
    if (table->CheckCanMergePartitions(Self->SplitSettings, forceShardSplitSettings, shardIdx, shardsToMerge, mainTableForIndex)) {
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

    bool collectKeySample = false;
    if (table->ShouldSplitBySize(dataSize, forceShardSplitSettings)) {
        // We would like to split by size and do this no matter how many partitions there are
    } else if (table->GetPartitions().size() >= table->GetMaxPartitionsCount()) {
        // We cannot split as there are max partitions already
        return true;
    } else if (table->CheckSplitByLoad(Self->SplitSettings, shardIdx, dataSize, rowCount, mainTableForIndex)) {
        collectKeySample = true;
    } else {
        return true;
    }

    {
        auto path = TPath::Init(pathId, Self);
        auto checks = path.Check();

        constexpr ui64 deltaShards = 2;
        checks
            .PathShardsLimit(deltaShards)
            .ShardsLimit(deltaShards);

        if (!checks) {
            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                         "Do not request full stats from datashard"
                             << ", datashard: " << datashardId
                             << ", reason: " << checks.GetError());
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
    auto request = new TEvDataShard::TEvGetTableStats(pathId.LocalPathId);
    request->Record.SetCollectKeySample(collectKeySample);
    PendingMessages.emplace_back(item.Ev->Sender, request);

    return true;
}

void TTxStoreTableStats::Complete(const TActorContext& ctx) {
    MergeOpSideEffects.ApplyOnComplete(Self, ctx);

    for (auto& m: PendingMessages) {
        Y_ABORT_UNLESS(m.Event);
        ctx.Send(m.Actor, m.Event.Release());
    }

    Queue.WriteQueueSizeMetric();
}

void TTxStoreTableStats::ScheduleNextBatch(const TActorContext& ctx) {
    Self->ExecuteTableStatsBatch(ctx);
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

    LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Got periodic table stats at tablet " << TabletID()
                                                     << " from shard " << datashardId
                                                     << " pathId " << pathId
                                                     << " raw table stats:\n" << tableStats.ShortDebugString());

    TStatsId statsId(pathId, datashardId);

    switch(TableStatsQueue.Add(statsId, ev.Release())) {
        case READY:
            ExecuteTableStatsBatch(ctx);
            break;

        case NOT_READY:
            ScheduleTableStatsBatch(ctx);
            break;

        default:
          Y_ABORT("Unknown batch status");
    }
}

void TSchemeShard::Handle(TEvPrivate::TEvPersistTableStats::TPtr&, const TActorContext& ctx) {
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
           "Started TEvPersistStats at tablet " << TabletID() << ", queue size# " << TableStatsQueue.Size());

    TableStatsBatchScheduled = false;
    ExecuteTableStatsBatch(ctx);
}

void TSchemeShard::ExecuteTableStatsBatch(const TActorContext& ctx) {
    if (!TablePersistStatsPending && !TableStatsQueue.Empty()) {
        TablePersistStatsPending = true;
        EnqueueExecute(new TTxStoreTableStats(this, TableStatsQueue, TablePersistStatsPending));
        LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "Will execute TTxStoreStats, queue# " << TableStatsQueue.Size());
        ScheduleTableStatsBatch(ctx);
    }
}

void TSchemeShard::ScheduleTableStatsBatch(const TActorContext& ctx) {
    if (!TableStatsBatchScheduled && !TableStatsQueue.Empty()) {
        TDuration delay = TableStatsQueue.Delay();
        LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Will delay TTxStoreTableStats on# " << delay << ", queue# " << TableStatsQueue.Size());

        ctx.Schedule(delay, new TEvPrivate::TEvPersistTableStats());
        TableStatsBatchScheduled = true;
    }
}

}}
