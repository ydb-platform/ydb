#include "schemeshard__stats_impl.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/cputime.h>
#include <ydb/core/protos/sys_view.pb.h>
#include <ydb/core/protos/table_stats.pb.h>


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

auto TSchemeShard::BuildStatsForCollector(TPathId pathId, TShardIdx shardIdx, TTabletId datashardId, ui32 followerId,
    TMaybe<ui32> nodeId, TMaybe<ui64> startTime, const TPartitionStats& stats, const TActorContext& ctx)
{
    LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "BuildStatsForCollector: datashardId " <<  datashardId << ", followerId " << followerId);

    auto ev = MakeHolder<NSysView::TEvSysView::TEvSendPartitionStats>(
        GetDomainKey(pathId), pathId, std::make_pair(ui64(shardIdx.GetOwnerId()), ui64(shardIdx.GetLocalId())));

    auto& sysStats = ev->Stats;
    sysStats.SetDataSize(stats.DataSize);
    sysStats.SetRowCount(stats.RowCount);
    sysStats.SetIndexSize(stats.IndexSize);
    sysStats.SetByKeyFilterSize(stats.ByKeyFilterSize);
    sysStats.SetCPUCores(std::min(stats.GetCurrentRawCpuUsage() / 1000000., 1.0));
    sysStats.SetTabletId(ui64(datashardId));
    sysStats.SetFollowerId(followerId);
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
    sysStats.SetLocksAcquired(stats.LocksAcquired);
    sysStats.SetLocksWholeShard(stats.LocksWholeShard);
    sysStats.SetLocksBroken(stats.LocksBroken);

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
    bool PersistSingleStats(const TPathId& pathId, const TStatsQueue<TEvDataShard::TEvPeriodicTableStats>::TItem& item, TInstant now, TTransactionContext& txc, const TActorContext& ctx) override;
    void ScheduleNextBatch(const TActorContext& ctx) override;

    template <typename T>
    TPartitionStats PrepareStats(const T& rec, TInstant now, const NKikimr::TStoragePools& pools, const NKikimr::TChannelsBindings& bindings) const;
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
TPartitionStats TTxStoreTableStats::PrepareStats(const T& rec,
                                                 TInstant now,
                                                 const NKikimr::TStoragePools& pools,
                                                 const NKikimr::TChannelsBindings& bindings
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

    Y_UNUSED(pools);

    for (const auto& channelStats : tableStats.GetChannels()) {
        const auto& channelBind = bindings[channelStats.GetChannel()];
        const auto& poolKind = channelBind.GetStoragePoolKind();
        auto& [dataSize, indexSize] = newStats.StoragePoolsStats[poolKind];
        dataSize += channelStats.GetDataSize();
        indexSize += channelStats.GetIndexSize();
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

    newStats.LocksAcquired = tableStats.GetLocksAcquired();
    newStats.LocksWholeShard = tableStats.GetLocksWholeShard();
    newStats.LocksBroken = tableStats.GetLocksBroken();

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
    newStats.HasSchemaChanges = tableStats.GetHasSchemaChanges();
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
                                            TInstant now,
                                            NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) {
    const auto& rec = item.Ev->Get()->Record;
    const auto datashardId = TTabletId(rec.GetDatashardId());
    const ui32 followerId = rec.GetFollowerId();

    const auto& tableStats = rec.GetTableStats();
    ui64 dataSize = tableStats.GetDataSize();
    ui64 rowCount = tableStats.GetRowCount();

    const auto pathElementIt = Self->PathsById.find(pathId);
    if (pathElementIt == Self->PathsById.end()) {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "PersistSingleStats for pathId " << pathId
            << ", tabletId " << datashardId
            << ", followerId " << followerId
            << ": unknown pathId"
        );
        return true;
    }
    const auto& pathElement = pathElementIt->second;
    if (pathElement->Dropped()) {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "PersistSingleStats for pathId " << pathId
            << ", tabletId " << datashardId
            << ", followerId " << followerId
            << ": pathId is dropped"
        );
        return true;
    }

    const bool isDataShard = pathElement->IsTable();
    const bool isOlapStore = pathElement->IsOlapStore();
    const bool isColumnTable = pathElement->IsColumnTable();

    if (!isDataShard && !isOlapStore && !isColumnTable) {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Unexpected stats from shard " << datashardId);
        return true;
    }

    TShardIdx shardIdx = [this, &datashardId]() {
        auto found = Self->TabletIdToShardIdx.find(datashardId);
        return (found != Self->TabletIdToShardIdx.end()) ? found->second : InvalidShardIdx;
    }();
    if (!shardIdx) {
        LOG_ERROR_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "No shardIdx for shard " << datashardId);
        return true;
    }

    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "PersistSingleStats for pathId " << pathId.LocalPathId << " shard idx " << shardIdx << " data size " << dataSize << " row count " << rowCount
    );
    const auto* shardInfo = Self->ShardInfos.FindPtr(shardIdx);
    if (!shardInfo) {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "No ShardInfo by shardIdx " << shardIdx << " of shard " << datashardId;
        );
        return true;
    }

    auto subDomainInfo = Self->ResolveDomainInfo(pathElement);

    const TPartitionStats newStats = PrepareStats(rec, now, subDomainInfo->EffectiveStoragePools(), shardInfo->BindedChannels);

    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TTxStoreTableStats.PersistSingleStats: main stats from"
                    << " datashardId(TabletID)=" << datashardId << " maps to shardIdx: " << shardIdx
                    << " followerId=" << followerId
                    << ", pathId: " << pathId << ", pathId map=" << pathElement->Name
                    << ", is column=" << isColumnTable << ", is olap=" << isOlapStore
                    << ", RowCount " << newStats.RowCount
                    << ", DataSize " << newStats.DataSize
                    << (newStats.HasBorrowedData ? ", with borrowed parts" : ""));

    NIceDb::TNiceDb db(txc.DB);

    TTableInfo::TPtr table;
    bool updateSubdomainInfo = false;

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
        Self->BuildStatsForCollector(pathId, shardIdx, datashardId, followerId, nodeId, startTime, newStats, ctx).Release());

    // Skip statistics from follower
    if (followerId) {
        return true;
    }

    TDiskSpaceUsageDelta diskSpaceUsageDelta;

    if (isDataShard) {
        table = Self->Tables[pathId];
        table->UpdateShardStats(&diskSpaceUsageDelta, shardIdx, newStats, now);

        if (!table->IsBackup) {
            Self->UpdateBackgroundCompaction(shardIdx, newStats);
            Self->UpdateShardMetrics(shardIdx, newStats, now);
        }

        if (!newStats.HasBorrowedData) {
            Self->RemoveBorrowedCompaction(shardIdx);
        } else if (Self->EnableBorrowedSplitCompaction && rec.GetIsDstSplit()) {
            // note that we want to compact only shards originating
            // from split/merge and not shards created via copytable
            Self->EnqueueBorrowedCompaction(shardIdx);
        }

        if (!table->IsBackup && !table->IsShardsStatsDetached()) {
            updateSubdomainInfo = true;
        }

        Self->PersistTablePartitionStats(db, pathId, shardIdx, table);
    } else if (isOlapStore) {
        TOlapStoreInfo::TPtr olapStore = Self->OlapStores[pathId];
        olapStore->UpdateShardStats(&diskSpaceUsageDelta, shardIdx, newStats, now);
        updateSubdomainInfo = true;

        const auto tables = rec.GetTables();
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "OLAP store contains " << tables.size() << " tables.");

        for (const auto& table : tables) {
            const TPartitionStats newTableStats = PrepareStats(table, now, {}, {});

            const TPathId tablePathId = TPathId(TOwnerId(pathId.OwnerId), TLocalPathId(table.GetTableLocalId()));

            if (Self->ColumnTables.contains(tablePathId)) {
                LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                            "add stats for exists table with pathId=" << tablePathId);

                Self->ColumnTables.GetVerifiedPtr(tablePathId)->UpdateTableStats(&diskSpaceUsageDelta, shardIdx, tablePathId, newTableStats, now);
            } else {
                LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                           "failed add stats for table with pathId=" << tablePathId);
            }
        }

        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "Aggregated stats for pathId " << pathId.LocalPathId
            << ": RowCount " << olapStore->Stats.Aggregated.RowCount
            << ", DataSize " << olapStore->Stats.Aggregated.DataSize
        );

    } else if (isColumnTable) {
        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "PersistSingleStats: ColumnTable rec.GetColumnTables() size=" << rec.GetTables().size());

        auto columnTable = Self->ColumnTables.GetVerifiedPtr(pathId);
        columnTable->UpdateShardStats(&diskSpaceUsageDelta, shardIdx, newStats, now);
        updateSubdomainInfo = true;

        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "Aggregated stats for pathId " << pathId.LocalPathId
            << ": RowCount " << columnTable->Stats.Aggregated.RowCount
            << ", DataSize " << columnTable->Stats.Aggregated.DataSize
        );
    }

    if (updateSubdomainInfo) {
        subDomainInfo->AggrDiskSpaceUsage(Self, diskSpaceUsageDelta);
        if (subDomainInfo->CheckDiskSpaceQuotas(Self)) {
            auto subDomainId = Self->ResolvePathIdForDomain(pathElement);
            Self->PersistSubDomainState(db, subDomainId, *subDomainInfo);
            // Publish is done in a separate transaction, so we may call this directly
            TDeque<TPathId> toPublish;
            toPublish.push_back(subDomainId);
            Self->PublishToSchemeBoard(TTxId(), std::move(toPublish), ctx);
        }
    }

    if (isOlapStore || isColumnTable) {
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

        if (now >= shardInfo.LastCondErase) {
            lag = now - shardInfo.LastCondErase;
        } else {
            lag = TDuration::Zero();
        }

        Self->TabletCounters->Percentile()[COUNTER_NUM_SHARDS_BY_TTL_LAG].IncrementFor(lag->Seconds());
    }

    const TTableIndexInfo* index = Self->Indexes.Value(pathElement->ParentPathId, nullptr).Get();
    const TTableInfo* mainTableForIndex = (index ? Self->GetMainTableForIndex(pathId) : nullptr);

    TString errStr;
    const auto forceShardSplitSettings = Self->SplitSettings.GetForceShardSplitSettings();
    TVector<TShardIdx> shardsToMerge;
    TString mergeReason;
    if ((!index || index->State == NKikimrSchemeOp::EIndexStateReady)
        && Self->CheckInFlightLimit(TTxState::ETxType::TxSplitTablePartition, errStr)
        && table->CheckCanMergePartitions(Self->SplitSettings, forceShardSplitSettings, shardIdx, Self->ShardInfos[shardIdx].TabletID, shardsToMerge, mainTableForIndex, now, mergeReason)
    ) {
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

        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "Propose merge request : " << request->Record.ShortDebugString()
            << ", reason: " << mergeReason);

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
    TString reason;
    if (table->ShouldSplitBySize(dataSize, forceShardSplitSettings, reason)) {
        // We would like to split by size and do this no matter how many partitions there are
        LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "Want to split tablet " << datashardId << " by size: " << reason);
    } else if (table->GetPartitions().size() >= table->GetMaxPartitionsCount()) {
        // We cannot split as there are max partitions already
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "Do not want to split tablet " << datashardId << " by size,"
            << " its table already has "<< table->GetPartitions().size() << " out of " << table->GetMaxPartitionsCount() << " partitions");
        return true;
    } else if (table->CheckSplitByLoad(Self->SplitSettings, shardIdx, dataSize, rowCount, mainTableForIndex, reason)) {
        LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "Want to split tablet " << datashardId << " by load: " << reason);
        collectKeySample = true;
    } else {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "Do not want to split tablet " << datashardId << ": " << reason);
        return true;
    }

    //NOTE: intentionally avoid using TPath.Check().{PathShardsLimit,ShardsLimit}() here.
    // PathShardsLimit() performs pedantic validation by recalculating shard count through
    // iteration over entire ShardInfos, which is too slow for this hot spot. It also performs
    // additional lookups we want to avoid.
    {
        constexpr ui64 deltaShards = 2;
        if ((pathElement->GetShardsInside() + deltaShards) > subDomainInfo->GetSchemeLimits().MaxShardsInPath) {
            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Do not request full stats from datashard " << datashardId
                << ", reason: shards count limit exceeded (in path)"
                << ", limit: " << subDomainInfo->GetSchemeLimits().MaxShardsInPath
                << ", current: " << pathElement->GetShardsInside()
                << ", delta: " << deltaShards
            );
            return true;
        }
        const auto currentShards = (subDomainInfo->GetShardsInside() - subDomainInfo->GetBackupShards());
        if ((currentShards + deltaShards) > subDomainInfo->GetSchemeLimits().MaxShards) {
            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Do not request full stats from datashard " << datashardId
                << ", datashard: " << datashardId
                << ", reason: shards count limit exceeded (in subdomain)"
                << ", limit: " << subDomainInfo->GetSchemeLimits().MaxShards
                << ", current: " << currentShards
                << ", delta: " << deltaShards
            );
            return true;
        }
    }

    if (newStats.HasBorrowedData) {
        LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "Postpone split tablet " << datashardId << " because it has borrow parts, enqueue compact them first");
        Self->EnqueueBorrowedCompaction(shardIdx);
        return true;
    }

    // path.IsLocked() and path.LockedBy() equivalent
    if (const auto& found = Self->LockedPaths.find(pathId); found != Self->LockedPaths.end()) {
        const auto txId = found->second;
        LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "Postpone split tablet " << datashardId << " because it is locked by " << txId);
        return true;
    }

    // Request histograms from the datashard
    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "Requesting full tablet stats " << datashardId << " to split it");
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
    const ui32 followerId = rec.GetFollowerId();
    const auto& tableStats = rec.GetTableStats();
    const auto& tabletMetrics = rec.GetTabletMetrics();
    ui64 dataSize = tableStats.GetDataSize();
    ui64 rowCount = tableStats.GetRowCount();

    TPathId pathId = rec.HasTableOwnerId()
            ? TPathId(TOwnerId(rec.GetTableOwnerId()), TLocalPathId(rec.GetTableLocalId()))
            : MakeLocalId(TLocalPathId(rec.GetTableLocalId()));

    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
               "Got periodic table stats at tablet " << TabletID()
                                                     << " from shard " << datashardId
                                                     << " followerId " << followerId
                                                     << " pathId " << pathId
                                                     << " state '" << DatashardStateName(rec.GetShardState()) << "'"
                                                     << " dataSize " << dataSize
                                                     << " rowCount " << rowCount
                                                     << " cpuUsage " << tabletMetrics.GetCPU()/10000.0);

    LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Got periodic table stats at tablet " << TabletID()
                                                     << " from shard " << datashardId
                                                     << " followerId " << followerId
                                                     << " pathId " << pathId
                                                     << " raw table stats:\n" << tableStats.ShortDebugString());

    TStatsId statsId(pathId, datashardId, followerId);

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
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
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

void TSchemeShard::UpdateShardMetrics(
    const TShardIdx& shardIdx,
    const TPartitionStats& newStats,
    TInstant now
) {
    if (newStats.HasBorrowedData)
        ShardsWithBorrowed.insert(shardIdx);
    else
        ShardsWithBorrowed.erase(shardIdx);
    TabletCounters->Simple()[COUNTER_SHARDS_WITH_BORROWED_DATA].Set(ShardsWithBorrowed.size());

    if (newStats.HasLoanedData)
        ShardsWithLoaned.insert(shardIdx);
    else
        ShardsWithLoaned.erase(shardIdx);
    TabletCounters->Simple()[COUNTER_SHARDS_WITH_LOANED_DATA].Set(ShardsWithLoaned.size());

    THashMap<TShardIdx, TPartitionMetrics>::insert_ctx insertCtx;
    auto it = PartitionMetricsMap.find(shardIdx, insertCtx);
    if (it != PartitionMetricsMap.end()) {
        const auto& metrics = it->second;
        TabletCounters->Percentile()[COUNTER_SHARDS_WITH_SEARCH_HEIGHT].DecrementFor(metrics.SearchHeight);
        TabletCounters->Percentile()[COUNTER_SHARDS_WITH_FULL_COMPACTION].DecrementFor(metrics.HoursSinceFullCompaction);
        TabletCounters->Percentile()[COUNTER_SHARDS_WITH_ROW_DELETES].DecrementFor(metrics.RowDeletes);
    } else {
        it = PartitionMetricsMap.insert_direct(std::make_pair(shardIdx, TPartitionMetrics()), insertCtx);
    }

    auto& metrics = it->second;

    metrics.SearchHeight = newStats.SearchHeight;
    TabletCounters->Percentile()[COUNTER_SHARDS_WITH_SEARCH_HEIGHT].IncrementFor(metrics.SearchHeight);

    metrics.RowDeletes = newStats.RowDeletes;
    TabletCounters->Percentile()[COUNTER_SHARDS_WITH_ROW_DELETES].IncrementFor(metrics.RowDeletes);

    auto compactionTime = TInstant::Seconds(newStats.FullCompactionTs);
    if (now >= compactionTime)
        metrics.HoursSinceFullCompaction = (now - compactionTime).Hours();
    else
        metrics.HoursSinceFullCompaction = 0;

    TabletCounters->Percentile()[COUNTER_SHARDS_WITH_FULL_COMPACTION].IncrementFor(metrics.HoursSinceFullCompaction);
}

void TSchemeShard::RemoveShardMetrics(const TShardIdx& shardIdx) {
    ShardsWithBorrowed.erase(shardIdx);
    TabletCounters->Simple()[COUNTER_SHARDS_WITH_BORROWED_DATA].Set(ShardsWithBorrowed.size());

    ShardsWithLoaned.erase(shardIdx);
    TabletCounters->Simple()[COUNTER_SHARDS_WITH_LOANED_DATA].Set(ShardsWithLoaned.size());

    auto it = PartitionMetricsMap.find(shardIdx);
    if (it == PartitionMetricsMap.end())
        return;

    const auto& metrics = it->second;
    TabletCounters->Percentile()[COUNTER_SHARDS_WITH_SEARCH_HEIGHT].DecrementFor(metrics.SearchHeight);
    TabletCounters->Percentile()[COUNTER_SHARDS_WITH_FULL_COMPACTION].DecrementFor(metrics.HoursSinceFullCompaction);
    TabletCounters->Percentile()[COUNTER_SHARDS_WITH_ROW_DELETES].DecrementFor(metrics.RowDeletes);

    PartitionMetricsMap.erase(it);
}

}}
