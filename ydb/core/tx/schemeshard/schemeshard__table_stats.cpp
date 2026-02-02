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

private:
    template <typename T>
    TPartitionStats PrepareStats(const T& rec, TInstant now, const NKikimr::TStoragePools& pools, const NKikimr::TChannelsBindings& bindings) const;

    /**
     * Verify that splitting the given partition is allowed (either by size or by load)
     * and send the EvGetTableStats message to the given tablet to prepare
     * the split operation.
     *
     * @param[in] ctx The actor execution context
     * @param[in] statsEventSender The ID of the actor which sent EvPeriodicTableStats
     * @param[in] datashardId The corresponding datashard ID
     * @param[in] shardIdx The corresponding shard index
     * @param[in] pathId The corresponding path ID
     * @param[in] pathElement The corresponding path element
     * @param[in] subDomainInfo The information about the corresponding subdomain
     * @param[in] newPartitionStats The new partition statistics (from the event)
     * @param[in] collectKeySample If true, request the key access sample to be collected
     */
    bool VerifySplitAndRequestStats(
        const TActorContext& ctx,
        const TActorId& statsEventSender,
        TTabletId datashardId,
        const TShardIdx& shardIdx,
        const TPathId& pathId,
        TPathElement::TPtr pathElement,
        TSubDomainInfo::TPtr subDomainInfo,
        const TPartitionStats& newPartitionStats,
        bool collectKeySample
    );
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

const TString* GetPoolKind(const NKikimr::TChannelBind& channelBind, const TStoragePools& pools) {
    auto findPoolByName = [](const auto& pools, const auto& name) {
        return std::find_if(pools.begin(), pools.end(), [&name](const auto& pool) {
            return pool.GetName() == name;
        });
    };
    // fast: use pool kind specified by the channel bind
    // slower: find pool kind by name
    if (const auto& poolKind = channelBind.GetStoragePoolKind(); !poolKind.empty()) {
        return &poolKind;
    } else if (const auto& found = findPoolByName(pools, channelBind.GetStoragePoolName()); found != pools.end()) {
        return &found->GetKind();
    }
    return nullptr;
};

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

    for (const auto& channelStats : tableStats.GetChannels()) {
        const ui32 channel = channelStats.GetChannel();
        if (channel < bindings.size()) {
            const auto& channelBind = bindings[channel];
            if (auto* poolKindPtr = GetPoolKind(channelBind, pools); poolKindPtr != nullptr) {
                auto& [dataSize, indexSize] = newStats.StoragePoolsStats[*poolKindPtr];
                dataSize += channelStats.GetDataSize();
                indexSize += channelStats.GetIndexSize();
            }
            // skip update for unknown pool kind
        }
        // skip update for unknown channel
        //NOTE: intentionally not logging to avoid flooding the log
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

    if (isDataShard && !Self->Tables.contains(pathId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Row table not found: " << pathId);
        return true;
    } else if (isOlapStore && !Self->OlapStores.contains(pathId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Olap store not found: " << pathId);
        return true;
    } else if (isColumnTable && !Self->ColumnTables.contains(pathId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Column table not found: " << pathId);
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
        if (!isDataShard) {
            return true;
        }

        table = Self->Tables[pathId];
        table->UpdateShardStatsForFollower(followerId, shardIdx, newStats);

        // NOTE: For split-by-size and merge-by-load cases it is sufficient
        //       to use EvPeriodicTableStats messages only from the leader
        //       as the trigger point. Using EvPeriodicTableStats messages from
        //       followers to start these operations would only introduce
        //       unnecessary load because the operations started from the follower
        //       messages and the leader message would compete with each other,
        //       but only one of them would win. These messages from leaders
        //       come frequently enough to trigger these operations within
        //       a reasonable time frame. More importantly, merge-by-load considers
        //       only the aggregated CPU usage across all followers (and the leader).
        //       This operation does not consider the CPU load on each individual
        //       follower (and the leader). And split-by-size does not even consider
        //       the CPU usage level when deciding to split a partition.
        //
        //       Only the split-by-load operation must be considered (and started)
        //       when the EvPeriodicTableStats message arrives from a follower
        //       because this operation should consider the CPU load on each specific
        //       follower (and the leader).
        const TTableInfo* mainTableForIndex = (Self->Indexes.contains(pathElement->ParentPathId))
            ? Self->GetMainTableForIndex(pathId)
            : nullptr;

        TString splitReason;

        if (!(table->CheckSplitByLoad(Self->SplitSettings, shardIdx, newStats.GetCurrentRawCpuUsage(), mainTableForIndex, splitReason))) {
            LOG_DEBUG_S(
                ctx,
                NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Do not want to split tablet " << datashardId
                    << " by the CPU load from the follower ID " << followerId
                    << ", reason: " << splitReason
            );

            return true;
        }

        LOG_NOTICE_S(
            ctx,
            NKikimrServices::FLAT_TX_SCHEMESHARD,
            "Want to split tablet " << datashardId
                << " by the CPU load from the follower ID " << followerId
                << ", reason: " << splitReason
        );

        VerifySplitAndRequestStats(
            ctx,
            item.Ev->Sender,
            datashardId,
            shardIdx,
            pathId,
            pathElement,
            subDomainInfo,
            newStats,
            true /* collectKeySample */
        );

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

                Self->ColumnTables.GetVerifiedPtr(tablePathId)->UpdateTableStats(shardIdx, tablePathId, newTableStats, now);
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

    // Save CPU resources when potential merge will certainly be immediately rejected by Self->IgniteOperation()
    // and potential split will probably be rejected later.
    TString inflightLimitErrStr;
    if (!Self->CheckInFlightLimit(TTxState::ETxType::TxSplitTablePartition, inflightLimitErrStr)) {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Do not consider split-merge: " << inflightLimitErrStr);
        return true;
    }

    const auto forceShardSplitSettings = Self->SplitSettings.GetForceShardSplitSettings();
    TVector<TShardIdx> shardsToMerge;
    TString mergeReason;
    if ((!index || index->State == NKikimrSchemeOp::EIndexStateReady)
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
            "Propose merge request: " << request->Record.ShortDebugString()
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
            "Do not want to split tablet " << datashardId << " by load,"
            << " its table already has "<< table->GetPartitions().size() << " out of " << table->GetMaxPartitionsCount() << " partitions");
        return true;
    } else if (table->CheckSplitByLoad(Self->SplitSettings, shardIdx, newStats.GetCurrentRawCpuUsage(), mainTableForIndex, reason)) {
        LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "Want to split tablet " << datashardId << " by load: " << reason);
        collectKeySample = true;
    } else {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "Do not want to split tablet " << datashardId << ": " << reason);
        return true;
    }

    // This partition needs to be split (by size or by load),
    // perform the final verification steps and send the EvGetTableStats request
    VerifySplitAndRequestStats(
        ctx,
        item.Ev->Sender,
        datashardId,
        shardIdx,
        pathId,
        pathElement,
        subDomainInfo,
        newStats,
        collectKeySample
    );

    return true;
}

bool TTxStoreTableStats::VerifySplitAndRequestStats(
    const TActorContext& ctx,
    const TActorId& statsEventSender,
    TTabletId datashardId,
    const TShardIdx& shardIdx,
    const TPathId& pathId,
    TPathElement::TPtr pathElement,
    TSubDomainInfo::TPtr subDomainInfo,
    const TPartitionStats& newPartitionStats,
    bool collectKeySample
) {
    // NOTE: intentionally avoid using TPath.Check().{PathShardsLimit,ShardsLimit}() here.
    // PathShardsLimit() no longer performs full shard count validation by iterating all ShardInfos
    // (too slow for this hot path), but still does additional lookups we want to avoid.
    {
        constexpr ui64 deltaShards = 2;
        if ((pathElement->GetShardsInside() + deltaShards) > subDomainInfo->GetSchemeLimits().MaxShardsInPath) {
            LOG_NOTICE_S(
                ctx,
                NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Do not request full stats from datashard " << datashardId
                    << ", reason: shards count limit exceeded (in path)"
                    << ", limit: " << subDomainInfo->GetSchemeLimits().MaxShardsInPath
                    << ", current: " << pathElement->GetShardsInside()
                    << ", delta: " << deltaShards
            );

            return false;
        }

        const auto currentShards = (subDomainInfo->GetShardsInside() - subDomainInfo->GetBackupShards());
        if ((currentShards + deltaShards) > subDomainInfo->GetSchemeLimits().MaxShards) {
            LOG_NOTICE_S(
                ctx,
                NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Do not request full stats from datashard " << datashardId
                    << ", datashard: " << datashardId
                    << ", reason: shards count limit exceeded (in subdomain)"
                    << ", limit: " << subDomainInfo->GetSchemeLimits().MaxShards
                    << ", current: " << currentShards
                    << ", delta: " << deltaShards
            );

            return false;
        }
    }

    if (newPartitionStats.HasBorrowedData) {
        LOG_NOTICE_S(
            ctx,
            NKikimrServices::FLAT_TX_SCHEMESHARD,
            "Postpone split tablet " << datashardId
                << " because it has borrow parts, enqueue compact them first"
        );

        Self->EnqueueBorrowedCompaction(shardIdx);
        return false;
    }

    // path.IsLocked() and path.LockedBy() equivalent
    if (const auto& found = Self->LockedPaths.find(pathId); found != Self->LockedPaths.end()) {
        const auto txId = found->second;
        LOG_NOTICE_S(
            ctx,
            NKikimrServices::FLAT_TX_SCHEMESHARD,
            "Postpone split tablet " << datashardId
                << " because it is locked by " << txId
        );

        return false;
    }

    // Request histograms from the datashard
    LOG_NOTICE_S(
        ctx,
        NKikimrServices::FLAT_TX_SCHEMESHARD,
        "Requesting full tablet stats " << datashardId
            << " to split it"
    );

    auto request = new TEvDataShard::TEvGetTableStats(pathId.LocalPathId);
    request->Record.SetCollectKeySample(collectKeySample);

    PendingMessages.emplace_back(statsEventSender, request);
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
