#include "schemeshard__stats_impl.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/cputime.h>
#include <ydb/core/protos/sys_view.pb.h>
#include <ydb/core/protos/table_stats.pb.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

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
    YDB_LOG_TRACE_CTX(ctx, "BuildStatsForCollector",
        {"datashard", datashardId},
        {"followerId", followerId},
    );

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
        , DemandTracking(AppData()->FeatureFlags.GetEnableSplitMergeDemandTracking())
    {
    }

    virtual ~TTxStoreTableStats() = default;

    void Complete(const TActorContext& ctx) override;

    // returns true to continue batching
    bool PersistSingleStats(const TPathId& pathId, const TStatsQueue<TEvDataShard::TEvPeriodicTableStats>::TItem& item, TInstant now, TTransactionContext& txc, const TActorContext& ctx) override;
    void ScheduleNextBatch(const TActorContext& ctx) override;

private:
    // Tx-level snapshot of the EnableSplitMergeDemandTracking feature flag.
    const bool DemandTracking;

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
     * @param[out] deferralReason reason why split must be deferred
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
        bool collectKeySample,
        TPartitionSplitMergeState::EDeferralReason& deferralReason
    );
};


THolder<TEvSchemeShard::TEvModifySchemeTransaction> MergeRequest(
    TSchemeShard* ss, TTxId& txId, TPathId& pathId, const TVector<TShardIdx>& shardsToMerge)
{
    auto request = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(txId), ui64(ss->TabletID()));
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

    return request;
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
    newStats.SmallBlobsVolumeBytes = tableStats.GetSmallBlobsVolumeBytes();
    newStats.SmallBlobsCount = tableStats.GetSmallBlobsCount();
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
        YDB_LOG_DEBUG_CTX(ctx, "PersistSingleStats: unknown pathId",
            {"pathId", pathId},
            {"tabletId", datashardId},
            {"followerId", followerId},
        );
        return true;
    }
    const auto& pathElement = pathElementIt->second;
    if (pathElement->Dropped()) {
        YDB_LOG_DEBUG_CTX(ctx, "PersistSingleStats: pathId is dropped",
            {"pathId", pathId},
            {"tabletId", datashardId},
            {"followerId", followerId},
        );
        return true;
    }

    const bool isDataShard = pathElement->IsTable();
    const bool isOlapStore = pathElement->IsOlapStore();
    const bool isColumnTable = pathElement->IsColumnTable();

    if (!isDataShard && !isOlapStore && !isColumnTable) {
        YDB_LOG_DEBUG_CTX(ctx, "Unexpected stats from shard",
            {"tabletId", datashardId},
        );
        return true;
    }

    TShardIdx shardIdx = [this, &datashardId]() {
        auto found = Self->TabletIdToShardIdx.find(datashardId);
        return (found != Self->TabletIdToShardIdx.end()) ? found->second : InvalidShardIdx;
    }();
    if (!shardIdx) {
        YDB_LOG_ERROR_CTX(ctx, "No shardIdx for shard",
            {"tabletId", datashardId},
        );
        return true;
    }

    YDB_LOG_DEBUG_CTX(ctx, "PersistSingleStats",
        {"pathId", pathId.LocalPathId},
        {"shardIdx", shardIdx},
        {"dataSize", dataSize},
        {"rowCount", rowCount},
    );
    const auto* shardInfo = Self->ShardInfos.FindPtr(shardIdx);
    if (!shardInfo) {
        YDB_LOG_DEBUG_CTX(ctx, "No ShardInfo by shardIdx",
            {"shardIdx", shardIdx},
            {"tabletId", datashardId},
        );
        return true;
    }

    auto subDomainInfo = Self->ResolveDomainInfo(pathElement);

    const TPartitionStats newStats = PrepareStats(rec, now, subDomainInfo->EffectiveStoragePools(), shardInfo->BindedChannels);

    YDB_LOG_DEBUG_CTX(ctx, "TTxStoreTableStats.PersistSingleStats: main stats from datashard",
        {"tabletId", datashardId},
        {"shardIdx", shardIdx},
        {"followerId", followerId},
        {"pathId", pathId},
        {"pathElement", pathElement->Name},
        {"isColumn", isColumnTable},
        {"isOlap", isOlapStore},
        {"rowCount", newStats.RowCount},
        {"dataSize", newStats.DataSize},
        {"hasBorrowedData", newStats.HasBorrowedData},
    );

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

        if (!Self->Tables.contains(pathId)) {
            YDB_LOG_WARN_CTX(ctx, "Row table not found",
                {"pathId", pathId},
            );
            return true;
        }

        table = Self->Tables.at(pathId);
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
            YDB_LOG_DEBUG_CTX(ctx, "Do not want to split tablet by the CPU load from the follower",
                {"tabletId", datashardId},
                {"followerId", followerId},
                {"reason", splitReason},
            );

            return true;
        }

        YDB_LOG_NOTICE_CTX(ctx, "Want to split tablet by the CPU load from the follower",
            {"tabletId", datashardId},
            {"followerId", followerId},
            {"reason", splitReason},
        );

        Self->NoteSplitDemandDetected();
        if (DemandTracking) {
            Self->RecordSplitDemand(pathId, *table, shardIdx, /* byLoad */ true, now);
        }

        // The leader and all followers report on the same physical partition (same shardIdx,
        // keyed by TShardIdx, not by FollowerId); mirror the leader path's success/deferralReason
        // handling below instead of discarding it, so a deferral detected here (borrowed data,
        // path lock, shard limits) is recorded exactly like it would be from the leader's own
        // report of the same shard.
        TPartitionSplitMergeState::EDeferralReason deferralReason = TPartitionSplitMergeState::EDeferralReason::InFlightLimit;
        const bool success = VerifySplitAndRequestStats(
            ctx,
            item.Ev->Sender,
            datashardId,
            shardIdx,
            pathId,
            pathElement,
            subDomainInfo,
            newStats,
            true /* collectKeySample */,
            deferralReason
        );
        if (!success) {
            Self->NoteSplitMergeDeferral();
            if (DemandTracking) {
                Self->RecordSplitDeferral(pathId, *table, shardIdx, deferralReason, now, DemandTracking);
            }
        }

        return true;
    }

    TDiskSpaceUsageDelta diskSpaceUsageDelta;
    i64 smallBlobsBytesDelta = 0;
    i64 smallBlobsCountDelta = 0;

    if (isDataShard) {
        if (!Self->Tables.contains(pathId)) {
            YDB_LOG_WARN_CTX(ctx, "Row table not found",
                {"pathId", pathId},
            );
            return true;
        }

        table = Self->Tables.at(pathId);
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
        if (!Self->OlapStores.contains(pathId)) {
            YDB_LOG_WARN_CTX(ctx, "Olap store not found",
                {"pathId", pathId},
            );
            return true;
        }

        TOlapStoreInfo::TPtr olapStore = Self->OlapStores.at(pathId);
        const ui64 prevSmallBlobsBytes = olapStore->Stats.Aggregated.SmallBlobsVolumeBytes;
        const ui64 prevSmallBlobsCount = olapStore->Stats.Aggregated.SmallBlobsCount;
        olapStore->UpdateShardStats(&diskSpaceUsageDelta, shardIdx, newStats, now);
        smallBlobsBytesDelta = static_cast<i64>(olapStore->Stats.Aggregated.SmallBlobsVolumeBytes) - static_cast<i64>(prevSmallBlobsBytes);
        smallBlobsCountDelta = static_cast<i64>(olapStore->Stats.Aggregated.SmallBlobsCount) - static_cast<i64>(prevSmallBlobsCount);
        updateSubdomainInfo = true;

        const auto tables = rec.GetTables();
        YDB_LOG_DEBUG_CTX(ctx, "OLAP store content",
            {"pathId", pathId},
            {"tableCount", tables.size()},
        );

        for (const auto& table : tables) {
            const TPartitionStats newTableStats = PrepareStats(table, now, {}, {});

            const TPathId tablePathId = TPathId(TOwnerId(pathId.OwnerId), TLocalPathId(table.GetTableLocalId()));

            if (Self->ColumnTables.contains(tablePathId)) {
                YDB_LOG_TRACE_CTX(ctx, "add stats for existing table",
                    {"pathId", tablePathId},
                );

                Self->ColumnTables.GetVerifiedPtr(tablePathId)->UpdateTableStats(shardIdx, tablePathId, newTableStats, now);
            } else {
                YDB_LOG_WARN_CTX(ctx, "Failed to add stats for table",
                    {"pathId", tablePathId},
                );
            }
        }

        YDB_LOG_DEBUG_CTX(ctx, "Aggregated stats for OLAP store",
            {"pathId", pathId.LocalPathId},
            {"rowCount", olapStore->Stats.Aggregated.RowCount},
            {"dataSize", olapStore->Stats.Aggregated.DataSize},
        );

    } else if (isColumnTable) {
        if (!Self->ColumnTables.contains(pathId)) {
            YDB_LOG_WARN_CTX(ctx, "Column table not found",
                {"pathId", pathId},
            );
            return true;
        }

        YDB_LOG_INFO_CTX(ctx, "PersistSingleStats: ColumnTable",
            {"tableCount", rec.GetTables().size()},
        );

        auto columnTable = Self->ColumnTables.GetVerifiedPtr(pathId);
        const ui64 prevSmallBlobsBytes = columnTable->Stats.Aggregated.SmallBlobsVolumeBytes;
        const ui64 prevSmallBlobsCount = columnTable->Stats.Aggregated.SmallBlobsCount;
        columnTable->UpdateShardStats(&diskSpaceUsageDelta, shardIdx, newStats, now);
        smallBlobsBytesDelta = static_cast<i64>(columnTable->Stats.Aggregated.SmallBlobsVolumeBytes) - static_cast<i64>(prevSmallBlobsBytes);
        smallBlobsCountDelta = static_cast<i64>(columnTable->Stats.Aggregated.SmallBlobsCount) - static_cast<i64>(prevSmallBlobsCount);
        updateSubdomainInfo = true;

        YDB_LOG_DEBUG_CTX(ctx, "Aggregated stats for column table",
            {"pathId", pathId.LocalPathId},
            {"rowCount", columnTable->Stats.Aggregated.RowCount},
            {"dataSize", columnTable->Stats.Aggregated.DataSize},
        );
    }

    if (updateSubdomainInfo) {
        subDomainInfo->AggrDiskSpaceUsage(Self, diskSpaceUsageDelta);
        subDomainInfo->AggrSmallBlobsUsage(Self, smallBlobsBytesDelta, smallBlobsCountDelta);
        if (subDomainInfo->CheckQuotas(Self)) {
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

    if (Self->TTLEnabledTables.contains(pathId)) {
        if (auto* p = table->GetPartitionStore().FindPtr(shardIdx)) {
            auto& lag = p->LastCondEraseLag;

            if (lag) {
                Self->TabletCounters->Percentile()[COUNTER_NUM_SHARDS_BY_TTL_LAG].DecrementFor(lag->Seconds());
            }

            if (now >= p->LastCondErase) {
                lag = now - p->LastCondErase;
            } else {
                lag = TDuration::Zero();
            }

            Self->TabletCounters->Percentile()[COUNTER_NUM_SHARDS_BY_TTL_LAG].IncrementFor(lag->Seconds());
        }
    }

    const TTableIndexInfo* index = Self->Indexes.Value(pathElement->ParentPathId, nullptr).Get();
    const TTableInfo* mainTableForIndex = (index ? Self->GetMainTableForIndex(pathId) : nullptr);

    // Save CPU resources when potential merge will certainly be immediately rejected by Self->IgniteOperation()
    // and potential split will probably be rejected later.
    // Stats processing is a hot path, so the expensive split-merge direction evaluation below
    // runs only when a slot is available -- otherwise, only once per new deferred episode
    // (to record the direction-aware deferral); shards already sitting in DeferredShards
    // short-circuit without re-evaluation.
    TString inflightLimitErrStr;
    const bool slotBlocked = !Self->CheckInFlightLimit(TTxState::ETxType::TxSplitTablePartition, inflightLimitErrStr);

    if (slotBlocked) {
        YDB_LOG_DEBUG_CTX(ctx, "Do not consider split-merge",
            {"reason", inflightLimitErrStr},
        );
        if (!DemandTracking) {
            return true;
        }
        if (const auto* deferred = table->GetTableSplitMergeState().DeferredShards.FindPtr(shardIdx)) {
            // Already deferred with a known direction and a recorded demand: nothing new to
            // learn here. The direction is known for free from the stored flag, so the
            // always-on demand-detection counter still re-reports the unmet demand each
            // stats cycle even under saturation (comparable across flag states).
            if (*deferred) {
                Self->NoteSplitDemandDetected();
            } else {
                Self->NoteMergeDemandDetected();
            }
            Self->NoteSplitMergeDeferral();
            return true;
        }
        // else check for demand type and record proper deferral
    }

    const auto forceShardSplitSettings = Self->SplitSettings.GetForceShardSplitSettings();

    // Determine the demand direction, so that a slot-limit deferral is recorded with the
    // shard's actual direction instead of being hardcoded as split demand (the former
    // PartitionsWithDeferredSplitDemand overcount caveat).
    TVector<TShardIdx> shardsToMerge;
    TString mergeReason;
    bool mergeByLoad = false;
    if ((!index || index->State == NKikimrSchemeOp::EIndexStateReady)
        && table->CheckCanMergePartitions(Self->SplitSettings, forceShardSplitSettings, shardIdx, Self->ShardInfos[shardIdx].TabletID, shardsToMerge, mainTableForIndex, now, mergeReason, mergeByLoad)) {

        Self->NoteMergeDemandDetected();
        if (DemandTracking) {
            Self->RecordMergeDemand(pathId, *table, shardIdx, mergeByLoad, now);
        }

        if (slotBlocked) {
            // The slot limit is exhausted: this partition is a deferred candidate. Record it with
            // the now-known direction so the fair scheduler can service it (round-robin) when a
            // slot frees, instead of waiting for the next stats cycle (whoever reports first
            // would otherwise win).
            Self->NoteSplitMergeDeferral();
            if (DemandTracking) {
                Self->RecordMergeDeferral(pathId, *table, shardIdx, TPartitionSplitMergeState::EDeferralReason::InFlightLimit, now, DemandTracking);
            }
            return true;
        }

        TTxId txId = Self->GetCachedTxId(ctx);

        if (!txId) {
            YDB_LOG_WARN_CTX(ctx, "Do not request merge op: no cached tx ids for internal operation",
                {"shardIdx", shardIdx},
                {"mergeSize", shardsToMerge.size()},
            );
            // The merge is wanted but cannot start without an internal txId: record the
            // deferral so the fair scheduler retries when txIds are available again.
            Self->NoteSplitMergeDeferral();
            if (DemandTracking) {
                Self->RecordMergeDeferral(pathId, *table, shardIdx, TPartitionSplitMergeState::EDeferralReason::NoCachedTxId, now, DemandTracking);
            }
            return true;
        }

        auto request = MergeRequest(Self, txId, Self->ShardInfos[shardIdx].PathId, shardsToMerge);

        YDB_LOG_INFO_CTX(ctx, "Propose merge request",
            {"request", request->Record.ShortDebugString()},
            {"reason", mergeReason},
        );

        TMemoryChanges memChanges;
        TStorageChanges dbChanges;
        TProposeContext context{Self, txc, ctx, MergeOpSideEffects, memChanges, dbChanges};

        auto response = Self->IgniteOperation(*request, context);

        dbChanges.Apply(Self, txc, ctx);
        MergeOpSideEffects.ApplyOnExecute(Self, txc, ctx);

        if (DemandTracking) {
            // Only clear the deferred state when the op actually ignited; on rejection the
            // recorded demand must survive so the shard keeps its fair-scheduling turn.
            if (response->IsAccepted() || response->IsDone() || response->IsConditionalAccepted()) {
                for (const TShardIdx& merged : shardsToMerge) {
                    Self->RecordMergeApplied(pathId, *table, merged, now);
                }
            }
        }

        return false;
    }
    if (rec.GetShardState() != NKikimrTxDataShard::Ready) {
        return true;
    }

    TString splitReason;
    bool splitByLoad = false;
    if (table->ShouldSplitBySize(dataSize, forceShardSplitSettings, splitReason)) {
        // We would like to split by size and do this no matter how many partitions there are
        YDB_LOG_NOTICE_CTX(ctx, "Want to split tablet by size",
            {"datashard", datashardId},
            {"reason", splitReason},
        );

        Self->NoteSplitDemandDetected();
        if (DemandTracking) {
            Self->RecordSplitDemand(pathId, *table, shardIdx, splitByLoad, now);
        }

        if (slotBlocked) {
            Self->NoteSplitMergeDeferral();
            if (DemandTracking) {
                Self->RecordSplitDeferral(pathId, *table, shardIdx, TPartitionSplitMergeState::EDeferralReason::InFlightLimit, now, DemandTracking);
            }
            return true;
        }
    } else if (table->GetPartitions().size() >= table->GetMaxPartitionsCount()) {
        // We cannot split as there are max partitions already
        YDB_LOG_DEBUG_CTX(ctx, "Do not want to split tablet by load: table already has max partitions",
            {"datashard", datashardId},
            {"currentPartitions", table->GetPartitions().size()},
            {"maxPartitions", table->GetMaxPartitionsCount()},
        );
        return true;
    } else if (table->CheckSplitByLoad(Self->SplitSettings, shardIdx, newStats.GetCurrentRawCpuUsage(), mainTableForIndex, splitReason)) {
        YDB_LOG_NOTICE_CTX(ctx, "Want to split tablet by load",
            {"datashard", datashardId},
            {"reason", splitReason},
        );
        splitByLoad = true;

        Self->NoteSplitDemandDetected();
        if (DemandTracking) {
            Self->RecordSplitDemand(pathId, *table, shardIdx, splitByLoad, now);
        }

        if (slotBlocked) {
            Self->NoteSplitMergeDeferral();
            if (DemandTracking) {
                Self->RecordSplitDeferral(pathId, *table, shardIdx, TPartitionSplitMergeState::EDeferralReason::InFlightLimit, now, DemandTracking);
            }
            return true;
        }
    } else {
        // avoid double logging in case of in-flight limit blocks passive partition
        if (!slotBlocked) {
            YDB_LOG_DEBUG_CTX(ctx, "Do not want to split tablet",
                {"datashard", datashardId},
                {"reason", splitReason},
            );
        }
        // Demand expiry: this shard no longer meets any split or merge criteria with a
        // free slot, so a previously recorded deferral is stale. Drop it now, otherwise
        // the shard stays permanently queued (nonzero backlog gauges, endless stats
        // re-requests on every revisit wave). If the demand returns, the next stats
        // cycle re-records it.
        if (table->GetTableSplitMergeState().DeferredShards.contains(shardIdx)) {
            Self->RemoveDeferredPartition(pathId, *table, shardIdx);
        }
        return true;
    }

    // This partition really wants to be split (by size or by load).
    // Perform the final verification steps and send the EvGetTableStats request
    TPartitionSplitMergeState::EDeferralReason deferralReason = TPartitionSplitMergeState::EDeferralReason::InFlightLimit;
    const bool success = VerifySplitAndRequestStats(
        ctx,
        item.Ev->Sender,
        datashardId,
        shardIdx,
        pathId,
        pathElement,
        subDomainInfo,
        newStats,
        splitByLoad /* collectKeySample */,
        deferralReason
    );
    if (!success) {
        Self->NoteSplitMergeDeferral();
        if (DemandTracking) {
            Self->RecordSplitDeferral(pathId, *table, shardIdx, deferralReason, now, DemandTracking);
        }
    }

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
    bool collectKeySample,
    TPartitionSplitMergeState::EDeferralReason& deferralReason
) {
    // NOTE: intentionally avoid using TPath.Check().{PathShardsLimit,ShardsLimit}() here.
    // PathShardsLimit() no longer performs full shard count validation by iterating all ShardInfos
    // (too slow for this hot path), but still does additional lookups we want to avoid.
    {
        constexpr ui64 deltaShards = 2;
        if ((pathElement->GetShardsInside() + deltaShards) > subDomainInfo->GetSchemeLimits().MaxShardsInPath) {
            YDB_LOG_NOTICE_CTX(ctx, "Do not request full stats from datashard: shards count limit exceeded (in path)",
                {"datashard", datashardId},
                {"limit", subDomainInfo->GetSchemeLimits().MaxShardsInPath},
                {"current", pathElement->GetShardsInside()},
                {"delta", deltaShards},
            );

            deferralReason = TPartitionSplitMergeState::EDeferralReason::ShardLimitPath;
            return false;
        }

        const auto currentShards = (subDomainInfo->GetShardsInside() - subDomainInfo->GetBackupShards());
        if ((currentShards + deltaShards) > subDomainInfo->GetSchemeLimits().MaxShards) {
            YDB_LOG_NOTICE_CTX(ctx, "Do not request full stats from datashard: shards count limit exceeded (in subdomain)",
                {"datashard", datashardId},
                {"limit", subDomainInfo->GetSchemeLimits().MaxShards},
                {"current", currentShards},
                {"delta", deltaShards},
            );

            deferralReason = TPartitionSplitMergeState::EDeferralReason::ShardLimitSubdomain;
            return false;
        }
    }

    if (newPartitionStats.HasBorrowedData) {
        YDB_LOG_NOTICE_CTX(ctx, "Postpone split tablet: it has borrowed parts, enqueue compact them first",
            {"datashard", datashardId},
        );

        deferralReason = TPartitionSplitMergeState::EDeferralReason::Borrowed;
        Self->EnqueueBorrowedCompaction(shardIdx);
        return false;
    }

    // path.IsLocked() and path.LockedBy() equivalent
    if (const auto& found = Self->LockedPaths.find(pathId); found != Self->LockedPaths.end()) {
        const auto txId = found->second;
        YDB_LOG_NOTICE_CTX(ctx, "Postpone split tablet: it is locked by tx",
            {"datashard", datashardId},
            {"txId", txId},
        );

        deferralReason = TPartitionSplitMergeState::EDeferralReason::PathLocked;
        return false;
    }

    // Request histograms from the datashard
    YDB_LOG_NOTICE_CTX(ctx, "Requesting full tablet stats to split it",
        {"datashard", datashardId},
    );

    auto request = new TEvDataShard::TEvGetTableStats(pathId.LocalPathId);
    request->Record.SetCollectKeySample(collectKeySample);

    PendingMessages.emplace_back(statsEventSender, request);
    return true;
}

void TTxStoreTableStats::Complete(const TActorContext& ctx) {
    MergeOpSideEffects.ApplyOnComplete(Self, ctx);

    if (DemandTracking) {
        Self->UpdateSplitMergeCounters();
    } else {
        // Flag off: zero the gauges so they don't freeze at stale values from before the toggle.
        Self->ResetSplitMergeCounters();
    }

    for (auto& m: PendingMessages) {
        Y_ABORT_UNLESS(m.Event);
        ctx.Send(m.Actor, m.Event.Release());
    }

    Queue.WriteQueueSizeMetric();
}

void TTxStoreTableStats::ScheduleNextBatch(const TActorContext& ctx) {
    Self->ExecuteTableStatsBatch(ctx);
}

namespace {

// Triggers the lazy deserialization of a raw TEvDataShard::TEvPeriodicTableStats on this actor's
// thread (off the schemeshard's), then bounces the same handle back wrapped as
// TEvPrivate::TEvPeriodicTableStatsParsed. TEventPBBase caches the parsed record after the first
// Get()/Load() (event_pb.h), so the schemeshard's own Get() is a cache hit, not a second parse.
class TStatsParserActor : public TActor<TStatsParserActor> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SCHEMESHARD_STATS_PARSER;
    }

    explicit TStatsParserActor(const TActorId& selfActorId)
        : TActor(&TThis::StateWork)
        , SelfActorId(selfActorId)
    {}

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvDataShard::TEvPeriodicTableStats, Handle);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
        }
    }

private:
    void Handle(TEvDataShard::TEvPeriodicTableStats::TPtr& ev, const TActorContext& ctx) {
        ev->Get();
        ctx.Send(SelfActorId, new TEvPrivate::TEvPeriodicTableStatsParsed(std::move(ev)));
    }

    const TActorId SelfActorId;
};

} // anonymous namespace

IActor* CreateStatsParserActor(const TActorId& selfActorId) {
    return new TStatsParserActor(selfActorId);
}

void TSchemeShard::Handle(TEvDataShard::TEvPeriodicTableStats::TPtr& ev, const TActorContext& ctx) {
    const auto start = AppData()->MonotonicTimeProvider->Now();
    if (AppData()->FeatureFlags.GetEnablePeriodicTableStatsParseOffload()) {
        ctx.Send(ev->Forward(StatsParserActorId));
    } else {
        HandlePeriodicTableStats(ev, ctx);
    }
    const auto elapsed = AppData()->MonotonicTimeProvider->Now() - start;
    TabletCounters->Cumulative()[COUNTER_PERIODIC_TABLE_STATS_HANDLE_TIME_NS].Increment(elapsed.NanoSeconds());
}

void TSchemeShard::Handle(TEvPrivate::TEvPeriodicTableStatsParsed::TPtr& ev, const TActorContext& ctx) {
    HandlePeriodicTableStats(ev->Get()->Ev, ctx);
}

void TSchemeShard::HandlePeriodicTableStats(TEvDataShard::TEvPeriodicTableStats::TPtr& ev, const TActorContext& ctx) {
    auto* msg = ev->Get();
    const auto& rec = msg->Record;

    TabletCounters->Percentile()[COUNTER_PERIODIC_TABLE_STATS_ARENA_SPACE_USED].IncrementFor(msg->Arena->Get()->SpaceUsed());

    auto datashardId = TTabletId(rec.GetDatashardId());
    const ui32 followerId = rec.GetFollowerId();
    const auto& tableStats = rec.GetTableStats();
    const auto& tabletMetrics = rec.GetTabletMetrics();
    ui64 dataSize = tableStats.GetDataSize();
    ui64 rowCount = tableStats.GetRowCount();

    TPathId pathId = rec.HasTableOwnerId()
            ? TPathId(TOwnerId(rec.GetTableOwnerId()), TLocalPathId(rec.GetTableLocalId()))
            : MakeLocalId(TLocalPathId(rec.GetTableLocalId()));

    YDB_LOG_DEBUG_CTX(ctx, "Got periodic table stats",
        {"datashard", datashardId},
        {"followerId", followerId},
        {"pathId", pathId},
        {"state", DatashardStateName(rec.GetShardState())},
        {"dataSize", dataSize},
        {"rowCount", rowCount},
        {"cpuUsage", tabletMetrics.GetCPU()/10000.0},
        {"schemeshard", TabletID()},
    );

    YDB_LOG_TRACE_CTX(ctx, "Got periodic table stats (raw)",
        {"datashard", datashardId},
        {"followerId", followerId},
        {"pathId", pathId},
        {"statsMessage", tableStats.ShortDebugString()},
        {"schemeshard", TabletID()},
    );

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
    YDB_LOG_DEBUG_CTX(ctx, "Started TEvPersistStats",
        {"tableStatsQueueSize", TableStatsQueue.Size()},
        {"schemeshard", TabletID()},
    );

    TableStatsBatchScheduled = false;
    ExecuteTableStatsBatch(ctx);
}

void TSchemeShard::ExecuteTableStatsBatch(const TActorContext& ctx) {
    if (!TablePersistStatsPending && !TableStatsQueue.Empty()) {
        TablePersistStatsPending = true;
        EnqueueExecute(new TTxStoreTableStats(this, TableStatsQueue, TablePersistStatsPending));
        YDB_LOG_TRACE_CTX(ctx, "Will execute TTxStoreStats",
            {"tableStatsQueueSize", TableStatsQueue.Size()},
        );
        ScheduleTableStatsBatch(ctx);
    }
}

void TSchemeShard::ScheduleTableStatsBatch(const TActorContext& ctx) {
    if (!TableStatsBatchScheduled && !TableStatsQueue.Empty()) {
        TDuration delay = TableStatsQueue.Delay();
        YDB_LOG_TRACE_CTX(ctx, "Will delay TTxStoreTableStats",
            {"delay", delay},
            {"tableStatsQueueSize", TableStatsQueue.Size()},
        );

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

// ===================== Split/merge candidacy memory + fair scheduler =====================

void TSchemeShard::RecordSplitDemand(const TPathId& /*pathId*/, TTableInfo& table, const TShardIdx& shardIdx, bool byLoad, TInstant now) {
    auto& h = table.MutablePartitionSplitMergeState(shardIdx);
    ++h.SplitCandidateCount;
    h.LastSplitCandidate = now;
    (byLoad ? h.LoadThresholdCross.SplitSide : h.SizeThresholdCross.SplitSide) = now;
}

void TSchemeShard::RecordMergeDemand(const TPathId& /*pathId*/, TTableInfo& table, const TShardIdx& shardIdx, bool byLoad, TInstant now) {
    auto& h = table.MutablePartitionSplitMergeState(shardIdx);
    ++h.MergeCandidateCount;
    h.LastMergeCandidate = now;
    // Stamp only the threshold that actually triggered, so the per-reason crossing
    // history stays meaningful (the caller derives byLoad from the merge reason).
    (byLoad ? h.LoadThresholdCross.MergeSide : h.SizeThresholdCross.MergeSide) = now;
}

void TSchemeShard::RecordSplitDeferral(const TPathId& pathId, TTableInfo& table, const TShardIdx& shardIdx,
        TPartitionSplitMergeState::EDeferralReason reason, TInstant now, bool demandTracking) {
    RecordSplitMergeDeferralImpl(pathId, table, shardIdx, /* wantsSplit */ true, reason, now, demandTracking);
}

void TSchemeShard::RecordMergeDeferral(const TPathId& pathId, TTableInfo& table, const TShardIdx& shardIdx,
        TPartitionSplitMergeState::EDeferralReason reason, TInstant now, bool demandTracking) {
    RecordSplitMergeDeferralImpl(pathId, table, shardIdx, /* wantsSplit */ false, reason, now, demandTracking);
}

void TSchemeShard::RecordSplitMergeDeferralImpl(const TPathId& pathId, TTableInfo& table, const TShardIdx& shardIdx,
        bool wantsSplit, TPartitionSplitMergeState::EDeferralReason reason, TInstant now, bool demandTracking) {
    auto& h = table.MutablePartitionSplitMergeState(shardIdx);
    ++h.DeferredCount(wantsSplit);
    h.RecordDeferral(reason);

    auto& tableState = table.MutableTableSplitMergeState();
    const auto [it, newlyDeferred] = tableState.DeferredShards.emplace(shardIdx, wantsSplit);
    if (newlyDeferred) {
        ++tableState.DemandCount(wantsSplit);
        if (tableState.DeferredShards.size() == 1) {
            TablesWithDeferredSplitMerge.insert(pathId);
        }
    } else if (it->second != wantsSplit) {
        // Re-deferral with a changed direction: move the count so the aggregate always
        // mirrors the latest direction of the deferral.
        it->second = wantsSplit;
        tableState.MoveDemandCount(wantsSplit);
    }

    const TInstant candidate = h.LastCandidate(wantsSplit) ? h.LastCandidate(wantsSplit) : now;
    if (!tableState.OldestPendingCandidateAt || candidate < tableState.OldestPendingCandidateAt) {
        tableState.OldestPendingCandidateAt = candidate;
    }

    table.UpdateSplitMergePickCache(shardIdx);

    EnqueueSplitMergeRevisit(pathId, table, TActivationContext::AsActorContext(), demandTracking);
}

void TSchemeShard::RemoveDeferredPartition(const TPathId& pathId, TTableInfo& table, const TShardIdx& shardIdx) {
    // Delegate the per-table bookkeeping to TTableInfo::DropFromSplitMergeState (single source of
    // truth for stored-direction decrement + erase + cache invalidate + recompute), then do the
    // global membership cleanup that only the schemeshard side knows about.
    table.DropFromSplitMergeState(shardIdx);
    if (table.GetTableSplitMergeState().DeferredShards.empty()) {
        TablesWithDeferredSplitMerge.erase(pathId);
    }
}

void TSchemeShard::RecordSplitApplied(const TPathId& pathId, TTableInfo& table, const TShardIdx& shardIdx, TInstant now) {
    RecordSplitMergeAppliedImpl(pathId, table, shardIdx, /* wasSplit */ true, now);
}

void TSchemeShard::RecordMergeApplied(const TPathId& pathId, TTableInfo& table, const TShardIdx& shardIdx, TInstant now) {
    RecordSplitMergeAppliedImpl(pathId, table, shardIdx, /* wasSplit */ false, now);
}

void TSchemeShard::RecordSplitMergeAppliedImpl(const TPathId& pathId, TTableInfo& table, const TShardIdx& shardIdx,
    bool wasSplit, TInstant now) {
    // Drop from the deferred set first (reads history direction), then clear the stuck counters.
    RemoveDeferredPartition(pathId, table, shardIdx);

    auto& h = table.MutablePartitionSplitMergeState(shardIdx);
    h.ResetOnSplitMerge();
    if (wasSplit) {
        h.LastSplitTime = now;
    } else {
        h.LastMergeTime = now;
    }
    table.MutableTableSplitMergeState().LastSplitMergeTime = now;
}

void TSchemeShard::ResetSplitMergeCounters() {
    TabletCounters->Simple()[COUNTER_PARTITIONS_WITH_DEFERRED_SPLIT_DEMAND].Set(0);
    TabletCounters->Simple()[COUNTER_PARTITIONS_WITH_DEFERRED_MERGE_DEMAND].Set(0);
    TabletCounters->Simple()[COUNTER_PARTITIONS_WITH_DEFERRED_SPLIT_MERGE].Set(0);
    TabletCounters->Simple()[COUNTER_TABLES_WITH_DEFERRED_SPLIT_MERGE].Set(0);
}

void TSchemeShard::UpdateSplitMergeCounters() {
    // Derive the global totals from the authoritative per-table aggregates (once per stats batch,
    // O(tables-with-pending) -- not on the per-stat hot path). This also self-heals the membership
    // set: ApplySplitMerge can drain a table's deferred set without a Self-side drop (no global
    // access there), leaving a stale entry here; we prune such entries while summing.
    ui64 splitDemand = 0;
    ui64 mergeDemand = 0;
    TVector<TPathId> drained;
    for (const TPathId& pathId : TablesWithDeferredSplitMerge) {
        auto* tablePtr = Tables.FindPtr(pathId);
        if (!tablePtr) {
            drained.push_back(pathId);  // table dropped
            continue;
        }
        const auto& tableState = (*tablePtr)->GetTableSplitMergeState();
        if (tableState.DeferredShards.empty()) {
            drained.push_back(pathId);
            continue;
        }
        splitDemand += tableState.SplitDemandCount;
        mergeDemand += tableState.MergeDemandCount;
    }
    for (const TPathId& pathId : drained) {
        TablesWithDeferredSplitMerge.erase(pathId);
    }

    TabletCounters->Simple()[COUNTER_PARTITIONS_WITH_DEFERRED_SPLIT_DEMAND].Set(splitDemand);
    TabletCounters->Simple()[COUNTER_PARTITIONS_WITH_DEFERRED_MERGE_DEMAND].Set(mergeDemand);
    // Convenience sum of the two gauges above (kept for dashboards; no independent information).
    TabletCounters->Simple()[COUNTER_PARTITIONS_WITH_DEFERRED_SPLIT_MERGE].Set(splitDemand + mergeDemand);
    TabletCounters->Simple()[COUNTER_TABLES_WITH_DEFERRED_SPLIT_MERGE].Set(TablesWithDeferredSplitMerge.size());
}

void TSchemeShard::NoteSplitDemandDetected() {
    TabletCounters->Cumulative()[COUNTER_SPLIT_DEMAND_DETECTED].Increment(1);
}

void TSchemeShard::NoteMergeDemandDetected() {
    TabletCounters->Cumulative()[COUNTER_MERGE_DEMAND_DETECTED].Increment(1);
}

void TSchemeShard::NoteSplitMergeDeferral() {
    TabletCounters->Cumulative()[COUNTER_SPLIT_MERGE_DEFERRALS].Increment(1);
}

void TSchemeShard::EnqueueSplitMergeRevisit(const TPathId& pathId, TTableInfo& table, const TActorContext& ctx, bool demandTracking) {
    if (!IsSplitMergeFairSchedulingEnabled(demandTracking)) {
        return;
    }
    auto& tableState = table.MutableTableSplitMergeState();
    if (!tableState.QueuedForRevisit) {
        tableState.QueuedForRevisit = true;
        SplitMergeRevisitQueue.push_back(pathId);
    }
    ScheduleSplitMergeRevisit(ctx);
}

void TSchemeShard::ScheduleSplitMergeRevisit(const TActorContext& ctx) {
    // Coalesce: at most one re-eval event in flight regardless of finish rate.
    if (SplitMergeRevisitScheduled || SplitMergeRevisitQueue.empty()) {
        return;
    }
    SplitMergeRevisitScheduled = true;
    ctx.Send(SelfId(), new TEvPrivate::TEvRevisitSplitMerge());
}

class TTxRevisitSplitMerge: public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
    TSideEffects SideEffects;
    // Datashards to re-request fresh stats from (re-drives a deferred split with a freed slot).
    struct TStatsRequest {
        TTabletId TabletId;
        TLocalPathId LocalPathId;
        bool CollectKeySample;
    };
    TVector<TStatsRequest> StatsRequests;
    bool DemandTracking = false;

public:
    explicit TTxRevisitSplitMerge(TSchemeShard* ss)
        : TBase(ss)
    {}

    TTxType GetTxType() const override {
        return TXTYPE_REVISIT_SPLIT_MERGE;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        Self->SplitMergeRevisitScheduled = false;

        DemandTracking = AppData()->FeatureFlags.GetEnableSplitMergeDemandTracking();
        if (!Self->IsSplitMergeFairSchedulingEnabled(DemandTracking)) {
            // Gate off: drain the queue and clear the membership guard, otherwise still-deferred
            // tables keep QueuedForRevisit=true and are silently skipped after re-enabling.
            while (!Self->SplitMergeRevisitQueue.empty()) {
                const TPathId pathId = Self->SplitMergeRevisitQueue.front();
                Self->SplitMergeRevisitQueue.pop_front();
                if (auto* tablePtr = Self->Tables.FindPtr(pathId)) {
                    (*tablePtr)->MutableTableSplitMergeState().QueuedForRevisit = false;
                }
            }
            return true;
        }

        const TInstant now = ctx.Now();
        // Round-robin: give each currently-waiting table at most one turn this wave. The number of
        // ops that actually start is bounded by free slots (CheckInFlightLimit, re-checked each turn).
        size_t turns = Self->SplitMergeRevisitQueue.size();
        TString inflightErr;
        while (turns-- > 0 && !Self->SplitMergeRevisitQueue.empty()) {
            if (!Self->CheckInFlightLimit(TTxState::ETxType::TxSplitTablePartition, inflightErr)) {
                break;
            }

            const TPathId pathId = Self->SplitMergeRevisitQueue.front();
            Self->SplitMergeRevisitQueue.pop_front();

            auto* tablePtr = Self->Tables.FindPtr(pathId);
            if (!tablePtr) {
                continue;  // table gone; drop from the queue
            }
            TTableInfo::TPtr table = *tablePtr;
            if (table->GetTableSplitMergeState().DeferredShards.empty()) {
                table->MutableTableSplitMergeState().QueuedForRevisit = false;
                continue;
            }

            const TShardIdx shardIdx = table->PickMostDeferredPartition();
            if (shardIdx != InvalidShardIdx) {
                if (!TryMergeInline(pathId, table, shardIdx, now, txc, ctx)) {
                    // Not mergeable from cached stats -> re-request fresh stats to re-drive a split
                    // with the now-free slot (the histogram path fires it and records it).
                    if (const auto* shardInfo = Self->ShardInfos.FindPtr(shardIdx)) {
                        // Key sample is only needed to compute a split boundary; skip the
                        // extra datashard work for merge-wanting shards.
                        const bool wantsSplit = table->GetTableSplitMergeState().DeferredShards.Value(shardIdx, true);
                        StatsRequests.push_back({shardInfo->TabletID, pathId.LocalPathId, wantsSplit});
                    }
                }
            }

            // Re-append to the tail if demand remains; otherwise leave the queue.
            if (!table->GetTableSplitMergeState().DeferredShards.empty()) {
                Self->SplitMergeRevisitQueue.push_back(pathId);
            } else {
                table->MutableTableSplitMergeState().QueuedForRevisit = false;
            }
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        SideEffects.ApplyOnComplete(Self, ctx);

        // Prune stale TablesWithDeferredSplitMerge entries (dropped tables, drained deferred
        // sets) even when stats batches stop flowing, so the gauges never freeze at a stale
        // non-zero value. UpdateSplitMergeCounters recomputes the gauges from authoritative
        // per-table state and prunes membership; it is cheap (O(tables-with-pending)).
        if (DemandTracking) {
            Self->UpdateSplitMergeCounters();
        }

        for (const auto& req : StatsRequests) {
            auto request = MakeHolder<TEvDataShard::TEvGetTableStats>(req.LocalPathId);
            request->Record.SetCollectKeySample(req.CollectKeySample);
            Self->PipeClientCache->Send(ctx, ui64(req.TabletId), request.Release());
        }
        // Intentionally NOT self-rescheduling: re-eval is edge-triggered (slot-free / borrowed-done).
        // Self-rescheduling would busy-loop re-requesting stats whose responses are still in flight.
    }

private:
    bool TryMergeInline(const TPathId& pathId, TTableInfo::TPtr table, const TShardIdx& shardIdx,
            TInstant now, TTransactionContext& txc, const TActorContext& ctx) {
        const auto* shardInfo = Self->ShardInfos.FindPtr(shardIdx);
        if (!shardInfo) {
            return false;
        }
        const auto pathElementIt = Self->PathsById.find(pathId);
        if (pathElementIt == Self->PathsById.end() || pathElementIt->second->Dropped()) {
            return false;
        }

        // Mirror the split-path pre-checks: a locked path would reject the propose inside
        // IgniteOperation anyway -- cheaper to catch it here and keep the deferred state.
        if (Self->LockedPaths.contains(pathId)) {
            return false;
        }

        const TTableIndexInfo* index = Self->Indexes.Value(pathElementIt->second->ParentPathId, nullptr).Get();
        const TTableInfo* mainTableForIndex = (index ? Self->GetMainTableForIndex(pathId) : nullptr);
        const auto forceShardSplitSettings = Self->SplitSettings.GetForceShardSplitSettings();

        TVector<TShardIdx> shardsToMerge;
        TString mergeReason;
        bool mergeByLoad = false;
        if ((index && index->State != NKikimrSchemeOp::EIndexStateReady)
            || !table->CheckCanMergePartitions(Self->SplitSettings, forceShardSplitSettings, shardIdx,
                shardInfo->TabletID, shardsToMerge, mainTableForIndex, now, mergeReason, mergeByLoad)) {
            return false;
        }

        TTxId txId = Self->GetCachedTxId(ctx);
        if (!txId) {
            return false;
        }

        auto request = MergeRequest(Self, txId, Self->ShardInfos[shardIdx].PathId, shardsToMerge);

        YDB_LOG_INFO_CTX(ctx, "Fair scheduler proposing merge",
            {"reason", mergeReason},
            {"message", request->Record.ShortDebugString()}
        );

        TMemoryChanges memChanges;
        TStorageChanges dbChanges;
        TProposeContext context{Self, txc, ctx, SideEffects, memChanges, dbChanges};

        auto response = Self->IgniteOperation(*request, context);

        dbChanges.Apply(Self, txc, ctx);
        SideEffects.ApplyOnExecute(Self, txc, ctx);

        // Only clear the deferred state when the op actually ignited; on rejection the
        // recorded demand survives so the shard keeps its fair-scheduling turn.
        const bool ignited = response
            && (response->IsAccepted() || response->IsDone() || response->IsConditionalAccepted());
        if (!ignited) {
            YDB_LOG_NOTICE_CTX(ctx, "Fair scheduler merge propose rejected; deferred state kept",
                {"status", response ? NKikimrScheme::EStatus_Name(response->Record.GetStatus()) : TString("unknown")},
                {"reason", response ? response->Record.GetReason() : TString()},
            );
            return true;
        }

        for (const TShardIdx& merged : shardsToMerge) {
            Self->RecordMergeApplied(pathId, *table, merged, now);
        }
        return true;
    }
};

void TSchemeShard::Handle(TEvPrivate::TEvRevisitSplitMerge::TPtr&, const TActorContext& ctx) {
    Execute(new TTxRevisitSplitMerge(this), ctx);
}

}}

#undef YDB_LOG_THIS_FILE_COMPONENT
