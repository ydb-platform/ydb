#include "schemeshard_impl.h"
#include <ydb/core/base/appdata.h>
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
    ctx.Send(ev->Forward(SysPartitionStatsCollector));
}

auto TSchemeShard::BuildStatsForCollector(TPathId pathId, TShardIdx shardIdx, TTabletId datashardId,
    TMaybe<ui32> nodeId, TMaybe<ui64> startTime, const TTableInfo::TPartitionStats& stats)
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
    TEvDataShard::TEvPeriodicTableStats::TPtr Ev;

    THolder<NSysView::TEvSysView::TEvSendPartitionStats> StatsCollectorEv;
    THolder<TEvDataShard::TEvGetTableStats> GetStatsEv;
    THolder<TEvDataShard::TEvCompactBorrowed> CompactEv;

    TSideEffects MergeOpSideEffects;

public:
    explicit TTxStorePartitionStats(TSelf* self, TEvDataShard::TEvPeriodicTableStats::TPtr& ev)
        : TBase(self)
        , Ev(ev)
    {
    }

    virtual ~TTxStorePartitionStats() = default;

    TTxType GetTxType() const override {
        return TXTYPE_STORE_PARTITION_STATS;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;

}; // TTxStorePartitionStats

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
    const auto& rec = Ev->Get()->Record;
    auto datashardId = TTabletId(rec.GetDatashardId());
    TPathId tableId = InvalidPathId;
    if (rec.HasTableOwnerId()) {
        tableId = TPathId(TOwnerId(rec.GetTableOwnerId()),
                          TLocalPathId(rec.GetTableLocalId()));
    } else {
        tableId = Self->MakeLocalId(TLocalPathId(rec.GetTableLocalId()));
    }

    const auto& tableStats = rec.GetTableStats();
    const auto& tabletMetrics = rec.GetTabletMetrics();
    ui64 dataSize = tableStats.GetDataSize();
    ui64 rowCount = tableStats.GetRowCount();

    if (!Self->Tables.contains(tableId)) {
        return true;
    }

    TTableInfo::TPtr table = Self->Tables[tableId];

    if (!Self->TabletIdToShardIdx.contains(datashardId)) {
        return true;
    }

    auto shardIdx = Self->TabletIdToShardIdx[datashardId];
    const auto forceShardSplitSettings = Self->SplitSettings.GetForceShardSplitSettings();

    TTableInfo::TPartitionStats newStats;
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

    auto oldAggrStats = table->GetStats().Aggregated;
    table->UpdateShardStats(shardIdx, newStats);

    if (!table->IsBackup) {
        Self->UpdateBackgroundCompaction(shardIdx, newStats);
        Self->UpdateShardMetrics(shardIdx, newStats);
    }

    if (!newStats.HasBorrowedData) {
        Self->RemoveBorrowedCompaction(shardIdx);
    }

    NIceDb::TNiceDb db(txc.DB);

    if (!table->IsBackup && !table->IsShardsStatsDetached()) {
        auto newAggrStats = table->GetStats().Aggregated;
        auto subDomainId = Self->ResolveDomainId(tableId);
        auto subDomainInfo = Self->ResolveDomainInfo(tableId);
        subDomainInfo->AggrDiskSpaceUsage(Self, newAggrStats, oldAggrStats);
        if (subDomainInfo->CheckDiskSpaceQuotas(Self)) {
            Self->PersistSubDomainState(db, subDomainId, *subDomainInfo);
            // Publish is done in a separate transaction, so we may call this directly
            TDeque<TPathId> toPublish;
            toPublish.push_back(subDomainId);
            Self->PublishToSchemeBoard(TTxId(), std::move(toPublish), ctx);
        }
    }

    Self->PersistTablePartitionStats(db, tableId, shardIdx, table);

    if (AppData(ctx)->FeatureFlags.GetEnableSystemViews()) {
        TMaybe<ui32> nodeId;
        if (rec.HasNodeId()) {
            nodeId = rec.GetNodeId();
        }
        TMaybe<ui64> startTime;
        if (rec.HasStartTime()) {
            startTime = rec.GetStartTime();
        }
        StatsCollectorEv = Self->BuildStatsForCollector(tableId, shardIdx, datashardId, nodeId, startTime, newStats);
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

        return true;
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
        TPathElement::TPtr path = Self->PathsById.at(tableId);
        TSubDomainInfo::TPtr domainInfo = Self->ResolveDomainInfo(tableId);

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
    GetStatsEv.Reset(new TEvDataShard::TEvGetTableStats(tableId.LocalPathId, dataSizeResolution, rowCountResolution, collectKeySample));

    return true;
}

void TTxStorePartitionStats::Complete(const TActorContext& ctx) {
    MergeOpSideEffects.ApplyOnComplete(Self, ctx);

    if (StatsCollectorEv) {
        ctx.Send(Self->SysPartitionStatsCollector, StatsCollectorEv.Release());
    }

    if (CompactEv) {
        LOG_DEBUG(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Requesting borrowed compaction from datasbard %" PRIu64, Ev->Get()->Record.GetDatashardId());
        ctx.Send(Ev->Sender, CompactEv.Release());
    }

    if (GetStatsEv) {
        LOG_DEBUG(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                 "Requesting full stats from datashard %" PRIu64, Ev->Get()->Record.GetDatashardId());
        ctx.Send(Ev->Sender, GetStatsEv.Release());
    }
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
                                                     << " from datashard " << datashardId
                                                     << " pathId " << pathId
                                                     << " state '" << DatashardStateName(rec.GetShardState()) << "'"
                                                     << " dataSize " << dataSize
                                                     << " rowCount " << rowCount
                                                     << " cpuUsage " << tabletMetrics.GetCPU()/10000.0);

    Execute(new TTxStorePartitionStats(this, ev), ctx);
}

}}
