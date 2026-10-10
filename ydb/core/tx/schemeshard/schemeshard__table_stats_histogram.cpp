#include "schemeshard_impl.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/table_stats.pb.h>
#include <ydb/core/tablet_flat/flat_stat_table.h>
#include <ydb/core/split/split.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

namespace NKikimr {
namespace NSchemeShard {

TSerializedCellVec ChooseSplitKeyByHistogram(const NKikimrTableStats::THistogram& histogram, const TConstArrayRef<NScheme::TTypeInfo> &keyColumnTypes, ui64 totalSize) {
    const auto &buckets = histogram.GetBuckets();

    NTable::THistogram hist;
    hist.reserve(buckets.size());
    for (const auto& bucket : buckets) {
        hist.emplace_back(bucket.GetKey(), bucket.GetValue());
    }

    return NSplitMerge::SelectShortestMedianKeyPrefix(hist, totalSize, keyColumnTypes);
}

TSerializedCellVec ChooseSplitKeyByKeySample(const NKikimrTableStats::THistogram& keySample, const TConstArrayRef<NScheme::TTypeInfo>& keyColumnTypes, bool sortHistogram) {
    const auto &buckets = keySample.GetBuckets();

    TVector<std::pair<TSerializedCellVec, ui64>> hist;
    hist.reserve(buckets.size());
    for (const auto& bucket : buckets) {
        hist.emplace_back(TSerializedCellVec(bucket.GetKey()), bucket.GetValue());
    }
    if (sortHistogram) {
        NSplitMerge::MakeKeyAccessHistogram(hist, keyColumnTypes);
    }
    NSplitMerge::ConvertToCumulativeHistogram(hist);

    return NSplitMerge::SelectShortestMedianKeyPrefix(hist, keyColumnTypes);
}

// Version 0: KeyAccessSample (if present) contains unsorted, repeated keys with unit (in practice) weights.
// SplitByLoadSuggestedKey is never present.
// Then: Schemeshard must sort, accumulate and build cumulative histogram from KeyAccessSample
// and select split boundary/key prefix from it.
//
// Version 1: KeyAccessSample (if present) contains already sorted and deduplicated array with accumulated weights (but not turned into cumulative).
// SplitByLoadSuggestedKey is never present.
// Then: Schemeshard must build cumulative histogram directly from KeyAccessSample
// and select split boundary/key prefix from it.
//
// Version 2: KeyAccessSample is irrelevant. SplitByLoadSuggestedKey (if present) contains split boundary/key prefix already selected by a datashard.
// Then: Schemeshard must directly use suggested split boundary.
//
// Version 3: KeyAccessSample is never present. SplitByLoadSuggestedKey (if present) contains split boundary/key prefix already selected by a datashard.
// Then: Schemeshard must directly use suggested split boundary.
//
// Version 4+: Unknown version. Can't suggest that stats contain anything useful.
//
TSerializedCellVec GetSplitBoundaryByLoad(const NKikimrTableStats::TTableStats& inputStats, const TConstArrayRef<NScheme::TTypeInfo> &keyColumnTypes) {
    const ui32 protocolVersion = inputStats.GetSplitProtocolVersion();

    switch (protocolVersion) {
        case 0:
        case 1:
            if (inputStats.HasKeyAccessSample()) {
                const bool sortHistogram = (protocolVersion == 0);
                return ChooseSplitKeyByKeySample(inputStats.GetKeyAccessSample(), keyColumnTypes, sortHistogram);
            }
            break;
        case 2:
        case 3:
            if (inputStats.HasSplitByLoadSuggestedKey()) {
                return TSerializedCellVec(inputStats.GetSplitByLoadSuggestedKey());
            }
            break;
        default:
            // unknown version: can't use anything from the stats
            break;
    }

    return {};
}
bool HasDataForSplitByLoad(const NKikimrTableStats::TTableStats& inputStats) {
    const ui32 protocolVersion = inputStats.GetSplitProtocolVersion();
    switch (protocolVersion) {
        case 0:
        case 1:
            return inputStats.HasKeyAccessSample();
        case 2:
        case 3:
            return inputStats.HasSplitByLoadSuggestedKey();
        default:
            return false;
    }
}

// Version 0 and 1: DataSizeHistogram may be present, SplitBySizeSuggestedKey is never present.
// Then: Schemeshard must select split boundary/key prefix from DataSizeHistogram.
//
// Version 2: DataSizeHistogram is irrelevant, SplitBySizeSuggestedKey (if present) contains split boundary/key prefix already selected by a datashard.
// Then: Schemeshard must directly use suggested split boundary.
//
// Version 3: DataSizeHistogram is never present, SplitBySizeSuggestedKey (if present) contains split boundary/key prefix already selected by a datashard.
// Then: Schemeshard must directly use suggested split boundary.
//
// Version 4+: Unknown version. Can't suggest that stats contain anything useful.
//
TSerializedCellVec GetSplitBoundaryBySize(const NKikimrTableStats::TTableStats& inputStats, const TConstArrayRef<NScheme::TTypeInfo> &keyColumnTypes) {
    const ui32 protocolVersion = inputStats.GetSplitProtocolVersion();

    switch (protocolVersion) {
        case 0:
        case 1:
            if (inputStats.HasDataSizeHistogram()) {
                //NOTE: Selecting multiple split boundaries is unsafe — no guarantee that
                // resulting parts will have meaningful sizes (SST split may be unpredictable).
                return ChooseSplitKeyByHistogram(inputStats.GetDataSizeHistogram(), keyColumnTypes, inputStats.GetDataSize());
            }
            break;
        case 2:
        case 3:
            if (inputStats.HasSplitBySizeSuggestedKey()) {
                return TSerializedCellVec(inputStats.GetSplitBySizeSuggestedKey());
            }
            break;
        default:
            // unknown version: can't use anything from the stats
            break;
    }

    return {};
}
bool HasDataForSplitBySize(const NKikimrTableStats::TTableStats& inputStats) {
    const ui32 protocolVersion = inputStats.GetSplitProtocolVersion();
    switch (protocolVersion) {
        case 0:
        case 1:
            return inputStats.HasDataSizeHistogram();
        case 2:
        case 3:
            return inputStats.HasSplitBySizeSuggestedKey();
        default:
            return false;
    }
}

enum struct ESplitReason {
    NO_SPLIT = 0,
    SPLIT_BY_SIZE,
    SPLIT_BY_LOAD
};

const char* ToString(ESplitReason splitReason) {
    switch (splitReason) {
    case ESplitReason::NO_SPLIT:
        return "No split";
    case ESplitReason::SPLIT_BY_SIZE:
        return "Split by size";
    case ESplitReason::SPLIT_BY_LOAD:
        return "Split by load";
    default:
        Y_DEBUG_ABORT_UNLESS(!"Unexpected enum value");
        return "Unexpected enum value";
    }
}

class TTxPartitionHistogram: public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
    TEvDataShard::TEvGetTableStatsResult::TPtr Ev;

    TSideEffects SplitOpSideEffects;

public:
    explicit TTxPartitionHistogram(TSelf* self, TEvDataShard::TEvGetTableStatsResult::TPtr& ev)
        : TBase(self)
        , Ev(ev)
        , DemandTracking(AppData()->FeatureFlags.GetEnableSplitMergeDemandTracking())
    {
    }

    virtual ~TTxPartitionHistogram() = default;

    TTxType GetTxType() const override {
        return TXTYPE_PARTITION_HISTOGRAM;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;

private:
    // Tx-level snapshot of the EnableSplitMergeDemandTracking feature flag.
    const bool DemandTracking;

}; // TTxStorePartitionStats


void TSchemeShard::Handle(TEvDataShard::TEvGetTableStatsResult::TPtr& ev, const TActorContext& ctx) {
    auto* msg = ev->Get();
    const auto& rec = msg->Record;

    TabletCounters->Percentile()[COUNTER_GET_TABLE_STATS_RESULT_ARENA_SPACE_USED].IncrementFor(msg->Arena->Get()->SpaceUsed());

    auto datashardId = TTabletId(rec.GetDatashardId());
    ui64 dataSize = rec.GetTableStats().GetDataSize();
    ui64 rowCount = rec.GetTableStats().GetRowCount();

    YDB_LOG_NOTICE_CTX(ctx, "Got partition histogram",
        {"datashard", datashardId},
        {"datashardState", DatashardStateName(rec.GetShardState())},
        {"dataSize", dataSize},
        {"rowCount", rowCount},
        {"dataSizeBucketCount", rec.GetTableStats().GetDataSizeHistogram().BucketsSize()},
        {"fullStatsReady", rec.GetFullStatsReady()},
        {"schemeshard", TabletID()},
    );

    Execute(new TTxPartitionHistogram(this, ev), ctx);
}


TSmallVec<NScheme::TTypeInfo> GetKeyColumnTypes(const TTableInfo& tableInfo) {
    TSmallVec<NScheme::TTypeInfo> keyColumnTypes(tableInfo.KeyColumnIds.size());
    for (size_t ki = 0; ki < tableInfo.KeyColumnIds.size(); ++ki) {
        keyColumnTypes[ki] = tableInfo.Columns.FindPtr(tableInfo.KeyColumnIds[ki])->PType;
    }
    return keyColumnTypes;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> SplitRequest(
    TSchemeShard* ss, TTxId& txId, const TPathId& pathId, TTabletId datashardId, const TString& keyBuff,
    bool loadSplitLineage)
{
    auto request = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(txId), ui64(ss->SelfTabletId()));
    auto& record = request->Record;

    TPath tablePath = TPath::Init(pathId, ss);

    auto& propose = *record.AddTransaction();
    propose.SetFailOnExist(false);
    propose.SetOperationType(NKikimrSchemeOp::ESchemeOpSplitMergeTablePartitions);
    propose.SetInternal(true);

    propose.SetWorkingDir(tablePath.Parent().PathString());

    auto& split = *propose.MutableSplitMergeTablePartitions();
    split.SetTablePath(tablePath.PathString());
    // OwnerId/LocalId let the propose resolve the table without a path lookup and let
    // TSplitMerge::Propose attribute a lock rejection to the deferred shards (pathId
    // is derived from these fields, not from TablePath).
    split.SetTableOwnerId(ui64(pathId.OwnerId));
    split.SetTableLocalId(pathId.LocalPathId);
    split.SetSchemeshardId(ss->TabletID());

    split.AddSourceTabletId(ui64(datashardId));
    split.AddSplitBoundary()->SetSerializedKeyPrefix(keyBuff);
    // Travels with the op (persisted in TxInFlightV2 with the tx state) so ApplySplitMerge,
    // run at op completion, knows whether to deepen the by-load split lineage -- no fire-time stamping.
    split.SetLoadSplitLineage(loadSplitLineage);

    return request;
}

bool TTxPartitionHistogram::Execute(TTransactionContext& txc, const TActorContext& ctx) {
    const auto& rec = Ev->Get()->Record;

    // NOTE: EvGetTableStatsResult must contain data for split-by-size or split-by-load decisions.
    //
    // Split-by-size: data size histogram or preselected split boundary (leader only)
    // Split-by-load: key access sample or preselected split boundary (leader or followers)
    bool trySplitBySize = (
        (rec.GetFollowerId() == 0) &&
        (rec.GetFullStatsReady()) &&
        HasDataForSplitBySize(rec.GetTableStats())
    );

    bool trySplitByLoad = HasDataForSplitByLoad(rec.GetTableStats());

    const TTabletId datashardId = TTabletId(rec.GetDatashardId());
    const TPathId tableId = (rec.HasTableOwnerId())
        ? TPathId(TOwnerId(rec.GetTableOwnerId()), TLocalPathId(rec.GetTableLocalId()))
        : Self->MakeLocalId(TLocalPathId(rec.GetTableLocalId()));

    // Save CPU resources when potential split will certainly be immediately rejected by Self->IgniteOperation()
    TString inflightLimitErrStr;
    if ((trySplitBySize || trySplitByLoad)
            && !Self->CheckInFlightLimit(TTxState::ETxType::TxSplitTablePartition, inflightLimitErrStr)) {
        YDB_LOG_DEBUG_CTX(ctx, "TTxPartitionHistogram Do not process detailed partition statistics",
            {"error", inflightLimitErrStr},
            {"datashard", datashardId},
            {"followerId", rec.GetFollowerId()},
            {"pathId", tableId},
            {"datashardState", DatashardStateName(rec.GetShardState())},
            {"dataSizeBucketCount", rec.GetTableStats().GetDataSizeHistogram().GetBuckets().size()},
            {"keyAccessBucketCount", rec.GetTableStats().GetKeyAccessSample().GetBuckets().size()},
            {"schemeshard", Self->TabletID()},
        );
        // Slot limit exhausted at the histogram stage too: this partition is a deferred split
        // candidate (histogram data only drives splits, so the direction is known here) --
        // record it for the fair scheduler (resolve its shard/table locally first).
        Self->NoteSplitMergeDeferral();
        if (DemandTracking) {
            const auto shardIt = Self->TabletIdToShardIdx.find(datashardId);
            if (auto* table = Self->Tables.FindPtr(tableId);
                    table && shardIt != Self->TabletIdToShardIdx.end()
                    // Live-partition check: a delayed histogram response can outlive the shard
                    // (split/merge/drop reshaped the table). Recording deferral for a dead
                    // shardIdx would pollute DeferredShards, the pick cache and the gauges.
                    && (*table)->GetPartitionStore().contains(shardIt->second)) {
                Self->RecordSplitDeferral(tableId, **table, shardIt->second,
                    TPartitionSplitMergeState::EDeferralReason::InFlightLimit, ctx.Now(), DemandTracking);
            }
        }
        return true;
    }

    YDB_LOG_INFO_CTX(ctx, "TTxPartitionHistogram Process detailed partition statistics",
        {"schemeshard", Self->TabletID()},
        {"datashard", datashardId},
        {"followerId", rec.GetFollowerId()},
        {"pathId", tableId},
        {"datashardState", DatashardStateName(rec.GetShardState())},
        {"dataSizeBucketCount", rec.GetTableStats().GetDataSizeHistogram().GetBuckets().size()},
        {"keyAccessBucketCount", rec.GetTableStats().GetKeyAccessSample().GetBuckets().size()},
    );

    const TTableInfo::TPtr tableInfo = Self->Tables.Value(tableId, nullptr);

    if (!tableInfo) {
        YDB_LOG_DEBUG_CTX(ctx, "TTxPartitionHistogram Unknown table",
            {"tableId", tableId},
            {"tablet", datashardId},
        );
        return true;
    }

    const auto shardIt = Self->TabletIdToShardIdx.find(datashardId);

    if (shardIt == Self->TabletIdToShardIdx.end()) {
        YDB_LOG_DEBUG_CTX(ctx, "TTxPartitionHistogram Unknown tablet",
            {"tablet", datashardId},
        );
        return true;
    }

    const auto& shardIdx = shardIt->second;

    // Live-partition check: the revisit route (TTxRevisitSplitMerge) re-requests stats for a
    // shard picked from cached DeferredShards, and this response can arrive after the table
    // was reshaped (split/merge/drop). TabletIdToShardIdx still resolves the tablet until
    // shard teardown, so a dead shardIdx would otherwise flow into the propose and into
    // RecordSplitApplied. The main stats path is guarded at its sender
    // (VerifySplitAndRequestStats); the revisit sender has no such guard, so the check
    // must live here, at the point of mutation.
    if (!tableInfo->GetPartitionStore().contains(shardIdx)) {
        YDB_LOG_DEBUG_CTX(ctx, "TTxPartitionHistogram Shard is not a live partition of the table",
            {"datashard", datashardId},
            {"shardIdx", shardIdx},
            {"pathId", tableId},
        );
        return true;
    }

    if (!trySplitBySize && !trySplitByLoad) {
        // The response carries no split evidence (e.g. a revisit re-request answered by a
        // cooled-down shard). A previously recorded deferral is stale now: expire it,
        // otherwise the gauges freeze at stale nonzero values forever (Finding 26). The
        // inline counter refresh is required because nothing else runs after this point
        // when periodic stats are blocked and the revisit queue is drained.
        //
        // Expiry requires authoritative evidence (Review B regression (b)): only a leader's
        // FullStatsReady response proves the shard produced its full stats and they show no
        // split demand. A not-ready response (heavy load -- precisely the split-by-load
        // scenario) or a follower sample must NOT drop a previously recorded deferral and
        // the shard's queue seniority; the next periodic cycle re-records demand if it
        // returns. And only split-wanting deferrals are expired (regression (a)): this
        // response says nothing about merge demand, so a merge deferral must survive it.
        const auto* deferred = tableInfo->GetTableSplitMergeState().DeferredShards.FindPtr(shardIdx);
        if (deferred
                && deferred->WantsSplit
                && rec.GetFollowerId() == 0
                && rec.GetFullStatsReady()) {
            Self->RemoveDeferredPartition(tableId, *tableInfo, shardIdx);
            Self->UpdateSplitMergeCounters();
        }
        return true;
    }

    // Borrowed-data recheck. Pre-PR the only sender of TEvGetTableStats was
    // VerifySplitAndRequestStats, which never requests stats for a shard with borrowed
    // parts. The revisit sender bypasses that guard (it re-requests precisely because its
    // cached state is stale, and it is edge-triggered by global slot-free/borrowed-done
    // events, not by this shard's own recovery). The authoritative borrowed state
    // (UserTablePartOwners) is in this response, so the guard belongs here: a shard with
    // borrowed parts must not be split (back-borrow chains break DataShard part
    // ref-counting). Just defer; borrowed compaction is already enqueued by the periodic
    // stats path, and its completion re-nudges the scheduler.
    const bool hasBorrowedData = [&rec]() {
        for (ui64 tabletId : rec.GetUserTablePartOwners()) {
            if (tabletId != rec.GetDatashardId()) {
                return true;
            }
        }
        return false;
    }();
    if (hasBorrowedData) {
        YDB_LOG_NOTICE_CTX(ctx, "TTxPartitionHistogram Postpone split tablet: it has borrowed parts",
            {"datashard", datashardId},
            {"shardIdx", shardIdx},
            {"pathId", tableId},
        );
        // Count the deferral unconditionally (like every other deferral site) so the
        // cumulative COUNTER_SPLIT_MERGE_DEFERRALS does not undercount borrowed
        // deferrals (Finding 31b); the structured recording stays flag-gated.
        Self->NoteSplitMergeDeferral();
        if (DemandTracking) {
            Self->RecordSplitDeferral(tableId, *tableInfo, shardIdx,
                TPartitionSplitMergeState::EDeferralReason::Borrowed, ctx.Now(), DemandTracking);
        }
        return true;
    }

    // Don't split/merge backup tables
    if (tableInfo->IsBackup) {
        YDB_LOG_NOTICE_CTX(ctx, "TTxPartitionHistogram Skip backup table",
            {"datashard", datashardId},
        );
        return true;
    }

    const auto path = TPath::Init(tableId, Self);

    if (path.IsLocked()) {
        YDB_LOG_NOTICE_CTX(ctx, "TTxPartitionHistogram Skip locked table",
            {"datashard", datashardId},
            {"lockedBy", path.LockedBy()},
        );
        // Record the lock as the deferral reason so the revisit path stops re-requesting
        // stats for this shard (fresh stats cannot resolve a path lock; the drop-lock edge
        // re-nudges the scheduler instead).
        if (DemandTracking) {
            tableInfo->UpdateDeferredShardReason(shardIdx,
                TPartitionSplitMergeState::EDeferralReason::PathLocked);
        }
        return true;
    }

    // The first priority is split-by-size
    ESplitReason splitReason = ESplitReason::NO_SPLIT;
    TString splitReasonMsg;

    if (trySplitBySize) {
        if (tableInfo->ShouldSplitBySize(
            rec.GetTableStats().GetDataSize(),
            Self->SplitSettings.GetForceShardSplitSettings(),
            splitReasonMsg
        )) {
            splitReason = ESplitReason::SPLIT_BY_SIZE;
        }
    }

    // The second priority is split-by-load
    if ((splitReason == ESplitReason::NO_SPLIT) && trySplitByLoad) {
        // NOTE: When considering split-by-load, prefer using the current CPU usage
        //       from the EvGetTableStatsResult message. It is the most recent
        //       and the most accurate. However, it may not be present in some cases.
        //       If this happens, use the cached CPU usage, which is reported
        //       by the leader though the EvPeriodicTableStats messages.
        ui64 currentCpuUsage = rec.GetTabletMetrics().GetCPU();

        if (!(rec.GetTabletMetrics().HasCPU())) {
            const auto* stats = tableInfo->GetStats().PartitionStats.FindPtr(shardIdx);

            if (!stats) {
                YDB_LOG_DEBUG_CTX(ctx, "TTxPartitionHistogram Unknown shard index",
                    {"shardIdx", shardIdx},
                    {"datashard", datashardId},
                    {"pathId", tableId},
                    {"schemeshard", Self->TabletID()},
                );

                return true;
            }

            currentCpuUsage = stats->GetCurrentRawCpuUsage();
        }

        if (tableInfo->CheckSplitByLoad(
            Self->SplitSettings,
            shardIdx,
            currentCpuUsage,
            Self->GetMainTableForIndex(tableId),
            splitReasonMsg
        )) {
            splitReason = ESplitReason::SPLIT_BY_LOAD;

            if (tableInfo->GetPartitions().size() >= tableInfo->GetMaxPartitionsCount()) {
                YDB_LOG_DEBUG_CTX(ctx, "TTxPartitionHistogram Do not want to split tablet by load, its table already has the maximum number of partitions",
                    {"datashard", datashardId},
                    {"partitions", tableInfo->GetPartitions().size()},
                    {"maxPartitions", tableInfo->GetMaxPartitionsCount()},
                );

                return true;
            }
        }
    }

    if (splitReason == ESplitReason::NO_SPLIT) {
        YDB_LOG_DEBUG_CTX(ctx, "TTxPartitionHistogram Do not want to split tablet",
            {"datashard", datashardId},
            {"reason", splitReasonMsg},
        );
        // Fresh split evidence says the shard no longer wants to split: a previously
        // recorded deferral is stale -- expire it and refresh the gauges inline
        // (Finding 26). Only split-wanting deferrals: this branch evaluates split
        // criteria only and carries no information about merge demand, so a merge
        // deferral must survive it (Review B regression (a)).
        const auto* deferred = tableInfo->GetTableSplitMergeState().DeferredShards.FindPtr(shardIdx);
        if (deferred && deferred->WantsSplit) {
            Self->RemoveDeferredPartition(tableId, *tableInfo, shardIdx);
            Self->UpdateSplitMergeCounters();
        }
        return true;
    }

    YDB_LOG_NOTICE_CTX(ctx, "TTxPartitionHistogram Want to split",
        {"datashard", datashardId},
        {"splitKind", ToString(splitReason)},
        {"reason", splitReasonMsg},
    );

    TTxId txId = Self->GetCachedTxId(ctx);

    if (!txId) {
        YDB_LOG_WARN_CTX(ctx, "TTxPartitionHistogram Do not request split: no cached tx ids for internal operation",
            {"datashard", datashardId},
            {"shardIdx", shardIdx},
            {"splitKind", ToString(splitReason)},
            {"reason", splitReasonMsg},
        );
        return true;
    }

    const auto getSplitBoundary = (splitReason == ESplitReason::SPLIT_BY_LOAD ? GetSplitBoundaryByLoad : GetSplitBoundaryBySize);

    TSerializedCellVec splitKey = getSplitBoundary(rec.GetTableStats(), GetKeyColumnTypes(*tableInfo));

    const auto specialType = tableInfo->TableDescription.GetPartitionConfig().GetSpecialTableType();
    if (specialType == NKikimrSchemeOp::ESpecialTableType::ESpecialTableTypeFulltextCompact ||
        specialType == NKikimrSchemeOp::ESpecialTableType::ESpecialTableTypeFulltextCompactRelevance) {
        const auto prefixSize = tableInfo->KeyColumnIds.size() - NTableIndex::NFulltext::CompactTableKeySize;
        if (splitKey.GetCells().size() > prefixSize + 1) {
            // For now, only allow to split compact fulltext index table by prefix + __ydb_token
            splitKey = TSerializedCellVec(splitKey.GetCells().Slice(0, prefixSize + 1));
        }
    }

    if (splitKey.GetBuffer().empty()) {
        YDB_LOG_WARN_CTX(ctx, "TTxPartitionHistogram Failed to find proper split key",
            {"splitKind", ToString(splitReason)},
            {"reason", splitReasonMsg},
            {"datashard", datashardId},
        );
        Self->ReturnTxIdToCache(txId);
        return true;
    }

    auto request = SplitRequest(Self, txId, tableId, datashardId, splitKey.GetBuffer(),
        /* loadSplitLineage */ splitReason == ESplitReason::SPLIT_BY_LOAD);

    YDB_LOG_NOTICE_CTX(ctx, "TTxPartitionHistogram Propose",
        {"datashard", datashardId},
        {"splitKind", ToString(splitReason)},
        {"reason", splitReasonMsg},
        {"message", request->Record.ShortDebugString()},
    );

    TMemoryChanges memChanges;
    TStorageChanges dbChanges;
    TProposeContext context{Self, txc, ctx, SplitOpSideEffects, memChanges, dbChanges};

    auto response = Self->IgniteOperation(*request, context);

    dbChanges.Apply(Self, txc, ctx);
    SplitOpSideEffects.ApplyOnExecute(Self, txc, ctx);

    // Only clear the deferred state when the op actually ignited; on rejection the
    // recorded demand must survive so the shard keeps its fair-scheduling turn.
    const bool ignited = response
        && (response->IsAccepted() || response->IsDone() || response->IsConditionalAccepted());
    if (!ignited) {
        YDB_LOG_NOTICE_CTX(ctx, "Histogram split propose rejected; deferred state kept",
            {"status", response ? NKikimrScheme::EStatus_Name(response->Record.GetStatus()) : TString("unknown")},
            {"reason", response ? response->Record.GetReason() : TString()},
        );
        return true;
    }

    if (DemandTracking) {
        // The shard got a slot: drop it from the deferred set and reset its stuck counters.
        // The by-load reason is NOT stamped here -- it travels with the op
        // (TTxState::LoadSplitLineage, persisted in TxInFlightV2) and is applied at op completion.
        Self->RecordSplitApplied(tableId, *tableInfo, shardIdx, ctx.Now());
    }

    return true;
}


void TTxPartitionHistogram::Complete(const TActorContext& ctx) {
    SplitOpSideEffects.ApplyOnComplete(Self, ctx);
}

}}

#undef YDB_LOG_THIS_FILE_COMPONENT
