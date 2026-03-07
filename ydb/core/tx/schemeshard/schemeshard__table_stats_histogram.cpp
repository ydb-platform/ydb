#include "schemeshard_impl.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/table_stats.pb.h>
#include <ydb/core/tablet_flat/flat_stat_table.h>
#include <ydb/core/split/split.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

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

// Version 0: KeyAccessSample contains unsorted, repeated keys with unit (in practice) weights.
// SplitByAccessSuggestedKey not present.
// Then: Schemeshard must sort, accumulate and build cumulative histogram from KeyAccessSample
// and select split boundary/key prefix from it.
//
// Version 1: KeyAccessSample contains already sorted and deduplicated array with accumulated weights (but not turned into cumulative).
// SplitByAccessSuggestedKey may be present -- split boundary/key prefix already selected by a datashard.
// Then: Schemeshard can skip sorting and accumulation steps and build cumulative histogram directly from KeyAccessSample
// to select split boundary/key prefix from it.
// Or directly use split boundary suggested by a datashard instead.
// Depending on feature flags.
//
// Version 2+: Unknown version. Expect suggested key, expect KeyAccessSample but assume it non-sorted.
// Then: Schemeshard must use suggested key if present, else go version 0 path.
//
// GetSplitBoundaryByLoadModeFlags translates feature flags and input stats into directives for
// whether schemeshard should sort (and accumulate) an input histogram,
// whether it should select split boundary from the histogram,
// whether it should directly use split boundary from the input.
std::pair<bool, bool> GetSplitBoundaryByLoadModeFlags(ui32 protocolVersion, bool canUseSuggestedKey, bool shouldSortHistogram) {
    Cerr << "TEST GetSplitBoundaryByLoadModeFlags, protocolVersion " << protocolVersion << ", canUseSuggestedKey " << canUseSuggestedKey << ", shouldSortHistogram " << shouldSortHistogram << Endl;

    bool useSuggestedKey = ((protocolVersion >= 1)
        && canUseSuggestedKey
    );
    bool sortHistogram = ((protocolVersion != 1)
        || shouldSortHistogram
    );

    Cerr << "TEST GetSplitBoundaryByLoadModeFlags, use suggested key " << useSuggestedKey << ", sortHistogram " << sortHistogram << Endl;

    return std::make_pair(useSuggestedKey, sortHistogram);
}
TSerializedCellVec GetSplitBoundaryByLoad(
    const NKikimrTableStats::TTableStats& inputStats,
    const TConstArrayRef<NScheme::TTypeInfo> &keyColumnTypes,
    bool canUseSuggestedKey,
    bool shouldSortHistogram
) {
    const auto [useSuggestedKey, sortHistogram] = GetSplitBoundaryByLoadModeFlags(inputStats.GetSplitProtocolVersion(), canUseSuggestedKey, shouldSortHistogram);

    Cerr << "TEST GetSplitBoundaryByLoad, use suggested key " << useSuggestedKey << ", sortHistogram " << sortHistogram << Endl;

    if (useSuggestedKey && inputStats.HasSplitByAccessSuggestedKey()) {
        return TSerializedCellVec(inputStats.GetSplitByAccessSuggestedKey());
    }
    if (inputStats.HasKeyAccessSample()) {
        const auto& keySample = inputStats.GetKeyAccessSample();
        return ChooseSplitKeyByKeySample(keySample, keyColumnTypes, sortHistogram);
    } else {
        return TSerializedCellVec();
    }
}

// Version 0: DataSizeHistogram present, SplitBySizeSuggestedKey not present.
// Then: Schemeshard must select split boundary/key prefix from DataSizeHistogram.
//
// Version 1: DataSizeHistogram present.
// SplitBySizeSuggestedKey may be present -- split boundary/key prefix already selected by a datashard.
// Then: Schemeshard can directly use the suggested split boundary.
// Or ignore it and select split boundary/key prefix from DataSizeHistogram.
// Depending on feature flag.
//
// Version 2+: Unknown version. Expect suggested key, expect DataSizeHistogram
// Then: Schemeshard must use suggested key if present, else go version 0 path.
//
// GetSplitBoundaryBySizeModeFlag translates feature flag and input stats into directive for
// whether schemeshard should select split boundary key from input histogram
// or directly use split boundary from the input.
bool GetSplitBoundaryBySizeModeFlag(ui32 protocolVersion, bool canUseSuggestedKey) {
    bool useSuggestedKey = ((protocolVersion >= 1)
        && canUseSuggestedKey
    );

    return useSuggestedKey;
}
TSerializedCellVec GetSplitBoundaryBySize(
    const NKikimrTableStats::TTableStats& inputStats,
    const TConstArrayRef<NScheme::TTypeInfo> &keyColumnTypes,
    bool canUseSuggestedKey
) {
    bool useSuggestedKey = GetSplitBoundaryBySizeModeFlag(inputStats.GetSplitProtocolVersion(), canUseSuggestedKey);

    if (useSuggestedKey && inputStats.HasSplitBySizeSuggestedKey()) {
        return TSerializedCellVec(inputStats.GetSplitBySizeSuggestedKey());
    }
    if (inputStats.HasDataSizeHistogram()) {
        //NOTE: Selecting multiple split boundaries is unsafe — no guarantee that
        // resulting parts will have meaningful sizes (SST split may be unpredictable).
        const auto& histogram = inputStats.GetDataSizeHistogram();
        return ChooseSplitKeyByHistogram(histogram, keyColumnTypes, inputStats.GetDataSize());
    } else {
        return TSerializedCellVec();
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
    {
    }

    virtual ~TTxPartitionHistogram() = default;

    TTxType GetTxType() const override {
        return TXTYPE_PARTITION_HISTOGRAM;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;

}; // TTxStorePartitionStats


void TSchemeShard::Handle(TEvDataShard::TEvGetTableStatsResult::TPtr& ev, const TActorContext& ctx) {
    const auto& rec = ev->Get()->Record;

    auto datashardId = TTabletId(rec.GetDatashardId());
    ui64 dataSize = rec.GetTableStats().GetDataSize();
    ui64 rowCount = rec.GetTableStats().GetRowCount();

    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
               "Got partition histogram at tablet " << TabletID()
               <<" from datashard " << datashardId
               << " state " << DatashardStateName(rec.GetShardState())
               << " data size " << dataSize
               << " row count " << rowCount
               << " buckets " << rec.GetTableStats().GetDataSizeHistogram().BucketsSize()
               << " ready " << rec.GetFullStatsReady()
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

THolder<TProposeRequest> SplitRequest(
    TSchemeShard* ss, TTxId& txId, const TPathId& pathId, TTabletId datashardId, const TString& keyBuff)
{
    auto request = MakeHolder<TProposeRequest>(ui64(txId), ui64(ss->SelfTabletId()));
    auto& record = request->Record;

    TPath tablePath = TPath::Init(pathId, ss);

    auto& propose = *record.AddTransaction();
    propose.SetFailOnExist(false);
    propose.SetOperationType(NKikimrSchemeOp::ESchemeOpSplitMergeTablePartitions);
    propose.SetInternal(true);

    propose.SetWorkingDir(tablePath.Parent().PathString());

    auto& split = *propose.MutableSplitMergeTablePartitions();
    split.SetTablePath(tablePath.PathString());
    split.SetSchemeshardId(ss->TabletID());

    split.AddSourceTabletId(ui64(datashardId));
    split.AddSplitBoundary()->SetSerializedKeyPrefix(keyBuff);

    return request;
}

bool TTxPartitionHistogram::Execute(TTransactionContext& txc, const TActorContext& ctx) {
    const auto& rec = Ev->Get()->Record;

    // NOTE: To make a decision how to split the partition (if needed),
    //       the EvGetTableStatsResult message should contain either
    //       the full table statistics (from the leader only, needed for
    //       the split-by-size) or the key access sample (either from the leader
    //       or any of the followers, needed for the split-by-load)
    bool trySplitBySize = (
        (rec.GetFollowerId() == 0) &&
        (rec.GetFullStatsReady()) &&
        (!(rec.GetTableStats().GetDataSizeHistogram().GetBuckets().empty()))
    );

    bool trySplitByLoad = !(rec.GetTableStats().GetKeyAccessSample().GetBuckets().empty());

    if (!trySplitBySize && !trySplitByLoad) {
        return true;
    }

    const TTabletId datashardId = TTabletId(rec.GetDatashardId());
    const TPathId tableId = (rec.HasTableOwnerId())
        ? TPathId(TOwnerId(rec.GetTableOwnerId()), TLocalPathId(rec.GetTableLocalId()))
        : Self->MakeLocalId(TLocalPathId(rec.GetTableLocalId()));

    // Save CPU resources when potential split will certainly be immediately rejected by Self->IgniteOperation()
    TString inflightLimitErrStr;
    if (!Self->CheckInFlightLimit(TTxState::ETxType::TxSplitTablePartition, inflightLimitErrStr)) {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxPartitionHistogram Do not process detailed partition statistics: " << inflightLimitErrStr
            << " at tablet " << Self->SelfTabletId()
            << " from datashard " << datashardId
            << " from follower ID " << rec.GetFollowerId()
            << " for pathId " << tableId
            << ", state " << DatashardStateName(rec.GetShardState())
            << ", data size buckets " << rec.GetTableStats().GetDataSizeHistogram().GetBuckets().size()
            << ", key access buckets " << rec.GetTableStats().GetKeyAccessSample().GetBuckets().size()
        );
        return true;
    }

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "TTxPartitionHistogram Process detailed partition statistics"
            << " at tablet " << Self->SelfTabletId()
            << " from datashard " << datashardId
            << " from follower ID " << rec.GetFollowerId()
            << " for pathId " << tableId
            << ", state " << DatashardStateName(rec.GetShardState())
            << ", data size buckets " << rec.GetTableStats().GetDataSizeHistogram().GetBuckets().size()
            << ", key access buckets " << rec.GetTableStats().GetKeyAccessSample().GetBuckets().size()
    );

    const TTableInfo::TPtr tableInfo = Self->Tables.Value(tableId, nullptr);

    if (!tableInfo) {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxPartitionHistogram Unknown table " << tableId << " tablet " << datashardId);
        return true;
    }

    const auto shardIt = Self->TabletIdToShardIdx.find(datashardId);

    if (shardIt == Self->TabletIdToShardIdx.end()) {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxPartitionHistogram Unknown tablet " << datashardId);
        return true;
    }

    const auto& shardIdx = shardIt->second;

    // Don't split/merge backup tables
    if (tableInfo->IsBackup) {
        LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxPartitionHistogram Skip backup table tablet " << datashardId);
        return true;
    }

    const auto path = TPath::Init(tableId, Self);

    if (path.IsLocked()) {
        LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxPartitionHistogram Skip locked table tablet " << datashardId << " by " << path.LockedBy());
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
                LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TTxPartitionHistogram Unknown shard index " << shardIdx
                        << " at tablet " << Self->SelfTabletId()
                        << " from datashard " << datashardId
                        << " for pathId " << tableId
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
                LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TTxPartitionHistogram Do not want to split tablet " << datashardId
                        << " by load, its table already has " << tableInfo->GetPartitions().size()
                        << " out of " << tableInfo->GetMaxPartitionsCount()
                        << " partitions"
                );

                return true;
            }
        }
    }

    if (splitReason == ESplitReason::NO_SPLIT) {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxPartitionHistogram Do not want to split tablet " << datashardId
            << ": " << splitReasonMsg);
        return true;
    }

    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "TTxPartitionHistogram Want to " << ToString(splitReason)
            << ": " << splitReasonMsg
            << " tablet " << datashardId
    );

    TTxId txId = Self->GetCachedTxId(ctx);

    if (!txId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxPartitionHistogram Do not request split: no cached tx ids for internal operation"
            << " " << ToString(splitReason) << ": " << splitReasonMsg
            << " tablet " << datashardId
            << " shardIdx " << shardIdx);
        return true;
    }

    const bool canUseSuggestedKey = AppData()->FeatureFlags.GetEnableSchemeShardSplitKeySelection();
    const auto& inputStats = rec.GetTableStats();
    TSerializedCellVec splitKey;

    if (splitReason == ESplitReason::SPLIT_BY_LOAD) {
        const bool shouldSortHistogram = AppData()->FeatureFlags.GetEnableSchemeShardSplitHistogramSorting();
        splitKey = GetSplitBoundaryByLoad(inputStats, GetKeyColumnTypes(*tableInfo), canUseSuggestedKey, shouldSortHistogram);

        if (splitKey.GetBuffer().empty()) {
            LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TTxPartitionHistogram Failed to find proper split key for"
                << " " << ToString(splitReason) << ": " << splitReasonMsg
                << " tablet " << datashardId);
            return true;
        }
    } else {  // ESplitReason::SPLIT_BY_SIZE
        splitKey = GetSplitBoundaryBySize(inputStats, GetKeyColumnTypes(*tableInfo), canUseSuggestedKey);

        if (splitKey.GetBuffer().empty()) {
            LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TTxPartitionHistogram Failed to find proper split key (initially) for"
                << " " << ToString(splitReason) << ": " << splitReasonMsg
                << " tablet " << datashardId);
            return true;
        }

        //XXX: Is this still necessary?
        // Split key must not be less than the first key
        TSerializedCellVec lowestKey(inputStats.GetDataSizeHistogram().GetBuckets(0).GetKey());
        if (0 < CompareTypedCellVectors(lowestKey.GetCells().data(), splitKey.GetCells().data(),
            GetKeyColumnTypes(*tableInfo).data(),
            lowestKey.GetCells().size(), splitKey.GetCells().size())
        ) {
            LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TTxPartitionHistogram Failed to find proper split key (less than first) for"
                << " " << ToString(splitReason) << ": " << splitReasonMsg
                << " tablet " << datashardId);
            return true;
        }
    }

    auto request = SplitRequest(Self, txId, tableId, datashardId, splitKey.GetBuffer());

    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "TTxPartitionHistogram Propose"
        << " " << ToString(splitReason) << ": " << splitReasonMsg
        << " tablet " << datashardId
        << " request " << request->Record.ShortDebugString());

    TMemoryChanges memChanges;
    TStorageChanges dbChanges;
    TOperationContext context{Self, txc, ctx, SplitOpSideEffects, memChanges, dbChanges};

    auto response = Self->IgniteOperation(*request, context);

    dbChanges.Apply(Self, txc, ctx);
    SplitOpSideEffects.ApplyOnExecute(Self, txc, ctx);

    return true;
}


void TTxPartitionHistogram::Complete(const TActorContext& ctx) {
    SplitOpSideEffects.ApplyOnComplete(Self, ctx);
}

}}
