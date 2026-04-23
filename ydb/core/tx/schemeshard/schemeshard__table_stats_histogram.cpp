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

    const auto getSplitBoundary = (splitReason == ESplitReason::SPLIT_BY_LOAD ? GetSplitBoundaryByLoad : GetSplitBoundaryBySize);

    TSerializedCellVec splitKey = getSplitBoundary(rec.GetTableStats(), GetKeyColumnTypes(*tableInfo));

    if (splitKey.GetBuffer().empty()) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxPartitionHistogram Failed to find proper split key for"
            << " " << ToString(splitReason) << ": " << splitReasonMsg
            << " tablet " << datashardId);
        Self->ReturnTxIdToCache(txId);
        return true;
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
