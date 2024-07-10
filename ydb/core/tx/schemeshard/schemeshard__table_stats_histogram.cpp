#include "schemeshard_impl.h"
#include <ydb/core/base/appdata.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

namespace NKikimr {
namespace NSchemeShard {

static bool IsIntegerType(NScheme::TTypeInfo type) {
    // TODO: support pg types
    switch (type.GetTypeId()) {
    case NScheme::NTypeIds::Bool:

    case NScheme::NTypeIds::Int8:
    case NScheme::NTypeIds::Uint8:
    case NScheme::NTypeIds::Int16:
    case NScheme::NTypeIds::Uint16:
    case NScheme::NTypeIds::Int32:
    case NScheme::NTypeIds::Uint32:
    case NScheme::NTypeIds::Int64:
    case NScheme::NTypeIds::Uint64:

    case NScheme::NTypeIds::Date:
    case NScheme::NTypeIds::Datetime:
    case NScheme::NTypeIds::Timestamp:
    case NScheme::NTypeIds::Interval:
    case NScheme::NTypeIds::Date32:
    case NScheme::NTypeIds::Datetime64:
    case NScheme::NTypeIds::Timestamp64:
    case NScheme::NTypeIds::Interval64:
        return true;

    default:
        return false;
    }
}

TSerializedCellVec ChooseSplitKeyByHistogram(const NKikimrTableStats::THistogram& histogram, ui64 total, const TConstArrayRef<NScheme::TTypeInfo> &keyColumnTypes) {
    if (histogram.GetBuckets().empty()) {
        return {};
    }

    ui64 idxLo = Max<ui64>(), idxMed = Max<ui64>(), idxHi = Max<ui64>();
    { // search for median and acceptable bounds range so that after the split smallest size is >= 25%
        ui64 idxMedDiff = Max<ui64>(), idx = 0;
        for (const auto& point : histogram.GetBuckets()) {
            ui64 leftSize = Min(point.GetValue(), total);
            ui64 rightSize = total - leftSize;
            
            // search for a median point at which abs(leftSize - rightSize) is minimum
            ui64 sizesDiff = Max(leftSize, rightSize) - Min(leftSize, rightSize);
            if (idxMedDiff > sizesDiff) {
                idxMed = idx;
                idxMedDiff = sizesDiff;
            }

            if (leftSize * 4 >= total && idxLo == Max<ui64>()) {
                idxLo = idx; // first point at which leftSize >= 25%
            }
            if (rightSize * 4 >= total) {
                idxHi = idx; // last point at which rightSize >= 25%
            }

            idx++;
        }

        bool canSplit = idxLo != Max<ui64>() && idxLo <= idxMed && idxMed <= idxHi && idxHi != Max<ui64>();

        if (!canSplit) {
            return {};
        }
    }

    TSerializedCellVec keyLo(histogram.GetBuckets(idxLo).GetKey());
    TSerializedCellVec keyMed(histogram.GetBuckets(idxMed).GetKey());
    TSerializedCellVec keyHi(histogram.GetBuckets(idxHi).GetKey());

    TVector<TCell> splitKey(keyMed.GetCells().size());

    for (size_t i = 0; i < keyMed.GetCells().size(); ++i) {
        auto columnType = keyColumnTypes[i];

        if (0 == CompareTypedCells(keyLo.GetCells()[i], keyHi.GetCells()[i], columnType)) {
            // lo == hi, so we add this value and proceed to the next column
            splitKey[i] = keyLo.GetCells()[i];
            continue;
        }

        if (0 != CompareTypedCells(keyLo.GetCells()[i], keyMed.GetCells()[i], columnType)) {
            // med != lo
            splitKey[i] = keyMed.GetCells()[i];
        } else {
            // med == lo and med != hi, so we want to find a value that is > med and <= hi
            if (IsIntegerType(columnType) && !keyMed.GetCells()[i].IsNull()) {
                // For integer types we can add 1 to med
                ui64 val = 0;
                size_t sz =  keyMed.GetCells()[i].Size();
                memcpy(&val, keyMed.GetCells()[i].Data(), sz);
                val++;
                splitKey[i] = TCell((const char*)&val, sz);
            } else {
                // For other types let's do binary search between med and hi to find smallest key > med

                // Compares only i-th cell in keys
                auto fnCmpCurrentCell = [i, columnType] (const auto& keyMed, const auto& bucket) {
                    TSerializedCellVec bucketCells(bucket.GetKey());
                    return CompareTypedCells(keyMed.GetCells()[i], bucketCells.GetCells()[i], columnType) < 0;
                };

                const auto bucketsBegin = histogram.GetBuckets().begin();
                const auto it = UpperBound(
                            bucketsBegin + idxMed,
                            bucketsBegin + idxHi,
                            keyMed,
                            fnCmpCurrentCell);
                TSerializedCellVec keyFound(it->GetKey());
                splitKey[i] = keyFound.GetCells()[i];
            }
        }
        break;
    }

    return TSerializedCellVec(splitKey);
}

TSerializedCellVec DoFindSplitKey(const TVector<std::pair<TSerializedCellVec, ui64>>& keysHist,
                                  const TConstArrayRef<NScheme::TTypeInfo>& keyColumnTypes,
                                  const size_t prefixSize)
{
    ui64 total = keysHist.back().second;

    // Compares bucket value
    auto fnValueLess = [] (ui64 val, const auto& bucket) {
        return val < bucket.second;
    };

    // Find the position of total/2
    auto halfIt = std::upper_bound(keysHist.begin(), keysHist.end(), total*0.5, fnValueLess);
    auto loIt = std::upper_bound(keysHist.begin(), keysHist.end(), total*0.1, fnValueLess);
    auto hiIt = std::upper_bound(keysHist.begin(), keysHist.end(), total*0.9, fnValueLess);

    auto fnCmp = [&keyColumnTypes, prefixSize] (const auto& bucket1, const auto& bucket2) {
        return CompareTypedCellVectors(bucket1.first.GetCells().data(), bucket2.first.GetCells().data(),
                                       keyColumnTypes.data(),
                                       std::min(bucket1.first.GetCells().size(), prefixSize), std::min(bucket2.first.GetCells().size(), prefixSize));
    };

    // Check if half key is no equal to low and high keys
    if (fnCmp(*halfIt, *loIt) == 0)
        return TSerializedCellVec();
    if (fnCmp(*halfIt, *hiIt) == 0)
        return TSerializedCellVec();

    // Build split key by leaving the prefix and extending it with NULLs
    TVector<TCell> splitKey(halfIt->first.GetCells().begin(), halfIt->first.GetCells().end());
    splitKey.resize(prefixSize);
    splitKey.resize(keyColumnTypes.size());


    return TSerializedCellVec(splitKey);
}

TSerializedCellVec ChooseSplitKeyByKeySample(const NKikimrTableStats::THistogram& keySample, const TConstArrayRef<NScheme::TTypeInfo>& keyColumnTypes) {
    TVector<std::pair<TSerializedCellVec, ui64>> keysHist;
    const auto & buckets = keySample.GetBuckets();
    keysHist.reserve(buckets.size());

    for (const auto& bucket : buckets) {
        keysHist.emplace_back(std::make_pair(TSerializedCellVec(bucket.GetKey()), bucket.GetValue()));
    }

    auto fnCmp = [&keyColumnTypes] (const auto& key1, const auto& key2) {
        return CompareTypedCellVectors(key1.first.GetCells().data(), key2.first.GetCells().data(),
                                       keyColumnTypes.data(),
                                       key1.first.GetCells().size(), key2.first.GetCells().size());
    };

    Sort(keysHist, [&fnCmp] (const auto& key1, const auto& key2) { return fnCmp(key1, key2) < 0; });

    // The keys are now sorted. Next we convert the stats into a histogram by accumulating
    // stats for all previous keys at each key.
    size_t last = 0;
    for (size_t i = 1; i < keysHist.size(); ++i) {
        // Accumulate stats
        keysHist[i].second += keysHist[i-1].second;

        if (fnCmp(keysHist[i], keysHist[last]) == 0) {
            // Merge equal keys
            keysHist[last].second = keysHist[i].second;
        } else {
            ++last;
            if (last != i) {
                keysHist[last] = keysHist[i];
            }
        }
    }
    keysHist.resize(std::min(keysHist.size(), last + 1));

    if (keysHist.size() < 2)
        return TSerializedCellVec();

    // Find the median key with the shortest prefix
    size_t minPrefix = 0;
    size_t maxPrefix = keyColumnTypes.size();

    // Binary search for shortest prefix that can be used to split the load
    TSerializedCellVec splitKey;
    while (minPrefix + 1 < maxPrefix) {
        size_t prefixSize = (minPrefix + maxPrefix + 1) / 2;
        splitKey = DoFindSplitKey(keysHist, keyColumnTypes, prefixSize);
        if (splitKey.GetCells().empty()) {
            minPrefix = prefixSize;
        } else {
            maxPrefix = prefixSize;
        }
    }
    splitKey = DoFindSplitKey(keysHist, keyColumnTypes, maxPrefix);

    return splitKey;
}

enum struct ESplitReason {
    NO_SPLIT = 0,
    FAST_SPLIT_INDEX,
    SPLIT_BY_SIZE,
    SPLIT_BY_LOAD
};

const char* ToString(ESplitReason splitReason) {
    switch (splitReason) {
    case ESplitReason::NO_SPLIT:
        return "No split";
    case ESplitReason::FAST_SPLIT_INDEX:
        return "Fast split index table";
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

    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
               "Got partition histogram at tablet " << TabletID()
               <<" from datashard " << datashardId
               << " state: '" << DatashardStateName(rec.GetShardState()) << "'"
               << " data size: " << dataSize
               << " row count: " << rowCount
    );

    Execute(new TTxPartitionHistogram(this, ev), ctx);
}

THolder<TProposeRequest> SplitRequest(
    TSchemeShard* ss, TTxId& txId, TPathId& pathId, TTabletId datashardId, const TString& keyBuff)
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

    if (!rec.GetFullStatsReady())
        return true;

    auto datashardId = TTabletId(rec.GetDatashardId());
    TPathId tableId = InvalidPathId;
    if (rec.HasTableOwnerId()) {
        tableId = TPathId(TOwnerId(rec.GetTableOwnerId()),
                          TLocalPathId(rec.GetTableLocalId()));
    } else {
        tableId = Self->MakeLocalId(TLocalPathId(rec.GetTableLocalId()));
    }
    ui64 dataSize = rec.GetTableStats().GetDataSize();
    ui64 rowCount = rec.GetTableStats().GetRowCount();

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TTxPartitionHistogram::Execute partition histogram"
                    << " at tablet " << Self->SelfTabletId()
                    << " from datashard " << datashardId
                    << " for pathId " << tableId
                    << " state '" << DatashardStateName(rec.GetShardState()).data() << "'"
                    << " dataSize " << dataSize
                    << " rowCount " << rowCount
                    << " dataSizeHistogram buckets " << rec.GetTableStats().GetDataSizeHistogram().BucketsSize());

    if (!Self->Tables.contains(tableId))
        return true;

    TTableInfo::TPtr table = Self->Tables[tableId];

    if (!Self->TabletIdToShardIdx.contains(datashardId))
        return true;

    // Don't split/merge backup tables
    if (table->IsBackup)
        return true;

    auto shardIdx = Self->TabletIdToShardIdx[datashardId];
    const auto forceShardSplitSettings = Self->SplitSettings.GetForceShardSplitSettings();

    const TTableInfo* mainTableForIndex = Self->GetMainTableForIndex(tableId);

    ESplitReason splitReason = ESplitReason::NO_SPLIT;
    if (table->ShouldSplitBySize(dataSize, forceShardSplitSettings)) {
        splitReason = ESplitReason::SPLIT_BY_SIZE;
    }

    if (splitReason == ESplitReason::NO_SPLIT && table->CheckSplitByLoad(Self->SplitSettings, shardIdx, dataSize, rowCount, mainTableForIndex)) {
        splitReason = ESplitReason::SPLIT_BY_LOAD;
    }

    if (splitReason == ESplitReason::NO_SPLIT) {
        return true;
    }

    if (splitReason != ESplitReason::SPLIT_BY_SIZE && table->GetPartitions().size() >= table->GetMaxPartitionsCount()) {
        return true;
    }

    TSmallVec<NScheme::TTypeInfo> keyColumnTypes(table->KeyColumnIds.size());
    for (size_t ki = 0; ki < table->KeyColumnIds.size(); ++ki) {
        keyColumnTypes[ki] = table->Columns.FindPtr(table->KeyColumnIds[ki])->PType;
    }

    TSerializedCellVec splitKey;
    if (splitReason == ESplitReason::SPLIT_BY_LOAD) {
        // TODO: choose split key based on access stats for split by load
        const auto& keySample = rec.GetTableStats().GetKeyAccessSample();
        splitKey = ChooseSplitKeyByKeySample(keySample, keyColumnTypes);

        // TODO: check that the choosen key is valid
    } else {
        // Choose number of parts and split boundaries
        const auto& histogram = rec.GetTableStats().GetDataSizeHistogram();

        splitKey = ChooseSplitKeyByHistogram(histogram, dataSize, keyColumnTypes);
        if (splitKey.GetBuffer().empty()) {
            LOG_WARN(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Failed to find proper split key (initially) for '%s' of datashard %" PRIu64,
                ToString(splitReason), datashardId);
            return true;
        }

        // Split key must not be less than the first key
        TSerializedCellVec lowestKey(histogram.GetBuckets(0).GetKey());
        if (0 < CompareTypedCellVectors(lowestKey.GetCells().data(), splitKey.GetCells().data(),
                                    keyColumnTypes.data(),
                                    lowestKey.GetCells().size(), splitKey.GetCells().size()))
        {
            LOG_WARN(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "Failed to find proper split key (less than first) for '%s' of datashard %" PRIu64,
                      ToString(splitReason), datashardId);
            return true;
        }
    }

    if (splitKey.GetBuffer().empty()) {
        LOG_WARN(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                 "Failed to find proper split key for '%s' of datashard %" PRIu64,
                  ToString(splitReason), datashardId);
        return true;
    }

    TTxId txId = Self->GetCachedTxId(ctx);

    if (!txId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Do not request split op"
                   << ", reason: no cached tx ids for internal operation"
                   << ", shardIdx: " << shardIdx);
        return true;
    }

    auto request = SplitRequest(Self, txId, tableId, datashardId, splitKey.GetBuffer());

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
