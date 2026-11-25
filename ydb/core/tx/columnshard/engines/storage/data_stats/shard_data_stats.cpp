#include "shard_data_stats.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/table_stats.pb.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/counters/tablet_counters.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NColumnShard {

namespace {

void FillPortionStats(
    NKikimrTableStats::TTableStats& to, const NOlap::TSimplePortionsGroupInfo& from, const NOlap::TSmallBlobsStat& smallBlobs) {
    to.SetRowCount(from.GetRecordsCount());
    to.SetDataSize(from.GetBlobBytes());
    to.SetSmallBlobsVolumeBytes(smallBlobs.VolumeBytes);
    to.SetSmallBlobsCount(smallBlobs.Count);
}

void SetPortionCounters(const TTabletCountersHandle& counters, const NOlap::TSimplePortionsGroupInfo& portions, const ESimpleCounters count,
    const ESimpleCounters rows, const ESimpleCounters bytes, const ESimpleCounters rawBytes) {
    counters.SetCounter(count, portions.GetCount());
    counters.SetCounter(rows, portions.GetRecordsCount());
    counters.SetCounter(bytes, portions.GetBlobBytes());
    counters.SetCounter(rawBytes, portions.GetRawBytes());
}

}   // namespace

TShardDataStats::TShardDataStats(const std::shared_ptr<NOlap::IStoragesManager>& storagesManager, const ui64 tabletId)
    : StoragesManager(storagesManager)
    , TabletId(tabletId)
    , PortionSizeHistograms(Counters.BuildPortionSizeHistograms())
{
    AFL_VERIFY(StoragesManager);
}

TShardDataStats::~TShardDataStats() {
    AllShardsPortionsMetadataRamBytes.Sub(PortionsMetadataRamBytes);
}

void TShardDataStats::AddPortion(const std::shared_ptr<NOlap::TPortionInfo>& portion) {
    Granules[portion->GetPathId()].AddPortion(*portion, SmallBlobThresholdBytes);
    Total.AddPortion(*portion, SmallBlobThresholdBytes);

    SchemaVersionsInUse[portion->GetSchemaVersionVerified()].Inc();
    PortionsMetadataRamBytes += portion->GetMetadataMemorySize();
    Counters.OnIndexMetadataUsageBytes(AllShardsPortionsMetadataRamBytes.Add(portion->GetMetadataMemorySize()));
    PortionSizeHistograms.OnNewPortion(portion);
}

void TShardDataStats::RemovePortion(const std::shared_ptr<NOlap::TPortionInfo>& portion) {
    auto granule = Granules.find(portion->GetPathId());
    AFL_VERIFY(granule != Granules.end())("path_id", portion->GetPathId());
    granule->second.RemovePortion(*portion, SmallBlobThresholdBytes);
    if (granule->second.IsEmpty()) {
        Granules.erase(granule);
    }
    Total.RemovePortion(*portion, SmallBlobThresholdBytes);

    auto schemaVersion = SchemaVersionsInUse.find(portion->GetSchemaVersionVerified());
    AFL_VERIFY(schemaVersion != SchemaVersionsInUse.end());
    if (schemaVersion->second.Dec() == 0) {
        SchemaVersionsInUse.erase(schemaVersion);
    }
    PortionsMetadataRamBytes -= portion->GetMetadataMemorySize();
    AFL_VERIFY(PortionsMetadataRamBytes >= 0);
    Counters.OnIndexMetadataUsageBytes(AllShardsPortionsMetadataRamBytes.Sub(portion->GetMetadataMemorySize()));
    PortionSizeHistograms.OnDropPortion(portion);
}

const TGranuleDataStats& TShardDataStats::GetGranule(const TInternalPathId pathId) const {
    static const TGranuleDataStats empty;
    auto it = Granules.find(pathId);
    return it == Granules.end() ? empty : it->second;
}

bool TShardDataStats::HasSchemaVersion(const ui64 fromVersion, const ui64 version) const {
    AFL_VERIFY(fromVersion <= version);
    auto it = SchemaVersionsInUse.lower_bound(fromVersion);
    return it != SchemaVersionsInUse.end() && it->first <= version;
}

void TShardDataStats::FillTableStats(const TInternalPathId pathId, NKikimrTableStats::TTableStats& tableStats) const {
    const TGranuleDataStats& granule = GetGranule(pathId);
    FillPortionStats(tableStats, granule.GetDefaultStorage(), granule.GetSmallBlobs());
}

void TShardDataStats::FillTotalTableStats(NKikimrTableStats::TTableStats& tableStats) {
    FillPortionStats(tableStats, Total.GetDefaultStorage(), Total.GetSmallBlobs());

    NOlap::TSmallBlobsStat toDelete;
    for (auto&& i : StoragesManager->GetStorages()) {
        const auto stat = i.second->CalcSmallBlobsToDelete();
        toDelete.VolumeBytes += stat.VolumeBytes;
        toDelete.Count += stat.Count;
    }
    tableStats.SetSmallBlobsVolumeBytes(tableStats.GetSmallBlobsVolumeBytes() + toDelete.VolumeBytes);
    tableStats.SetSmallBlobsCount(NormalizeSmallBlobsCount(tableStats.GetSmallBlobsCount() + toDelete.Count));
}

const std::optional<TBlobStorageGroupType>& TShardDataStats::GetBlobStorageLayout() {
    if (!BlobStorageLayout) {
        BlobStorageLayout = StoragesManager->GetDefaultOperator()->GetBlobStorageLayout();
    }
    return BlobStorageLayout;
}

ui64 TShardDataStats::NormalizeSmallBlobsCount(const ui64 rawCount) {
    const auto& layout = GetBlobStorageLayout();
    if (!layout) {
        return rawCount;
    }
    const auto& quota = AppDataVerified().SmallBlobsQuotaConfig;
    ui32 coef = 1;
    switch (layout->GetErasure()) {
        case NKikimr::TErasureType::ErasureMirror3dc:
            coef = quota.GetMirror3dcStorageHardQuotaCoef();
            break;
        case NKikimr::TErasureType::Erasure4Plus2Block:
            coef = quota.GetBlock42StorageHardQuotaCoef();
            break;
        default:
            break;
    }
    coef = std::max<ui32>(1, coef);
    const ui32 vdisks = std::max<ui32>(1, layout->BlobSubgroupSize());
    // 1. Every vdisk in a group keeps a record for a blob in the index in RAM, so we multiply by the number of vdisks.
    // 2. Hard storage quota is set in term of "user data", not "actual blob storage consumption".
    // When the quota is set, it is usually "actual blobs storage allowance" / some coefficient (depending on the erasure).
    // So, here we take that coefficient into account too.
    return rawCount * vdisks / coef;
}

void TShardDataStats::UpdateTabletCounters(const TTabletCountersHandle& counters) const {
    counters.SetCounter(COUNTER_INDEX_TABLES, Granules.size());

    const NOlap::TSimplePortionsGroupInfo defaultStorage = Total.GetDefaultStorage();
    counters.SetCounter(COUNTER_DATA_BYTES, defaultStorage.GetDataBlobBytes());
    counters.SetCounter(COUNTER_INDEX_BYTES, defaultStorage.GetIndexBlobBytes());

    const NOlap::TSimplePortionsGroupInfo removed = Total.GetRemoved();
    const NOlap::TSimplePortionsGroupInfo evicted = Total.GetEvicted();
    SetPortionCounters(
        counters, Total.GetWritten(), COUNTER_INSERTED_PORTIONS, COUNTER_INSERTED_ROWS, COUNTER_INSERTED_BYTES, COUNTER_INSERTED_RAW_BYTES);
    // COMPACTED is never produced
    SetPortionCounters(counters, {}, COUNTER_COMPACTED_PORTIONS, COUNTER_COMPACTED_ROWS, COUNTER_COMPACTED_BYTES, COUNTER_COMPACTED_RAW_BYTES);
    SetPortionCounters(counters, Total.GetCompacted(), COUNTER_SPLIT_COMPACTED_PORTIONS, COUNTER_SPLIT_COMPACTED_ROWS,
        COUNTER_SPLIT_COMPACTED_BYTES, COUNTER_SPLIT_COMPACTED_RAW_BYTES);
    SetPortionCounters(counters, removed, COUNTER_INACTIVE_PORTIONS, COUNTER_INACTIVE_ROWS, COUNTER_INACTIVE_BYTES, COUNTER_INACTIVE_RAW_BYTES);
    SetPortionCounters(counters, evicted, COUNTER_EVICTED_PORTIONS, COUNTER_EVICTED_ROWS, COUNTER_EVICTED_BYTES, COUNTER_EVICTED_RAW_BYTES);

    YDB_LOG_DEBUG_COMP(NKikimrServices::TX_COLUMNSHARD, "Index",
        {"tablesCount", Granules.size()},
        {"insertedStats", Total.GetWritten().DebugString()},
        {"splitCompactedStats", Total.GetCompacted().DebugString()},
        {"inactiveStats", removed.DebugString()},
        {"evictedStats", evicted.DebugString()},
        {"tabletId", TabletId});
}

ui64 TShardDataStats::GetStorageBytes() const {
    return (Total.GetWritten() + Total.GetCompacted() + Total.GetRemoved()).GetBlobBytes();
}

}   // namespace NKikimr::NColumnShard
