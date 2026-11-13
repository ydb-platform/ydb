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

const TString& GetBlobStorageId() {
    return NOlap::NBlobOperations::TGlobal::DefaultStorageId;
}

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
    const ui64 smallBlobBytes = portion->GetSmallBlobBytesInBlobStorage(GetSmallBlobThreshold(GetBlobStorageId()));
    Granules[portion->GetPathId()].AddPortion(*portion, smallBlobBytes);
    Total.AddPortion(*portion, smallBlobBytes);

    SchemaVersionsInUse[portion->GetSchemaVersionVerified()].Inc();
    PortionsMetadataRamBytes += portion->GetMetadataMemorySize();
    Counters.OnIndexMetadataUsageBytes(AllShardsPortionsMetadataRamBytes.Add(portion->GetMetadataMemorySize()));
    PortionSizeHistograms.OnNewPortion(portion);
}

void TShardDataStats::RemovePortion(const std::shared_ptr<NOlap::TPortionInfo>& portion) {
    auto granule = Granules.find(portion->GetPathId());
    AFL_VERIFY(granule != Granules.end())("path_id", portion->GetPathId());
    const ui64 smallBlobBytes = portion->GetSmallBlobBytesInBlobStorage(GetSmallBlobThreshold(GetBlobStorageId()));
    granule->second.RemovePortion(*portion, smallBlobBytes);
    if (granule->second.IsEmpty()) {
        Granules.erase(granule);
    }
    Total.RemovePortion(*portion, smallBlobBytes);

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

ui64 TShardDataStats::GetSmallBlobThreshold(const TString& storageId) {
    auto it = SmallBlobThresholds.find(storageId);
    if (it == SmallBlobThresholds.end()) {
        it = SmallBlobThresholds.emplace(storageId, StoragesManager->GetOperatorVerified(storageId)->GetSmallBlobThresholdBytes()).first;
    }
    return it->second;
}

const TDataStatsByStorage& TShardDataStats::GetGranuleStats(const TInternalPathId pathId) const {
    static const TDataStatsByStorage empty;
    auto it = Granules.find(pathId);
    return it == Granules.end() ? empty : it->second;
}

bool TShardDataStats::HasSchemaVersion(const ui64 fromVersion, const ui64 version) const {
    AFL_VERIFY(fromVersion <= version);
    auto it = SchemaVersionsInUse.lower_bound(fromVersion);
    return it != SchemaVersionsInUse.end() && it->first <= version;
}

void TShardDataStats::FillTableStats(const TInternalPathId pathId, NKikimrTableStats::TTableStats& tableStats) const {
    const TDataStatsByStorage& granule = GetGranuleStats(pathId);
    FillPortionStats(tableStats, granule.GetAll(GetBlobStorageId()), granule.GetSmallBlobs(GetBlobStorageId()));
}

void TShardDataStats::FillTotalTableStats(NKikimrTableStats::TTableStats& tableStats) {
    const auto& blobStorage = GetStorageStats(GetBlobStorageId());
    FillPortionStats(tableStats, blobStorage.GetAll(), blobStorage.GetSmallBlobs());

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

    const auto& blobStorageStats = GetStorageStats(GetBlobStorageId());
    const NOlap::TSimplePortionsGroupInfo blobStorage = blobStorageStats.GetAll();
    counters.SetCounter(COUNTER_DATA_BYTES, blobStorage.GetDataBlobBytes());
    counters.SetCounter(COUNTER_INDEX_BYTES, blobStorage.GetIndexBlobBytes());

    const NOlap::TSimplePortionsGroupInfo written = blobStorageStats.GetWritten();
    const NOlap::TSimplePortionsGroupInfo compacted = blobStorageStats.GetCompacted();
    const NOlap::TSimplePortionsGroupInfo removed = Total.GetRemoved();
    NOlap::TSimplePortionsGroupInfo evicted;
    for (const auto& [storageId, storage] : Total.GetStorages()) {
        if (storageId != GetBlobStorageId()) {
            evicted += storage.GetWritten() + storage.GetCompacted();
        }
    }
    SetPortionCounters(counters, written, COUNTER_INSERTED_PORTIONS, COUNTER_INSERTED_ROWS, COUNTER_INSERTED_BYTES, COUNTER_INSERTED_RAW_BYTES);
    // COMPACTED is never produced
    SetPortionCounters(counters, {}, COUNTER_COMPACTED_PORTIONS, COUNTER_COMPACTED_ROWS, COUNTER_COMPACTED_BYTES, COUNTER_COMPACTED_RAW_BYTES);
    SetPortionCounters(counters, compacted, COUNTER_SPLIT_COMPACTED_PORTIONS, COUNTER_SPLIT_COMPACTED_ROWS, COUNTER_SPLIT_COMPACTED_BYTES,
        COUNTER_SPLIT_COMPACTED_RAW_BYTES);
    SetPortionCounters(counters, removed, COUNTER_INACTIVE_PORTIONS, COUNTER_INACTIVE_ROWS, COUNTER_INACTIVE_BYTES, COUNTER_INACTIVE_RAW_BYTES);
    SetPortionCounters(counters, evicted, COUNTER_EVICTED_PORTIONS, COUNTER_EVICTED_ROWS, COUNTER_EVICTED_BYTES, COUNTER_EVICTED_RAW_BYTES);

    YDB_LOG_DEBUG_COMP(NKikimrServices::TX_COLUMNSHARD, "Index",
        {"tablesCount", Granules.size()},
        {"insertedStats", written.DebugString()},
        {"splitCompactedStats", compacted.DebugString()},
        {"inactiveStats", removed.DebugString()},
        {"evictedStats", evicted.DebugString()},
        {"tabletId", TabletId});
}

ui64 TShardDataStats::GetBlobStorageBytes() const {
    return GetStorageStats(GetBlobStorageId()).GetAll().GetBlobBytes();
}

}   // namespace NKikimr::NColumnShard
