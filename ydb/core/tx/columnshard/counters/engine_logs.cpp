#include "engine_logs.h"
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/library/actors/core/log.h>

#include <util/generic/serialized_enum.h>

namespace NKikimr::NColumnShard {

TEngineLogsCounters::TEngineLogsCounters()
    : TBase("EngineLogs")
    , GranuleDataAgent("EngineLogs")
{
    const std::map<i64, TString> borders = {{0, "0"}, {512 * 1024, "512kb"}, {1024 * 1024, "1Mb"},
        {2 * 1024 * 1024, "2Mb"}, {4 * 1024 * 1024, "4Mb"},
        {5 * 1024 * 1024, "5Mb"}, {6 * 1024 * 1024, "6Mb"},
        {7 * 1024 * 1024, "7Mb"}, {8 * 1024 * 1024, "8Mb"}};
    const std::map<i64, TString> portionSizeBorders = {{0, "0"}, {512 * 1024, "512kb"}, {1024 * 1024, "1Mb"},
        {2 * 1024 * 1024, "2Mb"}, {4 * 1024 * 1024, "4Mb"},
        {8 * 1024 * 1024, "8Mb"}, {16 * 1024 * 1024, "16Mb"},
        {32 * 1024 * 1024, "32Mb"}, {64 * 1024 * 1024, "64Mb"}, {128 * 1024 * 1024, "128Mb"}, {256 * 1024 * 1024, "256Mb"}, {512 * 1024 * 1024, "512Mb"}};
    const std::set<i64> portionRecordBorders = {0, 2500, 5000, 7500, 9000, 10000, 20000, 40000, 80000, 160000, 320000, 640000, 1024000};
    for (auto&& i : GetEnumNames<NOlap::NPortion::EProduced>()) {
        if (BlobSizeDistribution.size() <= (ui32)i.first) {
            BlobSizeDistribution.resize((ui32)i.first + 1);
            PortionSizeDistribution.resize((ui32)i.first + 1);
            PortionRecordsDistribution.resize((ui32)i.first + 1);
        }
        if (PortionSizeDistribution.size() <= (ui32)i.first) {
            PortionSizeDistribution.resize((ui32)i.first + 1);
        }
        BlobSizeDistribution[(ui32)i.first] = std::make_shared<TIncrementalHistogram>("EngineLogs", "BlobSizeDistribution", i.second, borders);
        PortionSizeDistribution[(ui32)i.first] = std::make_shared<TIncrementalHistogram>("EngineLogs", "PortionSizeDistribution", i.second, portionSizeBorders);
        PortionRecordsDistribution[(ui32)i.first] = std::make_shared<TIncrementalHistogram>("EngineLogs", "PortionRecordsDistribution", i.second, portionRecordBorders);
    }
    for (auto&& i : BlobSizeDistribution) {
        Y_ABORT_UNLESS(i);
    }

    PortionToDropCount = TBase::GetDeriviative("Ttl/PortionToDrop/Count");
    PortionToDropBytes = TBase::GetDeriviative("Ttl/PortionToDrop/Bytes");
    PortionToDropLag = TBase::GetHistogram("Ttl/PortionToDrop/Lag/Duration", NMonitoring::ExponentialHistogram(18, 2, 5));
    SkipDeleteWithProcessMemory = TBase::GetHistogram("Ttl/PortionToDrop/Skip/ProcessMemory/Lag/Duration", NMonitoring::ExponentialHistogram(18, 2, 5));
    SkipDeleteWithTxLimit = TBase::GetHistogram("Ttl/PortionToDrop/Skip/TxLimit/Lag/Duration", NMonitoring::ExponentialHistogram(18, 2, 5));

    PortionToEvictCount = TBase::GetDeriviative("Ttl/PortionToEvict/Count");
    PortionToEvictBytes = TBase::GetDeriviative("Ttl/PortionToEvict/Bytes");
    PortionToEvictLag = TBase::GetHistogram("Ttl/PortionToEvict/Lag/Duration", NMonitoring::ExponentialHistogram(18, 2, 5));
    SkipEvictionWithProcessMemory = TBase::GetHistogram("Ttl/PortionToEvict/Skip/ProcessMemory/Lag/Duration", NMonitoring::ExponentialHistogram(18, 2, 5));
    SkipEvictionWithTxLimit = TBase::GetHistogram("Ttl/PortionToEvict/Skip/TxLimit/Lag/Duration", NMonitoring::ExponentialHistogram(18, 2, 5));

    ActualizationTaskSizeRemove = TBase::GetHistogram("Actualization/RemoveTasks/Size", NMonitoring::ExponentialHistogram(18, 2));
    ActualizationTaskSizeEvict = TBase::GetHistogram("Actualization/EvictTasks/Size", NMonitoring::ExponentialHistogram(18, 2));

    ActualizationSkipRWProgressCount = TBase::GetDeriviative("Actualization/Skip/RWProgress/Count");
    ActualizationSkipTooFreshPortion = TBase::GetHistogram("Actualization//Skip/TooFresh/Duration", NMonitoring::LinearHistogram(12, 0, 360));

    PortionNoTtlColumnCount = TBase::GetDeriviative("Ttl/PortionNoTtlColumn/Count");
    PortionNoTtlColumnBytes = TBase::GetDeriviative("Ttl/PortionNoTtlColumn/Bytes");

    PortionNoBorderCount = TBase::GetDeriviative("Ttl/PortionNoBorder/Count");
    PortionNoBorderBytes = TBase::GetDeriviative("Ttl/PortionNoBorder/Bytes");

    GranuleOptimizerLocked = TBase::GetDeriviative("Optimizer/Granules/Locked");

    IndexMetadataUsageBytes = TBase::GetValue("IndexMetadata/Usage/Bytes");

    StatUsageForTTLCount = TBase::GetDeriviative("Ttl/StatUsageForTTLCount/Count");
    ChunkUsageForTTLCount = TBase::GetDeriviative("Ttl/ChunkUsageForTTLCount/Count");
}

void TEngineLogsCounters::OnActualizationTask(const ui32 evictCount, const ui32 removeCount) const {
    AFL_VERIFY(evictCount * removeCount == 0)("evict", evictCount)("remove", removeCount);
    AFL_VERIFY(evictCount + removeCount);
    if (evictCount) {
        ActualizationTaskSizeEvict->Collect(evictCount);
    } else {
        ActualizationTaskSizeRemove->Collect(removeCount);
    }
}

void TEngineLogsCounters::TPortionsInfoGuard::OnNewPortion(const std::shared_ptr<NOlap::TPortionInfo>& portion) const {
    const ui32 producedId = (ui32)(portion->HasRemoveSnapshot() ? NOlap::NPortion::EProduced::INACTIVE : portion->GetMeta().Produced);
    Y_ABORT_UNLESS(producedId < BlobGuards.size());
    THashSet<NOlap::TUnifiedBlobId> blobIds;
    for (auto&& i : portion->GetRecords()) {
        const auto blobId = portion->GetBlobId(i.GetBlobRange().GetBlobIdxVerified());
        if (blobIds.emplace(blobId).second) {
            BlobGuards[producedId]->Add(blobId.BlobSize(), blobId.BlobSize());
        }
    }
    for (auto&& i : portion->GetIndexes()) {
        if (i.HasBlobRange()) {
            const auto blobId = portion->GetBlobId(i.GetBlobRangeVerified().GetBlobIdxVerified());
            if (blobIds.emplace(blobId).second) {
                BlobGuards[producedId]->Add(blobId.BlobSize(), blobId.BlobSize());
            }
        }
    }
    PortionRecordCountGuards[producedId]->Add(portion->GetRecordsCount(), 1);
    PortionSizeGuards[producedId]->Add(portion->GetTotalBlobBytes(), 1);
}

void TEngineLogsCounters::TPortionsInfoGuard::OnDropPortion(const std::shared_ptr<NOlap::TPortionInfo>& portion) const {
    const ui32 producedId = (ui32)(portion->HasRemoveSnapshot() ? NOlap::NPortion::EProduced::INACTIVE : portion->GetMeta().Produced);
    Y_ABORT_UNLESS(producedId < BlobGuards.size());
    THashSet<NOlap::TUnifiedBlobId> blobIds;
    for (auto&& i : portion->GetRecords()) {
        const auto blobId = portion->GetBlobId(i.GetBlobRange().GetBlobIdxVerified());
        if (blobIds.emplace(blobId).second) {
            BlobGuards[producedId]->Sub(blobId.BlobSize(), blobId.BlobSize());
        }
    }
    for (auto&& i : portion->GetIndexes()) {
        if (i.HasBlobRange()) {
            const auto blobId = portion->GetBlobId(i.GetBlobRangeVerified().GetBlobIdxVerified());
            if (blobIds.emplace(blobId).second) {
                BlobGuards[producedId]->Sub(blobId.BlobSize(), blobId.BlobSize());
            }
        }
    }
    PortionRecordCountGuards[producedId]->Sub(portion->GetRecordsCount(), 1);
    PortionSizeGuards[producedId]->Sub(portion->GetTotalBlobBytes(), 1);
}

NKikimr::NColumnShard::TBaseGranuleDataClassSummary TBaseGranuleDataClassSummary::operator+(const TBaseGranuleDataClassSummary& item) const {
    TBaseGranuleDataClassSummary result;
    result.TotalPortionsSize = TotalPortionsSize + item.TotalPortionsSize;
    result.ColumnPortionsSize = ColumnPortionsSize + item.ColumnPortionsSize;
    AFL_VERIFY(result.TotalPortionsSize >= 0);
    AFL_VERIFY(result.ColumnPortionsSize >= 0);
    result.PortionsCount = PortionsCount + item.PortionsCount;
    result.RecordsCount = RecordsCount + item.RecordsCount;
    result.MetadataMemoryPortionsSize = MetadataMemoryPortionsSize + item.MetadataMemoryPortionsSize;
    return result;
}

}
