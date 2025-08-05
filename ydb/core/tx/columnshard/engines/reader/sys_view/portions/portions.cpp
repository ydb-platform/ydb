#include "portions.h"

#include <ydb/core/formats/arrow/switch/switch_type.h>
#include <ydb/core/tx/columnshard/blobs_action/common/const.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_context.h>

namespace NKikimr::NOlap::NReader::NSysView::NPortions {

void TStatsIterator::AppendStats(const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders, const TPortionInfo& portion, const NColumnShard::TSchemeShardLocalPathId schemeShardLocalPathId) const {
    NArrow::Append<arrow::UInt64Type>(*builders[0], schemeShardLocalPathId.GetRawValue());
    const std::string prod = ::ToString(portion.GetProduced());
    NArrow::Append<arrow::StringType>(*builders[1], prod);
    NArrow::Append<arrow::UInt64Type>(*builders[2], ReadMetadata->GetTabletId());
    NArrow::Append<arrow::UInt64Type>(*builders[3], portion.GetRecordsCount());
    NArrow::Append<arrow::UInt64Type>(*builders[4], portion.GetColumnRawBytes());
    NArrow::Append<arrow::UInt64Type>(*builders[5], portion.GetIndexRawBytes());
    NArrow::Append<arrow::UInt64Type>(*builders[6], portion.GetColumnBlobBytes());
    NArrow::Append<arrow::UInt64Type>(*builders[7], portion.GetIndexBlobBytes());
    NArrow::Append<arrow::UInt64Type>(*builders[8], portion.GetPortionId());
    NArrow::Append<arrow::UInt8Type>(*builders[9], !portion.HasRemoveSnapshot());

    auto tierName = portion.GetTierNameDef(NBlobOperations::TGlobal::DefaultStorageId);
    NArrow::Append<arrow::StringType>(*builders[10], arrow::util::string_view(tierName.data(), tierName.size()));
    const TString statInfo = Default<TString>();
    NArrow::Append<arrow::StringType>(*builders[11], arrow::util::string_view(statInfo.data(), statInfo.size()));

    NArrow::Append<arrow::UInt8Type>(*builders[12], portion.HasRuntimeFeature(TPortionInfo::ERuntimeFeature::Optimized));
    NArrow::Append<arrow::UInt64Type>(*builders[13], portion.GetMeta().GetCompactionLevel());
    {
        NJson::TJsonValue details = NJson::JSON_MAP;
        details.InsertValue("snapshot_min", portion.RecordSnapshotMin().SerializeToJson());
        details.InsertValue("snapshot_max", portion.RecordSnapshotMax().SerializeToJson());
        details.InsertValue("primary_key_min", portion.IndexKeyStart().DebugString());
        details.InsertValue("primary_key_max", portion.IndexKeyEnd().DebugString());
        const auto detailsInfo = details.GetStringRobust();
        NArrow::Append<arrow::StringType>(*builders[14], arrow::util::string_view(detailsInfo.data(), detailsInfo.size()));
    }
}

ui32 TStatsIterator::PredictRecordsCount(const NAbstract::TGranuleMetaView& granule) const {
    return std::min<ui32>(10000, granule.GetPortions().size());
}

bool TStatsIterator::AppendStats(const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders, NAbstract::TGranuleMetaView& granule) const {
    ui64 recordsCount = 0;
    while (auto portion = granule.PopFrontPortion()) {
        recordsCount += 1;
        AFL_VERIFY(portion->GetPathId() == granule.GetPathId().InternalPathId);
        AppendStats(builders, *portion, granule.GetPathId().SchemeShardLocalPathId);
        if (recordsCount >= 10000) {
            break;
        }
    }
    return granule.GetPortions().size();
}

std::unique_ptr<TScanIteratorBase> TReadStatsMetadata::StartScan(const std::shared_ptr<TReadContext>& readContext) const {
    return std::make_unique<TStatsIterator>(readContext);
}

std::vector<std::pair<TString, NKikimr::NScheme::TTypeInfo>> TReadStatsMetadata::GetKeyYqlSchema() const {
    return GetColumns(TStatsIterator::StatsSchema, TStatsIterator::StatsSchema.KeyColumns);
}

std::shared_ptr<NAbstract::TReadStatsMetadata> TConstructor::BuildMetadata(
    const NColumnShard::TColumnShard* self, const TReadDescription& read) const {
    auto* index = self->GetIndexOptional();
    return std::make_shared<TReadStatsMetadata>(index ? index->CopyVersionedIndexPtr() : nullptr, self->TabletID(), Sorting, read.GetProgram(),
        index ? index->GetVersionedIndex().GetLastSchema() : nullptr, read.GetSnapshot());
}

}   // namespace NKikimr::NOlap::NReader::NSysView::NPortions
