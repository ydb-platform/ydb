#include "portions.h"
#include <ydb/core/formats/arrow/switch/switch_type.h>
#include <ydb/core/tx/columnshard/blobs_action/common/const.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_context.h>

namespace NKikimr::NOlap::NReader::NSysView::NPortions {

void TStatsIterator::AppendStats(const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders, const TPortionInfo& portion) const {
    NArrow::Append<arrow::UInt64Type>(*builders[0], portion.GetPathId());
    const std::string prod = ::ToString(portion.GetMeta().Produced);
    NArrow::Append<arrow::StringType>(*builders[1], prod);
    NArrow::Append<arrow::UInt64Type>(*builders[2], ReadMetadata->TabletId);
    NArrow::Append<arrow::UInt64Type>(*builders[3], portion.NumRows());
    NArrow::Append<arrow::UInt64Type>(*builders[4], portion.GetColumnRawBytes());
    NArrow::Append<arrow::UInt64Type>(*builders[5], portion.GetIndexRawBytes());
    NArrow::Append<arrow::UInt64Type>(*builders[6], portion.GetColumnBlobBytes());
    NArrow::Append<arrow::UInt64Type>(*builders[7], portion.GetIndexBlobBytes());
    NArrow::Append<arrow::UInt64Type>(*builders[8], portion.GetPortionId());
    NArrow::Append<arrow::UInt8Type>(*builders[9], !portion.IsRemovedFor(ReadMetadata->GetRequestSnapshot()));

    auto tierName = portion.GetTierNameDef(NBlobOperations::TGlobal::DefaultStorageId);
    NArrow::Append<arrow::StringType>(*builders[10], arrow::util::string_view(tierName.data(), tierName.size()));
    NJson::TJsonValue statReport = NJson::JSON_ARRAY;
    for (auto&& i : portion.GetIndexes()) {
        if (!i.HasBlobData()) {
            continue;
        }
        auto schema = portion.GetSchema(ReadMetadata->GetIndexVersions());
        auto indexMeta = schema->GetIndexInfo().GetIndexVerified(i.GetEntityId());
        statReport.AppendValue(indexMeta->SerializeDataToJson(i, schema->GetIndexInfo()));
    }
    auto statInfo = statReport.GetStringRobust();
    NArrow::Append<arrow::StringType>(*builders[11], arrow::util::string_view(statInfo.data(), statInfo.size()));

    NArrow::Append<arrow::UInt8Type>(*builders[12], portion.HasRuntimeFeature(TPortionInfo::ERuntimeFeature::Optimized));
}

ui32 TStatsIterator::PredictRecordsCount(const NAbstract::TGranuleMetaView& granule) const {
    return std::min<ui32>(10000, granule.GetPortions().size());
}

bool TStatsIterator::AppendStats(const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders, NAbstract::TGranuleMetaView& granule) const {
    ui64 recordsCount = 0;
    while (auto portion = granule.PopFrontPortion()) {
        recordsCount += 1;
        AppendStats(builders, *portion);
        if (recordsCount >= 10000) {
            break;
        }
    }
    return granule.GetPortions().size();
}

std::unique_ptr<TScanIteratorBase> TReadStatsMetadata::StartScan(const std::shared_ptr<TReadContext>& readContext) const {
    return std::make_unique<TStatsIterator>(readContext->GetReadMetadataPtrVerifiedAs<TReadStatsMetadata>());
}

std::vector<std::pair<TString, NKikimr::NScheme::TTypeInfo>> TReadStatsMetadata::GetKeyYqlSchema() const {
    return GetColumns(TStatsIterator::StatsSchema, TStatsIterator::StatsSchema.KeyColumns);
}

std::shared_ptr<NAbstract::TReadStatsMetadata> TConstructor::BuildMetadata(const NColumnShard::TColumnShard* self, const TReadDescription& read) const {
    auto* index = self->GetIndexOptional();
    return std::make_shared<TReadStatsMetadata>(index ? index->CopyVersionedIndexPtr() : nullptr, self->TabletID(),
        IsReverse ? TReadMetadataBase::ESorting::DESC : TReadMetadataBase::ESorting::ASC,
        read.GetProgram(), index ? index->GetVersionedIndex().GetLastSchema() : nullptr, read.GetSnapshot());
}

}
