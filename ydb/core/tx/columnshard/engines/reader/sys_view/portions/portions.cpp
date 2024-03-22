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
    NArrow::Append<arrow::UInt64Type>(*builders[4], portion.GetRawBytes());
    NArrow::Append<arrow::UInt64Type>(*builders[5], portion.GetPortionId());
    NArrow::Append<arrow::BooleanType>(*builders[6], !portion.IsRemovedFor(ReadMetadata->GetRequestSnapshot()));

    auto tierName = portion.GetTierNameDef(NBlobOperations::TGlobal::DefaultStorageId);
    NArrow::Append<arrow::StringType>(*builders[7], arrow::util::string_view(tierName.data(), tierName.size()));
    auto statInfo = portion.GetMeta().GetStatisticsStorage().SerializeToProto().DebugString();
    NArrow::Append<arrow::StringType>(*builders[8], arrow::util::string_view(statInfo.data(), statInfo.size()));
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
        read.GetProgram(), index ? index->GetVersionedIndex().GetSchema(read.GetSnapshot()) : nullptr, read.GetSnapshot());
}

}
