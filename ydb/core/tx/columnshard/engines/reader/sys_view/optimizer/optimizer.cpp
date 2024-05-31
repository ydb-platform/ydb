#include "optimizer.h"
#include <ydb/core/formats/arrow/switch/switch_type.h>
#include <ydb/core/tx/columnshard/blobs_action/common/const.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_context.h>
#include <util/system/hostname.h>

namespace NKikimr::NOlap::NReader::NSysView::NOptimizer {

bool TStatsIterator::AppendStats(const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders, NAbstract::TGranuleMetaView& granule) const {
    for (auto&& i : granule.GetOptimizerTasks()) {
        NArrow::Append<arrow::UInt64Type>(*builders[0], granule.GetPathId());
        NArrow::Append<arrow::UInt64Type>(*builders[1], ReadMetadata->TabletId);
        NArrow::Append<arrow::UInt64Type>(*builders[2], i.GetTaskId());
        NArrow::Append<arrow::StringType>(*builders[3], HostNameField);
        NArrow::Append<arrow::UInt64Type>(*builders[4], NActors::TActivationContext::AsActorContext().SelfID.NodeId());
        NArrow::Append<arrow::StringType>(*builders[5], arrow::util::string_view(i.GetStart().data(), i.GetStart().size()));
        NArrow::Append<arrow::StringType>(*builders[6], arrow::util::string_view(i.GetFinish().data(), i.GetFinish().size()));
        NArrow::Append<arrow::StringType>(*builders[7], arrow::util::string_view(i.GetDetails().data(), i.GetDetails().size()));
        NArrow::Append<arrow::UInt64Type>(*builders[8], i.GetWeightCategory());
        NArrow::Append<arrow::Int64Type>(*builders[9], i.GetWeight());
    }
    return false;
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
