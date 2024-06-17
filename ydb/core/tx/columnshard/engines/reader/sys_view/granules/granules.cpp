#include "granules.h"
#include <ydb/core/formats/arrow/switch/switch_type.h>
#include <ydb/core/tx/columnshard/blobs_action/common/const.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_context.h>
#include <util/system/hostname.h>

namespace NKikimr::NOlap::NReader::NSysView::NGranules {

bool TStatsIterator::AppendStats(const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders, NAbstract::TGranuleMetaView& granule) const {
    NArrow::Append<arrow::UInt64Type>(*builders[0], granule.GetPathId());
    NArrow::Append<arrow::UInt64Type>(*builders[1], ReadMetadata->TabletId);
    NArrow::Append<arrow::UInt64Type>(*builders[2], granule.GetPortions().size());
    NArrow::Append<arrow::StringType>(*builders[3], HostNameField);
    NArrow::Append<arrow::UInt64Type>(*builders[4], NActors::TActivationContext::AsActorContext().SelfID.NodeId());
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
