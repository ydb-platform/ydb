#include "tables.h"
#include <ydb/core/formats/arrow/switch/switch_type.h>
#include <ydb/core/tx/columnshard/blobs_action/common/const.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_context.h>

namespace NKikimr::NOlap::NReader::NSysView::NTables {

ui32 TStatsIterator::PredictRecordsCount(const NAbstract::TGranuleMetaView& /* granule */) const {
    return 100;
}

bool TStatsIterator::AppendStats(const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders, NAbstract::TGranuleMetaView& granule) const {
    const auto& pathId = granule.GetPathId();
    NArrow::Append<arrow::UInt64Type>(*builders[0], pathId.InternalPathId.GetRawValue());
    NArrow::Append<arrow::UInt64Type>(*builders[1], ReadMetadata->GetTabletId());
    NArrow::Append<arrow::UInt64Type>(*builders[2], pathId.SchemeShardLocalPathId.GetRawValue());
    return false;
}

std::unique_ptr<TScanIteratorBase> TReadStatsMetadata::StartScan(const std::shared_ptr<TReadContext>& readContext) const {
    return std::make_unique<TStatsIterator>(readContext);
}

std::vector<std::pair<TString, NKikimr::NScheme::TTypeInfo>> TReadStatsMetadata::GetKeyYqlSchema() const {
    return GetColumns(TStatsIterator::StatsSchema, TStatsIterator::StatsSchema.KeyColumns);
}

std::shared_ptr<NAbstract::TReadStatsMetadata> TConstructor::BuildMetadata(const NColumnShard::TColumnShard* self, const TReadDescription& read) const {
    auto* index = self->GetIndexOptional();
    return std::make_shared<TReadStatsMetadata>(index ? index->CopyVersionedIndexPtr() : nullptr, self->TabletID(), Sorting, read.GetProgram(),
        index ? index->GetVersionedIndex().GetLastSchema() : nullptr, read.GetSnapshot());
}

}
