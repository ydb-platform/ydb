#include "iterator.h"
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_context.h>

namespace NKikimr::NOlap::NReader::NSysView::NAbstract {

TStatsIteratorBase::TStatsIteratorBase(const std::shared_ptr<NReader::TReadContext>& context, const NTable::TScheme::TTableSchema& statsSchema)
    : StatsSchema(statsSchema)
    , Context(context)
    , ReadMetadata(context->GetReadMetadataPtrVerifiedAs<TReadStatsMetadata>())
    , KeySchema(MakeArrowSchema(StatsSchema.Columns, StatsSchema.KeyColumns))
    , ResultSchema(MakeArrowSchema(StatsSchema.Columns, ReadMetadata->ResultColumnIds))
    , IndexGranules(ReadMetadata->IndexGranules) {
    if (ResultSchema->num_fields() == 0) {
        ResultSchema = KeySchema;
    }
    std::vector<ui32> allColumnIds;
    for (const auto& c : StatsSchema.Columns) {
        allColumnIds.push_back(c.second.Id);
    }
    std::sort(allColumnIds.begin(), allColumnIds.end());
    DataSchema = MakeArrowSchema(StatsSchema.Columns, allColumnIds);
}

}   // namespace NKikimr::NOlap::NReader::NSysView::NAbstract
