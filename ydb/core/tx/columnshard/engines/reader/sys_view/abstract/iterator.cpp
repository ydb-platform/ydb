#include "iterator.h"

#include <ydb/core/formats/arrow/program/abstract.h>
#include <ydb/core/formats/arrow/program/collection.h>
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

TConclusion<std::shared_ptr<TPartialReadResult>> TStatsIteratorBase::GetBatch() {
    while (!Finished()) {
        if (!IsReadyForBatch()) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "batch_not_ready");
            return std::shared_ptr<TPartialReadResult>();
        }
        auto batchOpt = ExtractStatsBatch();
        if (!batchOpt) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "no_batch_on_finished");
            AFL_VERIFY(Finished());
            return std::shared_ptr<TPartialReadResult>();
        }
        auto originalBatch = *batchOpt;
        if (originalBatch->num_rows() == 0) {
            continue;
        }
        auto keyBatch = NArrow::TColumnOperator().VerifyIfAbsent().Adapt(originalBatch, KeySchema).DetachResult();
        auto lastKey = keyBatch->Slice(keyBatch->num_rows() - 1, 1);

        {
            NArrow::TColumnFilter filter = ReadMetadata->GetPKRangesFilter().BuildFilter(originalBatch);
            AFL_VERIFY(filter.Apply(originalBatch));
        }

        // Leave only requested columns
        auto resultBatch = NArrow::TColumnOperator().Adapt(originalBatch, ResultSchema).DetachResult();
        NArrow::NSSA::TSchemaColumnResolver resolver(DataSchema);
        auto collection = std::make_shared<NArrow::NAccessor::TAccessorsCollection>(resultBatch, resolver);
        auto source = std::make_shared<NArrow::NSSA::TFakeDataSource>();
        auto applyConclusion = ReadMetadata->GetProgram().ApplyProgram(collection, source);
        if (applyConclusion.IsFail()) {
            return applyConclusion;
        }
        if (collection->GetRecordsCountOptional().value_or(0) == 0) {
            continue;
        }
        auto table = collection->ToTable({}, &resolver, false);
        return std::make_shared<TPartialReadResult>(table, std::make_shared<TPlainScanCursor>(lastKey), Context, std::nullopt);
    }
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "finished_iterator");
    return std::shared_ptr<TPartialReadResult>();
}

}   // namespace NKikimr::NOlap::NReader::NSysView::NAbstract
