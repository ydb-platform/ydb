#include "filter_assembler.h"
#include "plain_read_data.h"
#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/tx/columnshard/engines/filter.h>

namespace NKikimr::NOlap::NPlainReader {

bool TAssembleFilter::DoExecute() {
    /// @warning The replace logic is correct only in assumption that predicate is applied over a part of ReplaceKey.
    /// It's not OK to apply predicate before replacing key duplicates otherwise.
    /// Assumption: dup(A, B) <=> PK(A) = PK(B) => Predicate(A) = Predicate(B) => all or no dups for PK(A) here

    TPortionInfo::TPreparedBatchData::TAssembleOptions options;
    options.IncludedColumnIds = FilterColumnIds;
    if (RecordsMaxSnapshot <= ReadMetadata->GetSnapshot() && UseFilter) {
        for (auto&& i : TIndexInfo::GetSpecialColumnIds()) {
            options.IncludedColumnIds->erase(i);
        }
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "skip_special_columns");
    }

    auto batchConstructor = BuildBatchConstructor(FilterColumnIds);

    auto batch = batchConstructor.AssembleTable(options);
    Y_ABORT_UNLESS(batch);
    Y_ABORT_UNLESS(batch->num_rows());
    if (RecordsMaxSnapshot <= ReadMetadata->GetSnapshot() && UseFilter) {
        for (auto&& f : TIndexInfo::ArrowSchemaSnapshot()->fields()) {
            auto c = NArrow::TStatusValidator::GetValid(arrow::MakeArrayOfNull(f->type(), batch->num_rows()));
            batch = NArrow::TStatusValidator::GetValid(batch->AddColumn(batch->num_columns(), f, std::make_shared<arrow::ChunkedArray>(c)));
        }
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "restore_fake_special_columns");
    }

    OriginalCount = batch->num_rows();
    AppliedFilter = std::make_shared<NArrow::TColumnFilter>(NOlap::FilterPortion(batch, *ReadMetadata, ReadMetadata->GetSnapshot() < RecordsMaxSnapshot));
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "first_filter_using");
    if (!AppliedFilter->Apply(batch)) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "skip_data")("original_count", OriginalCount)("columns_count", FilterColumnIds.size());
        return true;
    }
    auto earlyFilter = ReadMetadata->GetProgram().BuildEarlyFilter(batch);
    if (earlyFilter) {
        if (UseFilter) {
            AppliedFilter = std::make_shared<NArrow::TColumnFilter>(AppliedFilter->CombineSequentialAnd(*earlyFilter));
            if (!earlyFilter->Apply(batch)) {
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "skip_data")("original_count", OriginalCount)("columns_count", FilterColumnIds.size());;
                return true;
            }
        } else {
            EarlyFilter = earlyFilter;
        }
    }

    if ((size_t)batch->schema()->num_fields() < batchConstructor.GetColumnsCount()) {
        TPortionInfo::TPreparedBatchData::TAssembleOptions options;
        options.ExcludedColumnIds = FilterColumnIds;
        auto addBatch = batchConstructor.AssembleTable(options);
        Y_ABORT_UNLESS(addBatch);
        Y_ABORT_UNLESS(AppliedFilter->Apply(addBatch));
        Y_ABORT_UNLESS(NArrow::MergeBatchColumns({ batch, addBatch }, batch, batchConstructor.GetSchemaColumnNames(), true));
    }
    AFL_VERIFY(AppliedFilter->IsTotalAllowFilter() || AppliedFilter->Size() == OriginalCount)("original", OriginalCount)("af_count", AppliedFilter->Size());
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "not_skip_data")
        ("original_count", OriginalCount)("filtered_count", batch->num_rows())("columns_count", batchConstructor.GetColumnsCount())("use_filter", UseFilter)
        ("filter_columns", FilterColumnIds.size())("af_count", AppliedFilter->Size())("ef_count", earlyFilter ? earlyFilter->Size() : 0);

    FilteredBatch = NArrow::ToBatch(batch, true);
    return true;
}

bool TAssembleFilter::DoApply(IDataReader& /*owner*/) const {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "apply");
    Source->InitFilterStageData(AppliedFilter, EarlyFilter, FilteredBatch, Source);
    return true;
}

}
