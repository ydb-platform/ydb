#include "filter_assembler.h"
#include <ydb/core/tx/columnshard/engines/filter.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

namespace NKikimr::NOlap::NIndexedReader {

bool TAssembleFilter::DoExecuteImpl() {
    /// @warning The replace logic is correct only in assumption that predicate is applied over a part of ReplaceKey.
    /// It's not OK to apply predicate before replacing key duplicates otherwise.
    /// Assumption: dup(A, B) <=> PK(A) = PK(B) => Predicate(A) = Predicate(B) => all or no dups for PK(A) here

    TPortionInfo::TPreparedBatchData::TAssembleOptions options;
    options.IncludedColumnIds = FilterColumnIds;
    auto batch = BatchConstructor.Assemble(options);
    Y_VERIFY(batch);
    Y_VERIFY(batch->num_rows());
    OriginalCount = batch->num_rows();
    Filter = std::make_shared<NArrow::TColumnFilter>(NOlap::FilterPortion(batch, *ReadMetadata));
    if (!Filter->Apply(batch)) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "skip_data")("original_count", OriginalCount);
        FilteredBatch = nullptr;
        return true;
    }
    auto earlyFilter = ReadMetadata->GetProgram().BuildEarlyFilter(batch);
    if (earlyFilter) {
        if (AllowEarlyFilter) {
            Filter = std::make_shared<NArrow::TColumnFilter>(Filter->CombineSequentialAnd(*earlyFilter));
            if (!earlyFilter->Apply(batch)) {
                NYDBTest::TControllers::GetColumnShardController()->OnAfterFilterAssembling(batch);
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "skip_data")("original_count", OriginalCount);
                FilteredBatch = nullptr;
                return true;
            } else {
                NYDBTest::TControllers::GetColumnShardController()->OnAfterFilterAssembling(batch);
            }
        } else if (BatchesOrderPolicy->NeedNotAppliedEarlyFilter()) {
            EarlyFilter = earlyFilter;
        }
    }

    if ((size_t)batch->schema()->num_fields() < BatchConstructor.GetColumnsCount()) {
        TPortionInfo::TPreparedBatchData::TAssembleOptions options;
        options.ExcludedColumnIds = FilterColumnIds;
        auto addBatch = BatchConstructor.Assemble(options);
        Y_VERIFY(addBatch);
        Y_VERIFY(Filter->Apply(addBatch));
        Y_VERIFY(NArrow::MergeBatchColumns({ batch, addBatch }, batch, BatchConstructor.GetSchemaColumnNames(), true));
    }
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "not_skip_data")
        ("original_count", OriginalCount)("filtered_count", batch->num_rows())("columns_count", BatchConstructor.GetColumnsCount())("allow_early", AllowEarlyFilter)
        ("filter_columns", FilterColumnIds.size());

    FilteredBatch = batch;
    return true;
}

bool TAssembleFilter::DoApply(TGranulesFillingContext& owner) const {
    Y_VERIFY(OriginalCount);
    owner.GetCounters().OriginalRowsCount->Add(OriginalCount);
    owner.GetCounters().AssembleFilterCount->Add(1);
    TBatch* batch = owner.GetBatchInfo(BatchAddress);
    if (batch) {
        batch->InitFilter(Filter, FilteredBatch, OriginalCount, EarlyFilter);
    }
    return true;
}

}
