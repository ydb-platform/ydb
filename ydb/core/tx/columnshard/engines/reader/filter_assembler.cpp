#include "filter_assembler.h"
#include <ydb/core/tx/columnshard/engines/filter.h>

namespace NKikimr::NOlap::NIndexedReader {

bool TAssembleFilter::DoExecuteImpl() {
    /// @warning The replace logic is correct only in assumption that predicate is applied over a part of ReplaceKey.
    /// It's not OK to apply predicate before replacing key duplicates otherwise.
    /// Assumption: dup(A, B) <=> PK(A) = PK(B) => Predicate(A) = Predicate(B) => all or no dups for PK(A) here
    auto batch = BatchConstructor.Assemble();
    Y_VERIFY(batch);
    Y_VERIFY(batch->num_rows());
    const ui32 originalCount = batch->num_rows();
    Filter = std::make_shared<NArrow::TColumnFilter>(NOlap::FilterPortion(batch, *ReadMetadata));
    if (!Filter->Apply(batch)) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "skip_data")("original_count", originalCount);
        FilteredBatch = nullptr;
        return true;
    }
#if 1 // optimization
    if (ReadMetadata->Program && AllowEarlyFilter) {
        auto filter = NOlap::EarlyFilter(batch, ReadMetadata->Program);
        Filter->CombineSequential(filter);
        if (!filter.Apply(batch)) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "skip_data")("original_count", originalCount);
            FilteredBatch = nullptr;
            return true;
        }
    }
#else
    Y_UNUSED(AllowEarlyFilter);
#endif
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "not_skip_data")("original_count", originalCount)("filtered_count", batch->num_rows());

    FilteredBatch = batch;
    return true;
}

bool TAssembleFilter::DoApply(TIndexedReadData& owner) const {
    TBatch& batch = owner.GetBatchInfo(BatchNo);
    batch.InitFilter(Filter, FilteredBatch);
    if (batch.AskedColumnsAlready(owner.GetPostFilterColumns()) || !FilteredBatch || FilteredBatch->num_rows() == 0) {
        batch.InitBatch(FilteredBatch);
    } else {
        batch.Reset(&owner.GetPostFilterColumns());
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "additional_data")
            ("filtered_count", FilteredBatch->num_rows())
            ("blobs_count", batch.GetWaitingBlobs().size())
            ("columns_count", batch.GetCurrentColumnIds()->size())
            ("fetch_size", batch.GetWaitingBytes())
            ;
    }
    return true;
}

}
