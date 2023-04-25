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
    OriginalCount = batch->num_rows();
    Filter = std::make_shared<NArrow::TColumnFilter>(NOlap::FilterPortion(batch, *ReadMetadata));
    if (!Filter->Apply(batch)) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "skip_data")("original_count", OriginalCount);
        FilteredBatch = nullptr;
        return true;
    }
#if 1 // optimization
    if (ReadMetadata->Program && AllowEarlyFilter) {
        auto filter = NOlap::EarlyFilter(batch, ReadMetadata->Program);
        Filter->CombineSequential(filter);
        if (!filter.Apply(batch)) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "skip_data")("original_count", OriginalCount);
            FilteredBatch = nullptr;
            return true;
        }
    }
#else
    Y_UNUSED(AllowEarlyFilter);
#endif
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "not_skip_data")("original_count", OriginalCount)("filtered_count", batch->num_rows());

    FilteredBatch = batch;
    return true;
}

bool TAssembleFilter::DoApply(TIndexedReadData& owner) const {
    TBatch& batch = owner.GetBatchInfo(BatchNo);
    Y_VERIFY(OriginalCount);
    owner.GetCounters().GetOriginalRowsCount()->Add(OriginalCount);
    batch.InitFilter(Filter, FilteredBatch);
    if (!FilteredBatch || FilteredBatch->num_rows() == 0) {
        owner.GetCounters().GetEmptyFilterPortionsCount()->Add(1);
        owner.GetCounters().GetEmptyFilterPortionsBytes()->Add(batch.GetFetchedBytes());
        batch.InitBatch(FilteredBatch);
    } else {
        owner.GetCounters().GetFilteredRowsCount()->Add(FilteredBatch->num_rows());
        owner.GetCounters().GetUsefulFilterBytes()->Add(batch.GetFetchedBytes() * FilteredBatch->num_rows() / OriginalCount);
        if (batch.AskedColumnsAlready(owner.GetPostFilterColumns())) {
            owner.GetCounters().GetFilterOnlyPortionsCount()->Add(1);
            owner.GetCounters().GetFilterOnlyPortionsBytes()->Add(batch.GetFetchedBytes());
            batch.InitBatch(FilteredBatch);
        } else {
            owner.GetCounters().GetPostFilterPortionsCount()->Add(1);
            batch.Reset(&owner.GetPostFilterColumns());
            owner.GetCounters().GetUsefulPostFilterBytes()->Add(batch.GetWaitingBytes() * FilteredBatch->num_rows() / OriginalCount);
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "additional_data")
                ("filtered_count", FilteredBatch->num_rows())
                ("blobs_count", batch.GetWaitingBlobs().size())
                ("columns_count", batch.GetCurrentColumnIds()->size())
                ("fetch_size", batch.GetWaitingBytes())
                ;
        }
    }
    return true;
}

}
