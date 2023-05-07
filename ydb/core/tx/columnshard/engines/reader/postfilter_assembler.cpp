#include "postfilter_assembler.h"
#include "batch.h"
#include <ydb/core/tx/columnshard/engines/indexed_read_data.h>

namespace NKikimr::NOlap::NIndexedReader {

bool TAssembleBatch::DoExecuteImpl() {
    /// @warning The replace logic is correct only in assumption that predicate is applied over a part of ReplaceKey.
    /// It's not OK to apply predicate before replacing key duplicates otherwise.
    /// Assumption: dup(A, B) <=> PK(A) = PK(B) => Predicate(A) = Predicate(B) => all or no dups for PK(A) here

    Y_VERIFY(BatchConstructor.GetColumnsCount());

    TPortionInfo::TPreparedBatchData::TAssembleOptions options;
    options.RecordsCountLimit = Filter->Size();
    auto addBatch = BatchConstructor.Assemble(options);
    Y_VERIFY(addBatch);
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)
        ("columns_count", addBatch->num_columns())("num_rows", addBatch->num_rows());
    Y_VERIFY(Filter->Apply(addBatch));
    Y_VERIFY(NArrow::MergeBatchColumns({ FilterBatch, addBatch }, FullBatch, FullColumnsOrder, true));

    return true;
}

bool TAssembleBatch::DoApply(TGranulesFillingContext& owner) const {
    TBatch& batch = owner.GetBatchInfo(BatchAddress);
    batch.InitBatch(FullBatch);
    return true;
}

TAssembleBatch::TAssembleBatch(TPortionInfo::TPreparedBatchData&& batchConstructor,
    TBatch& currentBatch, const std::vector<std::string>& fullColumnsOrder,
    NColumnShard::IDataTasksProcessor::TPtr processor)
    : TBase(processor)
    , BatchConstructor(batchConstructor)
    , FullColumnsOrder(fullColumnsOrder)
    , Filter(currentBatch.GetFetchedInfo().GetFilter())
    , FilterBatch(currentBatch.GetFetchedInfo().GetFilterBatch())
    , BatchAddress(currentBatch.GetBatchAddress())
{
    TBase::SetPriority(TBase::EPriority::High);
    Y_VERIFY(currentBatch.GetFetchedInfo().GetFilter());
    Y_VERIFY(currentBatch.GetFetchedInfo().GetFilterBatch());
    Y_VERIFY(!currentBatch.GetFetchedInfo().GetFilteredBatch());
}

}
