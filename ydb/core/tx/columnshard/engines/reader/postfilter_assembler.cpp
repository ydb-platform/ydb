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
    if (Filter->GetInactiveHeadSize() > Filter->GetInactiveTailSize()) {
        options.SetRecordsCountLimit(Filter->Size() - Filter->GetInactiveHeadSize())
            .SetForwardAssemble(false);
        Filter->CutInactiveHead();
    } else {
        options.SetRecordsCountLimit(Filter->Size() - Filter->GetInactiveTailSize());
        Filter->CutInactiveTail();
    }

    auto addBatch = BatchConstructor.Assemble(options);
    Y_VERIFY(addBatch);
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)
        ("columns_count", addBatch->num_columns())("num_rows", addBatch->num_rows());
    Y_VERIFY(Filter->Apply(addBatch));
    Y_VERIFY(NArrow::MergeBatchColumns({ FilterBatch, addBatch }, FullBatch, FullColumnsOrder, true));

    return true;
}

bool TAssembleBatch::DoApply(TGranulesFillingContext& owner) const {
    TBatch& batch = owner.GetBatchInfo(BatchNo);
    batch.InitBatch(FullBatch);
    return true;
}

TAssembleBatch::TAssembleBatch(TPortionInfo::TPreparedBatchData&& batchConstructor,
    TBatch& currentBatch, const std::vector<std::string>& fullColumnsOrder,
    NColumnShard::IDataTasksProcessor::TPtr processor)
    : TBase(processor)
    , BatchConstructor(batchConstructor)
    , FullColumnsOrder(fullColumnsOrder)
    , Filter(currentBatch.GetFilter())
    , FilterBatch(currentBatch.GetFilterBatch())
    , BatchNo(currentBatch.GetBatchNo())
{
    TBase::SetPriority(TBase::EPriority::High);
    Y_VERIFY(currentBatch.GetFilter());
    Y_VERIFY(currentBatch.GetFilterBatch());
    Y_VERIFY(!currentBatch.GetFilteredBatch());
}

}
