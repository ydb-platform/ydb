#include "column_assembler.h"
#include "plain_read_data.h"

namespace NKikimr::NOlap::NPlainReader {

bool TAssembleBatch::DoExecuteImpl() {
    /// @warning The replace logic is correct only in assumption that predicate is applied over a part of ReplaceKey.
    /// It's not OK to apply predicate before replacing key duplicates otherwise.
    /// Assumption: dup(A, B) <=> PK(A) = PK(B) => Predicate(A) = Predicate(B) => all or no dups for PK(A) here

    Y_VERIFY(BatchConstructor.GetColumnsCount());

    TPortionInfo::TPreparedBatchData::TAssembleOptions options;
    auto addBatch = BatchConstructor.Assemble(options);
    Y_VERIFY(addBatch);
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)
        ("columns_count", addBatch->num_columns())("num_rows", addBatch->num_rows());
    Filter->Apply(addBatch);
    Result = addBatch;

    return true;
}

bool TAssembleFFBatch::DoApply(IDataReader& owner) const {
    owner.GetMeAs<TPlainReadData>().GetSourceByIdxVerified(SourceIdx).InitFetchStageData(Result);
    return true;
}

TAssembleBatch::TAssembleBatch(TPortionInfo::TPreparedBatchData&& batchConstructor,
    const ui32 sourceIdx, const std::shared_ptr<NArrow::TColumnFilter>& filter,
    const NColumnShard::IDataTasksProcessor::TPtr& processor)
    : TBase(processor)
    , BatchConstructor(batchConstructor)
    , Filter(filter)
    , SourceIdx(sourceIdx)
{
    TBase::SetPriority(TBase::EPriority::High);
}

}
