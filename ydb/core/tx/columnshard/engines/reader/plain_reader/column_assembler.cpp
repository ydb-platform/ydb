#include "column_assembler.h"
#include "plain_read_data.h"

namespace NKikimr::NOlap::NPlainReader {

bool TAssembleBatch::DoExecute() {
    /// @warning The replace logic is correct only in assumption that predicate is applied over a part of ReplaceKey.
    /// It's not OK to apply predicate before replacing key duplicates otherwise.
    /// Assumption: dup(A, B) <=> PK(A) = PK(B) => Predicate(A) = Predicate(B) => all or no dups for PK(A) here

    Y_ABORT_UNLESS(BatchConstructor.GetColumnsCount());

    TPortionInfo::TPreparedBatchData::TAssembleOptions options;
    auto addBatch = BatchConstructor.Assemble(options);
    Y_ABORT_UNLESS(addBatch);
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)
        ("columns_count", addBatch->num_columns())("num_rows", addBatch->num_rows());
    Filter->Apply(addBatch);
    Result = addBatch;

    return true;
}

bool TAssembleFFBatch::DoApply(IDataReader& /*owner*/) const {
    Source->InitFetchStageData(Result);
    return true;
}

TAssembleBatch::TAssembleBatch(const NActors::TActorId& scanActorId, TPortionInfo::TPreparedBatchData&& batchConstructor,
    const std::shared_ptr<IDataSource>& source, const std::shared_ptr<NArrow::TColumnFilter>& filter, NColumnShard::TCounterGuard&& taskGuard)
    : TBase(scanActorId)
    , BatchConstructor(batchConstructor)
    , Filter(filter)
    , Source(source)
    , TaskGuard(std::move(taskGuard))
{
    TBase::SetPriority(TBase::EPriority::High);
}

}
