#include "column_assembler.h"
#include "plain_read_data.h"

namespace NKikimr::NOlap::NReader::NPlain {

bool TAssembleBatch::DoExecute() {
    /// @warning The replace logic is correct only in assumption that predicate is applied over a part of ReplaceKey.
    /// It's not OK to apply predicate before replacing key duplicates otherwise.
    /// Assumption: dup(A, B) <=> PK(A) = PK(B) => Predicate(A) = Predicate(B) => all or no dups for PK(A) here

    auto batchConstructor = BuildBatchConstructor(FetchColumns->GetFilteredSchemaVerified());

    Y_ABORT_UNLESS(batchConstructor.GetColumnsCount());

    TPortionInfo::TPreparedBatchData::TAssembleOptions options;
    auto addBatch = batchConstructor.AssembleTable(options);
    Y_ABORT_UNLESS(addBatch);
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)
        ("columns_count", addBatch->num_columns())("num_rows", addBatch->num_rows());
    Filter->Apply(addBatch);
    Result = NArrow::ToBatch(addBatch, true);

    return true;
}

bool TAssembleFFBatch::DoApply(IDataReader& /*owner*/) const {
    Source->InitFetchStageData(Result);
    return true;
}

TAssembleBatch::TAssembleBatch(const std::shared_ptr<TSpecialReadContext>& context, const std::shared_ptr<TPortionInfo>& portionInfo,
    const std::shared_ptr<IDataSource>& source, const std::shared_ptr<TColumnsSet>& columns, const THashMap<TBlobRange, TPortionInfo::TAssembleBlobInfo>& blobs, const std::shared_ptr<NArrow::TColumnFilter>& filter)
    : TBase(context, portionInfo, source, std::move(blobs))
    , Filter(filter)
    , TaskGuard(Context->GetCommonContext()->GetCounters().GetAssembleTasksGuard())
    , FetchColumns(columns)
{
    TBase::SetPriority(TBase::EPriority::High);
}

}
