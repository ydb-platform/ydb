#include "index_info.h"
#include <ydb/core/sys_view/common/path.h>
#include <ydb/core/sys_view/common/schema.h>
#include <ydb/core/formats/arrow/simple_arrays_cache.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/scalar.h>

namespace NKikimr::NOlap {

std::shared_ptr<NKikimr::NOlap::TColumnLoader> IIndexInfo::GetColumnLoaderVerified(const ui32 columnId) const {
    auto result = GetColumnLoaderOptional(columnId);
    AFL_VERIFY(result);
    return result;
}

void IIndexInfo::AddDeleteFlagsColumn(NArrow::TGeneralContainer& batch, const bool isDelete) {
    const i64 numRows = batch.num_rows();

    batch.AddField(arrow::field(SPEC_COL_DELETE_FLAG, arrow::boolean()), 
        NArrow::TThreadSimpleArraysCache::GetConst(arrow::boolean(), std::make_shared<arrow::BooleanScalar>(isDelete), numRows)).Validate();
}

void IIndexInfo::AddSnapshotColumns(NArrow::TGeneralContainer& batch, const TSnapshot& snapshot) {
    const i64 numRows = batch.num_rows();

    batch.AddField(arrow::field(SPEC_COL_PLAN_STEP, arrow::uint64()), NArrow::MakeUI64Array(snapshot.GetPlanStep(), numRows)).Validate();
    batch.AddField(arrow::field(SPEC_COL_TX_ID, arrow::uint64()), NArrow::MakeUI64Array(snapshot.GetTxId(), numRows)).Validate();
}

void IIndexInfo::NormalizeDeletionColumn(NArrow::TGeneralContainer& batch) {
    if (batch.HasColumn(SPEC_COL_DELETE_FLAG)) {
        return;
    }
    AddDeleteFlagsColumn(batch, false);
}

std::optional<ui32> IIndexInfo::GetColumnIdOptional(const std::string& name) const {
    if (name == SPEC_COL_PLAN_STEP) {
        return ui32(ESpecialColumn::PLAN_STEP);
    } else if (name == SPEC_COL_TX_ID) {
        return ui32(ESpecialColumn::TX_ID);
    } else if (name == SPEC_COL_DELETE_FLAG) {
        return ui32(ESpecialColumn::DELETE_FLAG);
    }
    return {};
}

TString IIndexInfo::GetColumnName(ui32 id, bool required) const {
    if (ESpecialColumn(id) == ESpecialColumn::PLAN_STEP) {
        return SPEC_COL_PLAN_STEP;
    } else if (ESpecialColumn(id) == ESpecialColumn::TX_ID) {
        return SPEC_COL_TX_ID;
    } else if (ESpecialColumn(id) == ESpecialColumn::DELETE_FLAG) {
        return SPEC_COL_DELETE_FLAG;
    } else {
        AFL_VERIFY(!required);
        return Default<TString>();
    }
}

ui32 IIndexInfo::CalcDeletions(const std::shared_ptr<arrow::RecordBatch>& batch, const bool needExistsColumn) {
    auto c = batch->GetColumnByName(IIndexInfo::SPEC_COL_DELETE_FLAG);
    if (!needExistsColumn) {
        if (!c) {
            return 0;
        }
    }
    AFL_VERIFY(c);
    AFL_VERIFY(c->type()->id() == arrow::boolean()->id());
    auto cBool = static_pointer_cast<arrow::BooleanArray>(c);
    return cBool->true_count();
}

std::shared_ptr<arrow::Field> IIndexInfo::GetColumnFieldOptional(const ui32 columnId) {
    if (ESpecialColumn(columnId) == ESpecialColumn::PLAN_STEP) {
        return ArrowSchemaSnapshot()->field(0);
    } else if (ESpecialColumn(columnId) == ESpecialColumn::TX_ID) {
        return ArrowSchemaSnapshot()->field(1);
    } else if (ESpecialColumn(columnId) == ESpecialColumn::DELETE_FLAG) {
        return ArrowSchemaDeletion()->field(0);
    } else {
        return nullptr;
    }
}

std::shared_ptr<arrow::Field> IIndexInfo::GetColumnFieldVerified(const ui32 columnId) {
    auto result = GetColumnFieldOptional(columnId);
    AFL_VERIFY(result);
    return result;
}

std::shared_ptr<arrow::Scalar> IIndexInfo::DefaultColumnValue(const ui32 colId) {
    if (colId == (ui32)ESpecialColumn::PLAN_STEP) {
        return nullptr;
    } else if (colId == (ui32)ESpecialColumn::TX_ID) {
        return nullptr;
    } else if (colId == (ui32)ESpecialColumn::DELETE_FLAG) {
        static const std::shared_ptr<arrow::Scalar> deleteDefault(new arrow::BooleanScalar(false));
        return deleteDefault;
    } else {
        AFL_VERIFY(false);
        return nullptr;
    }
}

} // namespace NKikimr::NOlap
