#include "abstract_scheme.h"

#include <ydb/core/tx/columnshard/engines/index_info.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/library/formats/arrow/simple_arrays_cache.h>
#include <util/string/join.h>

namespace NKikimr::NOlap {

std::shared_ptr<arrow::Field> ISnapshotSchema::GetFieldByIndex(const int index) const {
    auto schema = GetSchema();
    if (!schema || index < 0 || index >= schema->num_fields()) {
        return nullptr;
    }
    return schema->field(index);
}
std::shared_ptr<arrow::Field> ISnapshotSchema::GetFieldByColumnIdOptional(const ui32 columnId) const {
    return GetFieldByIndex(GetFieldIndex(columnId));
}

std::set<ui32> ISnapshotSchema::GetPkColumnsIds() const {
    std::set<ui32> result;
    for (auto&& field : GetIndexInfo().GetReplaceKey()->fields()) {
        result.emplace(GetColumnId(field->name()));
    }
    return result;

}

TConclusion<std::shared_ptr<NArrow::TGeneralContainer>> ISnapshotSchema::NormalizeBatch(
    const ISnapshotSchema& dataSchema, const std::shared_ptr<NArrow::TGeneralContainer>& batch, const std::set<ui32>& restoreColumnIds) const {
    AFL_VERIFY(dataSchema.GetSnapshot() <= GetSnapshot());
    if (dataSchema.GetSnapshot() == GetSnapshot()) {
        if (batch->GetColumnsCount() == GetColumnsCount()) {
            return batch;
        }
    }
    const std::shared_ptr<NArrow::TSchemaLite>& resultArrowSchema = GetSchema();

    std::shared_ptr<NArrow::TGeneralContainer> result = std::make_shared<NArrow::TGeneralContainer>(batch->GetRecordsCount());
    for (size_t i = 0; i < resultArrowSchema->fields().size(); ++i) {
        auto& resultField = resultArrowSchema->fields()[i];
        auto columnId = GetIndexInfo().GetColumnIdVerified(resultField->name());
        auto oldField = dataSchema.GetFieldByColumnIdOptional(columnId);
        if (oldField) {
            auto fAccessor = batch->GetAccessorByNameOptional(oldField->name());
            if (fAccessor) {
                auto conclusion = result->AddField(resultField, fAccessor);
                if (conclusion.IsFail()) {
                    return conclusion;
                }
                continue;
            }
        }
        if (restoreColumnIds.contains(columnId)) {
            AFL_VERIFY(!!GetExternalDefaultValueVerified(columnId) || GetIndexInfo().IsNullableVerified(columnId))("column_name",
                                                                          GetIndexInfo().GetColumnName(columnId, false))("id", columnId);
            result->AddField(resultField, GetColumnLoaderVerified(columnId)->BuildDefaultAccessor(batch->num_rows())).Validate();
        }
    }
    return result;
}

TConclusion<std::shared_ptr<arrow::RecordBatch>> ISnapshotSchema::PrepareForModification(
    const std::shared_ptr<arrow::RecordBatch>& incomingBatch, const NEvWrite::EModificationType mType) const {
    if (!incomingBatch) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("error", "DeserializeBatch() failed");
        return TConclusionStatus::Fail("incorrect incoming batch");
    }
    if (incomingBatch->num_rows() == 0) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("error", "empty batch");
        return TConclusionStatus::Fail("empty incoming batch");
    }

#ifndef NDEBUG
    // its correct (dont check in release) through validation in long tx + validation in kqp streaming
    NArrow::TStatusValidator::Validate(incomingBatch->ValidateFull());
#endif

    const std::shared_ptr<NArrow::TSchemaLite> dstSchema = GetIndexInfo().ArrowSchema();
    std::vector<std::shared_ptr<arrow::Array>> pkColumns;
    pkColumns.resize(GetIndexInfo().GetReplaceKey()->num_fields());
    ui32 pkColumnsCount = 0;
    const auto pred = [&](const ui32 incomingIdx, const i32 targetIdx) {
        if (targetIdx == -1) {
            return TConclusionStatus::Success();
        }
        const std::optional<i32> pkFieldIdx = GetIndexInfo().GetPKColumnIndexByIndexVerified(targetIdx);
        if (!NArrow::HasNulls(incomingBatch->column(incomingIdx))) {
            if (pkFieldIdx) {
                AFL_VERIFY(*pkFieldIdx < (i32)pkColumns.size());
                AFL_VERIFY(!pkColumns[*pkFieldIdx]);
                pkColumns[*pkFieldIdx] = incomingBatch->column(incomingIdx);
                ++pkColumnsCount;
            }
            return TConclusionStatus::Success();
        }
        if (pkFieldIdx) {
            return TConclusionStatus::Fail("null data for pk column is impossible for '" + dstSchema->field(targetIdx)->name() + "'");
        }
        switch (mType) {
            case NEvWrite::EModificationType::Replace:
            case NEvWrite::EModificationType::Insert:
            case NEvWrite::EModificationType::Upsert: {
                if (GetIndexInfo().IsNullableVerifiedByIndex(targetIdx)) {
                    return TConclusionStatus::Success();
                }
                if (GetIndexInfo().GetColumnExternalDefaultValueByIndexVerified(targetIdx)) {
                    return TConclusionStatus::Success();
                } else {
                    return TConclusionStatus::Fail(
                        "empty field for non-default column: '" + dstSchema->field(targetIdx)->name() + "'");
                }
            }
            case NEvWrite::EModificationType::Delete:
            case NEvWrite::EModificationType::Update:
                return TConclusionStatus::Success();
        }
    };
    const auto nameResolver = [&](const std::string& fieldName) -> i32 {
        return GetIndexInfo().GetColumnIndexOptional(fieldName).value_or(-1);
    };
    auto batchConclusion =
        NArrow::TColumnOperator().SkipIfAbsent().ErrorOnDifferentFieldTypes().AdaptIncomingToDestinationExt(incomingBatch, dstSchema, pred, nameResolver);
    if (batchConclusion.IsFail()) {
        return batchConclusion;
    }
    if (pkColumnsCount < pkColumns.size()) {
        return TConclusionStatus::Fail("not enough pk fields");
    }
    auto batch = NArrow::SortBatch(batchConclusion.DetachResult(), pkColumns, true);
    Y_DEBUG_ABORT_UNLESS(NArrow::IsSortedAndUnique(batch, GetIndexInfo().GetPrimaryKey()));
    return batch;
}

void ISnapshotSchema::AdaptBatchToSchema(NArrow::TGeneralContainer& batch, const ISnapshotSchema::TPtr& targetSchema) const {
    if (targetSchema->GetVersion() != GetVersion()) {
        std::vector<ui32> columnIdxToDelete;
        for (size_t columnIdx = 0; columnIdx < batch.GetSchema()->GetFields().size(); ++columnIdx) {
            const std::optional<ui32> targetColumnId = targetSchema->GetColumnIdOptional(batch.GetSchema()->field(columnIdx)->name());
            const ui32 batchColumnId = GetColumnIdVerified(GetFieldByIndex(columnIdx)->name());
            if (!targetColumnId || *targetColumnId != batchColumnId) {
                columnIdxToDelete.emplace_back(columnIdx);
            }
        }
        if (!columnIdxToDelete.empty()) {
            batch.DeleteFieldsByIndex(columnIdxToDelete);
        }
    }
}

ui32 ISnapshotSchema::GetColumnId(const std::string& columnName) const {
    auto id = GetColumnIdOptional(columnName);
    AFL_VERIFY(id)("column_name", columnName)("schema", JoinSeq(",", GetSchema()->field_names()));
    return *id;
}

std::shared_ptr<arrow::Field> ISnapshotSchema::GetFieldByColumnIdVerified(const ui32 columnId) const {
    auto result = GetFieldByColumnIdOptional(columnId);
    AFL_VERIFY(result)("event", "unknown_column")("column_id", columnId)("schema", DebugString());
    return result;
}

std::shared_ptr<NArrow::NAccessor::TColumnLoader> ISnapshotSchema::GetColumnLoaderVerified(const ui32 columnId) const {
    auto result = GetColumnLoaderOptional(columnId);
    AFL_VERIFY(result);
    return result;
}

std::shared_ptr<NArrow::NAccessor::TColumnLoader> ISnapshotSchema::GetColumnLoaderVerified(const std::string& columnName) const {
    auto result = GetColumnLoaderOptional(columnName);
    AFL_VERIFY(result);
    return result;
}

std::shared_ptr<NArrow::NAccessor::TColumnLoader> ISnapshotSchema::GetColumnLoaderOptional(const std::string& columnName) const {
    const std::optional<ui32> id = GetColumnIdOptional(columnName);
    if (id) {
        return GetColumnLoaderOptional(*id);
    } else {
        return nullptr;
    }
}

std::vector<std::string> ISnapshotSchema::GetPKColumnNames() const {
    return GetIndexInfo().GetReplaceKey()->field_names();
}

std::vector<std::shared_ptr<arrow::Field>> ISnapshotSchema::GetAbsentFields(const std::shared_ptr<arrow::Schema>& existsSchema) const {
    std::vector<std::shared_ptr<arrow::Field>> result;
    for (auto&& f : GetIndexInfo().ArrowSchema()->fields()) {
        if (!existsSchema->GetFieldByName(f->name())) {
            result.emplace_back(f);
        }
    }
    return result;
}

TConclusionStatus ISnapshotSchema::CheckColumnsDefault(const std::vector<std::shared_ptr<arrow::Field>>& fields) const {
    for (auto&& i : fields) {
        const ui32 colId = GetColumnIdVerified(i->name());
        auto defaultValue = GetExternalDefaultValueVerified(colId);
        if (!defaultValue && !GetIndexInfo().IsNullableVerified(colId)) {
            return TConclusionStatus::Fail("not nullable field with no default: " + i->name());
        }
    }
    return TConclusionStatus::Success();
}

TConclusion<std::shared_ptr<arrow::RecordBatch>> ISnapshotSchema::BuildDefaultBatch(
    const std::vector<std::shared_ptr<arrow::Field>>& fields, const ui32 rowsCount, const bool force) const {
    std::vector<std::shared_ptr<arrow::Array>> columns;
    for (auto&& i : fields) {
        const ui32 columnId = GetColumnIdVerified(i->name());
        auto defaultValue = GetExternalDefaultValueVerified(columnId);
        if (!defaultValue && !GetIndexInfo().IsNullableVerified(columnId)) {
            if (force) {
                defaultValue = NArrow::DefaultScalar(i->type());
            } else {
                return TConclusionStatus::Fail("not nullable field with no default: " + i->name());
            }
        }
        columns.emplace_back(NArrow::TThreadSimpleArraysCache::Get(i->type(), defaultValue, rowsCount));
    }
    return arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(fields), rowsCount, columns);
}

std::shared_ptr<arrow::Scalar> ISnapshotSchema::GetExternalDefaultValueVerified(const std::string& columnName) const {
    return GetIndexInfo().GetColumnExternalDefaultValueVerified(columnName);
}

std::shared_ptr<arrow::Scalar> ISnapshotSchema::GetExternalDefaultValueVerified(const ui32 columnId) const {
    return GetIndexInfo().GetColumnExternalDefaultValueVerified(columnId);
}

bool ISnapshotSchema::IsSpecialColumnId(const ui32 columnId) const {
    return GetIndexInfo().IsSpecialColumn(columnId);
}

std::set<ui32> ISnapshotSchema::GetColumnsWithDifferentDefaults(
    const THashMap<ui64, ISnapshotSchema::TPtr>& schemas, const ISnapshotSchema::TPtr& targetSchema) {
    std::set<ui32> result;
    if (schemas.size() <= 1) {
        return {};
    }
    std::map<ui32, std::shared_ptr<arrow::Scalar>> defaults;
    for (auto& [_, blobSchema] : schemas) {
        for (auto&& columnId : blobSchema->GetIndexInfo().GetColumnIds(true)) {
            if (result.contains(columnId)) {
                continue;
            }
            if (targetSchema && !targetSchema->HasColumnId(columnId)) {
                continue;
            }
            auto def = blobSchema->GetIndexInfo().GetColumnExternalDefaultValueVerified(columnId);
            if (!blobSchema->GetIndexInfo().IsNullableVerified(columnId) && !def) {
                continue;
            }
            auto it = defaults.find(columnId);
            if (it == defaults.end()) {
                defaults.emplace(columnId, def);
            } else if (NArrow::ScalarCompareNullable(def, it->second) != 0) {
                result.emplace(columnId);
            }
        }
        if (targetSchema && result.size() == targetSchema->GetIndexInfo().GetColumnIds(true).size()) {
            break;
        }
    }
    return result;
}

}
