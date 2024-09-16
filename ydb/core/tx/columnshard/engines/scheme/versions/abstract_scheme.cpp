#include "abstract_scheme.h"

#include <ydb/core/tx/columnshard/engines/index_info.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/simple_arrays_cache.h>
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
    const std::shared_ptr<arrow::Schema>& resultArrowSchema = GetSchema();

    std::shared_ptr<NArrow::TGeneralContainer> result = std::make_shared<NArrow::TGeneralContainer>(batch->GetRecordsCount());
    for (size_t i = 0; i < resultArrowSchema->fields().size(); ++i) {
        auto& resultField = resultArrowSchema->fields()[i];
        auto columnId = GetIndexInfo().GetColumnId(resultField->name());
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
            result->AddField(resultField,
                    NArrow::TThreadSimpleArraysCache::Get(resultField->type(), GetExternalDefaultValueVerified(columnId), batch->num_rows()))
                .Validate();
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

    auto status = incomingBatch->ValidateFull();
    if (!status.ok()) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("error", status.ToString());
        return TConclusionStatus::Fail("not valid incoming batch: " + status.ToString());
    }

    const std::shared_ptr<arrow::Schema> dstSchema = GetIndexInfo().ArrowSchema();

    auto batch = NArrow::TColumnOperator().SkipIfAbsent().Extract(incomingBatch, dstSchema->field_names());

    for (auto&& i : batch->schema()->fields()) {
        AFL_VERIFY(GetIndexInfo().HasColumnName(i->name()));
        if (!dstSchema->GetFieldByName(i->name())->Equals(i)) {
            return TConclusionStatus::Fail("not equal field types for column '" + i->name() + "'");
        }
        if (GetIndexInfo().IsNullableVerified(i->name())) {
            continue;
        }
        if (NArrow::HasNulls(batch->GetColumnByName(i->name()))) {
            return TConclusionStatus::Fail("null data for not nullable column '" + i->name() + "'");
        }
    }

    AFL_VERIFY(GetIndexInfo().GetPrimaryKey());

    // Check PK is NOT NULL
    for (auto& field : GetIndexInfo().GetPrimaryKey()->fields()) {
        auto column = batch->GetColumnByName(field->name());
        if (!column) {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("error", TStringBuilder() << "missing PK column '" << field->name() << "'");
            return TConclusionStatus::Fail("missing PK column: '" + field->name() + "'");
        }
        if (NArrow::HasNulls(column)) {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("error", TStringBuilder() << "PK column '" << field->name() << "' contains NULLs");
            return TConclusionStatus::Fail(TStringBuilder() << "PK column '" << field->name() << "' contains NULLs");
        }
    }

    batch = NArrow::SortBatch(batch, GetIndexInfo().GetPrimaryKey(), true);
    Y_DEBUG_ABORT_UNLESS(NArrow::IsSortedAndUnique(batch, GetIndexInfo().GetPrimaryKey()));

    switch (mType) {
        case NEvWrite::EModificationType::Replace:
        case NEvWrite::EModificationType::Upsert: {
                AFL_VERIFY(batch->num_columns() <= dstSchema->num_fields());
            if (batch->num_columns() < dstSchema->num_fields()) {
                for (auto&& f : dstSchema->fields()) {
                    if (GetIndexInfo().IsNullableVerified(f->name())) {
                        continue;
                    }
                    if (batch->GetColumnByName(f->name())) {
                        continue;
                    }
                    if (!GetIndexInfo().GetColumnExternalDefaultValueVerified(f->name())) {
                        return TConclusionStatus::Fail("empty field for non-default column: '" + f->name() + "'");
                    }
                }
            }
            return batch;
        }
        case NEvWrite::EModificationType::Delete:
        case NEvWrite::EModificationType::Insert:
        case NEvWrite::EModificationType::Update:
            return batch;
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

std::shared_ptr<NKikimr::NOlap::TColumnLoader> ISnapshotSchema::GetColumnLoaderVerified(const ui32 columnId) const {
    auto result = GetColumnLoaderOptional(columnId);
    AFL_VERIFY(result);
    return result;
}

std::shared_ptr<NKikimr::NOlap::TColumnLoader> ISnapshotSchema::GetColumnLoaderVerified(const std::string& columnName) const {
    auto result = GetColumnLoaderOptional(columnName);
    AFL_VERIFY(result);
    return result;
}

std::shared_ptr<NKikimr::NOlap::TColumnLoader> ISnapshotSchema::GetColumnLoaderOptional(const std::string& columnName) const {
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
        auto defaultValue = GetExternalDefaultValueVerified(i->name());
        if (!defaultValue && !GetIndexInfo().IsNullableVerified(i->name())) {
            return TConclusionStatus::Fail("not nullable field with no default: " + i->name());
        }
    }
    return TConclusionStatus::Success();
}

TConclusion<std::shared_ptr<arrow::RecordBatch>> ISnapshotSchema::BuildDefaultBatch(
    const std::vector<std::shared_ptr<arrow::Field>>& fields, const ui32 rowsCount, const bool force) const {
    std::vector<std::shared_ptr<arrow::Array>> columns;
    for (auto&& i : fields) {
        auto defaultValue = GetExternalDefaultValueVerified(i->name());
        if (!defaultValue && !GetIndexInfo().IsNullableVerified(i->name())) {
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
