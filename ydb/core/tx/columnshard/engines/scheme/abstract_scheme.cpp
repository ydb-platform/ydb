#include "abstract_scheme.h"

#include <ydb/core/tx/columnshard/engines/index_info.h>

namespace NKikimr::NOlap {

std::shared_ptr<arrow::Field> ISnapshotSchema::GetFieldByIndex(const int index) const {
    auto schema = GetSchema();
    if (!schema || index < 0 || index >= schema->num_fields()) {
        return nullptr;
    }
    return schema->field(index);
}
std::shared_ptr<arrow::Field> ISnapshotSchema::GetFieldByColumnId(const ui32 columnId) const {
    return GetFieldByIndex(GetFieldIndex(columnId));
}

std::set<ui32> ISnapshotSchema::GetPkColumnsIds() const {
    std::set<ui32> result;
    for (auto&& field : GetSchema()->fields()) {
        result.emplace(GetColumnId(field->name()));
    }
    return result;

}

std::shared_ptr<arrow::RecordBatch> ISnapshotSchema::NormalizeBatch(const ISnapshotSchema& dataSchema, const std::shared_ptr<arrow::RecordBatch> batch) const {
    if (dataSchema.GetSnapshot() == GetSnapshot()) {
        return batch;
    }
    const std::shared_ptr<arrow::Schema>& resultArrowSchema = GetSchema();
    Y_ABORT_UNLESS(dataSchema.GetSnapshot() < GetSnapshot());
    std::vector<std::shared_ptr<arrow::Array>> newColumns;
    newColumns.reserve(resultArrowSchema->num_fields());

    for (size_t i = 0; i < resultArrowSchema->fields().size(); ++i) {
        auto& resultField = resultArrowSchema->fields()[i];
        auto columnId = GetIndexInfo().GetColumnId(resultField->name());
        auto oldColumnIndex = dataSchema.GetFieldIndex(columnId);
        if (oldColumnIndex >= 0) { // ColumnExists
            auto oldColumnInfo = dataSchema.GetFieldByIndex(oldColumnIndex);
            Y_ABORT_UNLESS(oldColumnInfo);
            auto columnData = batch->GetColumnByName(oldColumnInfo->name());
            Y_ABORT_UNLESS(columnData);
            newColumns.push_back(columnData);
        } else { // AddNullColumn
            auto nullColumn = NArrow::MakeEmptyBatch(arrow::schema({resultField}), batch->num_rows());
            newColumns.push_back(nullColumn->column(0));
        }
    }
    return arrow::RecordBatch::Make(resultArrowSchema, batch->num_rows(), newColumns);
}

std::shared_ptr<arrow::RecordBatch> ISnapshotSchema::PrepareForInsert(const TString& data, const std::shared_ptr<arrow::Schema>& dataSchema) const {
    std::shared_ptr<arrow::Schema> dstSchema = GetIndexInfo().ArrowSchema();
    auto batch = NArrow::DeserializeBatch(data, (dataSchema ? dataSchema : dstSchema));
    if (!batch) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("error", "DeserializeBatch() failed");
        return nullptr;
    }
    if (batch->num_rows() == 0) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("error", "empty batch");
        return nullptr;
    }

    // Correct schema
    if (dataSchema) {
        batch = NArrow::ExtractColumns(batch, dstSchema, true);
        if (!batch) {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("error", "cannot correct schema");
            return nullptr;
        }
    }

    if (!batch->schema()->Equals(dstSchema)) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("error", TStringBuilder() << "unexpected schema for insert batch: '" << batch->schema()->ToString() << "'");
        return nullptr;
    }

    const auto& sortingKey = GetIndexInfo().GetSortingKey();
    Y_ABORT_UNLESS(sortingKey);

    // Check PK is NOT NULL
    for (auto& field : sortingKey->fields()) {
        auto column = batch->GetColumnByName(field->name());
        if (!column) {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("error", TStringBuilder() << "missing PK column '" << field->name() << "'");
            return nullptr;
        }
        if (NArrow::HasNulls(column)) {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("error", TStringBuilder() << "PK column '" << field->name() << "' contains NULLs");
            return nullptr;
        }
    }

    auto status = batch->ValidateFull();
    if (!status.ok()) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("error", status.ToString());
        return nullptr;
    }
    batch = NArrow::SortBatch(batch, sortingKey, true);
    Y_DEBUG_ABORT_UNLESS(NArrow::IsSortedAndUnique(batch, sortingKey));
    return batch;
}

}
