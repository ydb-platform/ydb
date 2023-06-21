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

std::shared_ptr<arrow::RecordBatch> ISnapshotSchema::NormalizeBatch(const ISnapshotSchema& dataSchema, const std::shared_ptr<arrow::RecordBatch> batch) const {
    if (dataSchema.GetSnapshot() == GetSnapshot()) {
        return batch;
    }
    const std::shared_ptr<arrow::Schema>& resultArrowSchema = GetSchema();
    Y_VERIFY(dataSchema.GetSnapshot() < GetSnapshot());
    std::vector<std::shared_ptr<arrow::Array>> newColumns;
    newColumns.reserve(resultArrowSchema->num_fields());

    for (size_t i = 0; i < resultArrowSchema->fields().size(); ++i) {
        auto& resultField = resultArrowSchema->fields()[i];
        auto columnId = GetIndexInfo().GetColumnId(resultField->name());
        auto oldColumnIndex = dataSchema.GetFieldIndex(columnId);
        if (oldColumnIndex >= 0) { // ClumnExists
            auto oldColumnInfo = dataSchema.GetFieldByIndex(oldColumnIndex);
            Y_VERIFY(oldColumnInfo);
            auto columnData = batch->GetColumnByName(oldColumnInfo->name());
            Y_VERIFY(columnData);
            newColumns.push_back(columnData);
        } else { // AddNullColumn
            auto nullColumn = NArrow::MakeEmptyBatch(arrow::schema({resultField}), batch->num_rows());
            newColumns.push_back(nullColumn->column(0));
        }
    }
    return arrow::RecordBatch::Make(resultArrowSchema, batch->num_rows(), newColumns);
}

std::shared_ptr<arrow::RecordBatch> ISnapshotSchema::PrepareForInsert(const TString& data, const TString& dataSchemaStr, TString& strError) const {
    std::shared_ptr<arrow::Schema> dstSchema = GetIndexInfo().ArrowSchema();
    std::shared_ptr<arrow::Schema> dataSchema;
    if (dataSchemaStr.size()) {
        dataSchema = NArrow::DeserializeSchema(dataSchemaStr);
        if (!dataSchema) {
            strError = "DeserializeSchema() failed";
            return nullptr;
        }
    }

    auto batch = NArrow::DeserializeBatch(data, (dataSchema ? dataSchema : dstSchema));
    if (!batch) {
        strError = "DeserializeBatch() failed";
        return nullptr;
    }
    if (batch->num_rows() == 0) {
        strError = "empty batch";
        return nullptr;
    }

    // Correct schema
    if (dataSchema) {
        batch = NArrow::ExtractColumns(batch, dstSchema, true);
        if (!batch) {
            strError = "cannot correct schema";
            return nullptr;
        }
    }

    if (!batch->schema()->Equals(dstSchema)) {
        strError = "unexpected schema for insert batch: '" + batch->schema()->ToString() + "'";
        return nullptr;
    }

    const auto& sortingKey = GetIndexInfo().GetSortingKey();
    Y_VERIFY(sortingKey);

    // Check PK is NOT NULL
    for (auto& field : sortingKey->fields()) {
        auto column = batch->GetColumnByName(field->name());
        if (!column) {
            strError = "missing PK column '" + field->name() + "'";
            return nullptr;
        }
        if (NArrow::HasNulls(column)) {
            strError = "PK column '" + field->name() + "' contains NULLs";
            return nullptr;
        }
    }

    auto status = batch->ValidateFull();
    if (!status.ok()) {
        strError = status.ToString();
        return nullptr;
    }
    batch = NArrow::SortBatch(batch, sortingKey);
    Y_VERIFY_DEBUG(NArrow::IsSorted(batch, sortingKey));
    return batch;
}

}
