#include "filtered_scheme.h"
#include <util/string/join.h>


namespace NKikimr::NOlap {

TFilteredSnapshotSchema::TFilteredSnapshotSchema(const ISnapshotSchema::TPtr& originalSnapshot, const std::set<ui32>& columnIds)
    : TFilteredSnapshotSchema(originalSnapshot, std::vector(columnIds.begin(), columnIds.end())) {
}

TFilteredSnapshotSchema::TFilteredSnapshotSchema(const ISnapshotSchema::TPtr& originalSnapshot, const std::vector<ui32>& columnIds)
    : OriginalSnapshot(originalSnapshot)
    , ColumnIds(columnIds)
{
    std::vector<std::shared_ptr<arrow::Field>> schemaFields;
    for (auto&& i : columnIds) {
        IdIntoIndex.emplace(i, schemaFields.size());
        schemaFields.emplace_back(originalSnapshot->GetFieldByColumnIdVerified(i));
    }
    Schema = std::make_shared<NArrow::TSchemaLite>(schemaFields);
}

TColumnSaver TFilteredSnapshotSchema::GetColumnSaver(const ui32 columnId) const {
    AFL_VERIFY(std::find(ColumnIds.begin(), ColumnIds.end(), columnId) != ColumnIds.end());
    return OriginalSnapshot->GetColumnSaver(columnId);
}

std::shared_ptr<TColumnLoader> TFilteredSnapshotSchema::GetColumnLoaderOptional(const ui32 columnId) const {
    AFL_VERIFY(std::find(ColumnIds.begin(), ColumnIds.end(), columnId) != ColumnIds.end());
    return OriginalSnapshot->GetColumnLoaderOptional(columnId);
}

std::optional<ui32> TFilteredSnapshotSchema::GetColumnIdOptional(const std::string& columnName) const {
    auto result = OriginalSnapshot->GetColumnIdOptional(columnName);
    if (!result) {
        return result;
    }
    if (std::find(ColumnIds.begin(), ColumnIds.end(), *result) == ColumnIds.end()) {
        return std::nullopt;
    }
    return result;
}

ui32 TFilteredSnapshotSchema::GetColumnIdVerified(const std::string& columnName) const {
    auto result = OriginalSnapshot->GetColumnIdVerified(columnName);
    AFL_VERIFY(std::find(ColumnIds.begin(), ColumnIds.end(), result) != ColumnIds.end());
    return result;
}

int TFilteredSnapshotSchema::GetFieldIndex(const ui32 columnId) const {
    auto it = IdIntoIndex.find(columnId);
    if (it == IdIntoIndex.end()) {
        return -1;
    }
    return it->second;
}

const std::shared_ptr<NArrow::TSchemaLite>& TFilteredSnapshotSchema::GetSchema() const {
    return Schema;
}

const TIndexInfo& TFilteredSnapshotSchema::GetIndexInfo() const {
    return OriginalSnapshot->GetIndexInfo();
}

const TSnapshot& TFilteredSnapshotSchema::GetSnapshot() const {
    return OriginalSnapshot->GetSnapshot();
}

ui32 TFilteredSnapshotSchema::GetColumnsCount() const {
    return Schema->num_fields();
}

ui64 TFilteredSnapshotSchema::GetVersion() const {
    return OriginalSnapshot->GetIndexInfo().GetVersion();
}

TString TFilteredSnapshotSchema::DoDebugString() const {
    return TStringBuilder() << "("
        << "original=" << OriginalSnapshot->DebugString() << ";"
        << "column_ids=[" << JoinSeq(",", ColumnIds) << "];"
        << ")"
        ;
}

}
