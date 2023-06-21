#include "filtered_scheme.h"


namespace NKikimr::NOlap {

TFilteredSnapshotSchema::TFilteredSnapshotSchema(ISnapshotSchema::TPtr originalSnapshot, const std::vector<ui32>& columnIds)
    : TFilteredSnapshotSchema(originalSnapshot, std::set(columnIds.begin(), columnIds.end()))
{}

TFilteredSnapshotSchema::TFilteredSnapshotSchema(ISnapshotSchema::TPtr originalSnapshot, const std::set<ui32>& columnIds)
    : OriginalSnapshot(originalSnapshot)
    , ColumnIds(columnIds)
{
    std::vector<std::shared_ptr<arrow::Field>> schemaFields;
    for (auto&& i : OriginalSnapshot->GetSchema()->fields()) {
        if (!ColumnIds.contains(OriginalSnapshot->GetIndexInfo().GetColumnId(i->name()))) {
            continue;
        }
        schemaFields.emplace_back(i);
    }
    Schema = std::make_shared<arrow::Schema>(schemaFields);
}

TColumnSaver TFilteredSnapshotSchema::GetColumnSaver(const ui32 columnId, const TSaverContext& context) const {
    Y_VERIFY(ColumnIds.contains(columnId));
    return OriginalSnapshot->GetColumnSaver(columnId, context);
}

std::shared_ptr<TColumnLoader> TFilteredSnapshotSchema::GetColumnLoader(const ui32 columnId) const {
    Y_VERIFY(ColumnIds.contains(columnId));
    return OriginalSnapshot->GetColumnLoader(columnId);
}

ui32 TFilteredSnapshotSchema::GetColumnId(const std::string& columnName) const {
    return OriginalSnapshot->GetColumnId(columnName);
}

int TFilteredSnapshotSchema::GetFieldIndex(const ui32 columnId) const {
    if (!ColumnIds.contains(columnId)) {
        return -1;
    }
    TString columnName = OriginalSnapshot->GetIndexInfo().GetColumnName(columnId, false);
    if (!columnName) {
        return -1;
    }
    std::string name(columnName.data(), columnName.size());
    return Schema->GetFieldIndex(name);
}

const std::shared_ptr<arrow::Schema>& TFilteredSnapshotSchema::GetSchema() const {
    return Schema;
}

const TIndexInfo& TFilteredSnapshotSchema::GetIndexInfo() const {
    return OriginalSnapshot->GetIndexInfo();
}

const TSnapshot& TFilteredSnapshotSchema::GetSnapshot() const {
    return OriginalSnapshot->GetSnapshot();
}

}
