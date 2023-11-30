#include "filtered_scheme.h"
#include <util/string/join.h>


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

TFilteredSnapshotSchema::TFilteredSnapshotSchema(ISnapshotSchema::TPtr originalSnapshot, const std::set<std::string>& columnNames)
    : OriginalSnapshot(originalSnapshot) {
    for (auto&& i : columnNames) {
        ColumnIds.emplace(OriginalSnapshot->GetColumnId(i));
    }
    std::vector<std::shared_ptr<arrow::Field>> schemaFields;
    for (auto&& i : OriginalSnapshot->GetSchema()->fields()) {
        if (!columnNames.contains(i->name())) {
            continue;
        }
        schemaFields.emplace_back(i);
    }
    Schema = std::make_shared<arrow::Schema>(schemaFields);
}

TColumnSaver TFilteredSnapshotSchema::GetColumnSaver(const ui32 columnId, const TSaverContext& context) const {
    Y_ABORT_UNLESS(ColumnIds.contains(columnId));
    return OriginalSnapshot->GetColumnSaver(columnId, context);
}

std::shared_ptr<TColumnLoader> TFilteredSnapshotSchema::GetColumnLoader(const ui32 columnId) const {
    Y_ABORT_UNLESS(ColumnIds.contains(columnId));
    return OriginalSnapshot->GetColumnLoader(columnId);
}

std::optional<ui32> TFilteredSnapshotSchema::GetColumnIdOptional(const std::string& columnName) const {
    return OriginalSnapshot->GetColumnIdOptional(columnName);
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
