#include "snapshot_scheme.h"

namespace NKikimr::NOlap {

TSnapshotSchema::TSnapshotSchema(TIndexInfo&& indexInfo, const TSnapshot& snapshot)
    : IndexInfo(std::move(indexInfo))
    , Schema(IndexInfo.ArrowSchemaWithSpecials())
    , Snapshot(snapshot)
{
}

TColumnSaver TSnapshotSchema::GetColumnSaver(const ui32 columnId, const TSaverContext& context) const {
    return IndexInfo.GetColumnSaver(columnId, context);
}

std::shared_ptr<TColumnLoader> TSnapshotSchema::GetColumnLoader(const ui32 columnId) const {
    return IndexInfo.GetColumnLoader(columnId);
}

ui32 TSnapshotSchema::GetColumnId(const std::string& columnName) const {
    return IndexInfo.GetColumnId(columnName);
}

int TSnapshotSchema::GetFieldIndex(const ui32 columnId) const {
    const TString& columnName = IndexInfo.GetColumnName(columnId, false);
    if (!columnName) {
        return -1;
    }
    std::string name(columnName.data(), columnName.size());
    return Schema->GetFieldIndex(name);
}

const std::shared_ptr<arrow::Schema>& TSnapshotSchema::GetSchema() const {
    return Schema;
}

const TIndexInfo& TSnapshotSchema::GetIndexInfo() const {
    return IndexInfo;
}

const TSnapshot& TSnapshotSchema::GetSnapshot() const {
    return Snapshot;
}

ui32 TSnapshotSchema::GetColumnsCount() const {
    return Schema->num_fields();
}

}
