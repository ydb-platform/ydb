#include "snapshot_scheme.h"

namespace NKikimr::NOlap {

TSnapshotSchema::TSnapshotSchema(TIndexInfo&& indexInfo, const TSnapshot& snapshot)
    : IndexInfo(std::move(indexInfo))
    , Schema(IndexInfo.ArrowSchemaWithSpecials())
    , Snapshot(snapshot)
{
}

TColumnSaver TSnapshotSchema::GetColumnSaver(const ui32 columnId) const {
    return IndexInfo.GetColumnSaver(columnId);
}

std::shared_ptr<TColumnLoader> TSnapshotSchema::GetColumnLoaderOptional(const ui32 columnId) const {
    return IndexInfo.GetColumnLoaderOptional(columnId);
}

std::optional<ui32> TSnapshotSchema::GetColumnIdOptional(const std::string& columnName) const {
    return IndexInfo.GetColumnIdOptional(columnName);
}

ui32 TSnapshotSchema::GetColumnIdVerified(const std::string& columnName) const {
    return IndexInfo.GetColumnIdVerified(columnName);
}

int TSnapshotSchema::GetFieldIndex(const ui32 columnId) const {
    return IndexInfo.GetColumnIndexOptional(columnId).value_or(-1);
}

const std::shared_ptr<NArrow::TSchemaLite>& TSnapshotSchema::GetSchema() const {
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

ui64 TSnapshotSchema::GetVersion() const {
    return IndexInfo.GetVersion();
}

}
