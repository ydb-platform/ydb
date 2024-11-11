#include "schema.h"

namespace NKikimr::NOlap {

TSchema::TSchema(TIndexInfo&& indexInfo, const TSnapshot& snapshot)
    : IndexInfo(std::move(indexInfo))
    , Schema(IndexInfo.ArrowSchemaWithSpecials())
    , Snapshot(snapshot)
{
}

TColumnSaver TSchema::GetColumnSaver(const ui32 columnId) const {
    return IndexInfo.GetColumnSaver(columnId);
}

std::shared_ptr<TColumnLoader> TSchema::GetColumnLoaderOptional(const ui32 columnId) const {
    return IndexInfo.GetColumnLoaderOptional(columnId);
}

std::optional<ui32> TSchema::GetColumnIdOptional(const std::string& columnName) const {
    return IndexInfo.GetColumnIdOptional(columnName);
}

ui32 TSchema::GetColumnIdVerified(const std::string& columnName) const {
    return IndexInfo.GetColumnIdVerified(columnName);
}

int TSchema::GetFieldIndex(const ui32 columnId) const {
    return IndexInfo.GetColumnIndexOptional(columnId).value_or(-1);
}

const std::shared_ptr<NArrow::TSchemaLite>& TSchema::GetSchema() const {
    return Schema;
}

const TIndexInfo& TSchema::GetIndexInfo() const {
    return IndexInfo;
}

const TSnapshot& TSchema::GetSnapshot() const {
    return Snapshot;
}

ui32 TSchema::GetColumnsCount() const {
    return Schema->num_fields();
}

ui64 TSchema::GetVersion() const {
    return IndexInfo.GetVersion();
}

}
