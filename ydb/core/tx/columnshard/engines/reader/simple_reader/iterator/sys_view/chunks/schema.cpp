#include "metadata.h"
#include "schema.h"

#include <ydb/core/sys_view/common/registry.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NChunks {

std::shared_ptr<ITableMetadataAccessor> TSchemaAdapter::BuildMetadataAccessor(
    const TString& tableName, const NColumnShard::TUnifiedOptionalPathId pathId) const {
    return std::make_shared<TAccessor>(tableName, pathId);
}

NArrow::TSimpleRow TSchemaAdapter::GetPKSimpleRow(
    const NColumnShard::TUnifiedPathId pathId, const ui64 tabletId, const ui64 portionId, const ui32 entityId, const ui64 chunkIdx) {
    NArrow::TSimpleRowViewV0::TWriter writer(sizeof(ui64) * 4 + sizeof(ui32));
    writer.Append<ui64>(pathId.SchemeShardLocalPathId.GetRawValue());
    writer.Append<ui64>(tabletId);
    writer.Append<ui64>(portionId);
    writer.Append<ui32>(entityId);
    writer.Append<ui64>(chunkIdx);
    return NArrow::TSimpleRow(writer.Finish(), GetPKSchema());
}

const std::shared_ptr<arrow20::Schema>& TSchemaAdapter::GetPKSchema() {
    static std::shared_ptr<arrow20::Schema> schema = []() {
        arrow20::FieldVector fields = { std::make_shared<arrow20::Field>("PathId", arrow20::uint64()),
            std::make_shared<arrow20::Field>("TabletId", arrow20::uint64()), std::make_shared<arrow20::Field>("PortionId", arrow20::uint64()),
            std::make_shared<arrow20::Field>("InternalEntityId", arrow20::uint32()), std::make_shared<arrow20::Field>("ChunkIdx", arrow20::uint64()) };
        return std::make_shared<arrow20::Schema>(std::move(fields));
    }();
    return schema;
}

TIndexInfo TSchemaAdapter::GetIndexInfo(
    const std::shared_ptr<IStoragesManager>& storagesManager, const std::shared_ptr<TSchemaObjectsCache>& schemaObjectsCache) const {
    return TBase::GetIndexInfo<NKikimr::NSysView::Schema::PrimaryIndexStats>(storagesManager, schemaObjectsCache);
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NChunks
