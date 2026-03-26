#include "metadata.h"
#include "schema.h"

#include <ydb/core/sys_view/common/registry.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NSchemas {

NArrow::TSimpleRow TSchemaAdapter::GetPKSimpleRow(const ui64 tabletId, const ui64 presetId, const ui64 schemaVersion) {
    NArrow::TSimpleRowViewV0::TWriter writer(sizeof(ui64) * 2);
    writer.Append<ui64>(tabletId);
    writer.Append<ui64>(presetId);
    writer.Append<ui64>(schemaVersion);
    return NArrow::TSimpleRow(writer.Finish(), GetPKSchema());
}

const std::shared_ptr<arrow20::Schema>& TSchemaAdapter::GetPKSchema() {
    static const std::shared_ptr<arrow20::Schema> schema = []() {
        arrow20::FieldVector fields = { std::make_shared<arrow20::Field>("TabletId", arrow20::uint64()),
            std::make_shared<arrow20::Field>("PresetId", arrow20::uint64()), std::make_shared<arrow20::Field>("SchemaVersion", arrow20::uint64()) };
        return std::make_shared<arrow20::Schema>(std::move(fields));
    }();
    return schema;
}

TIndexInfo TSchemaAdapter::GetIndexInfo(
    const std::shared_ptr<IStoragesManager>& storagesManager, const std::shared_ptr<TSchemaObjectsCache>& schemaObjectsCache) const {
    return TBase::GetIndexInfo<NKikimr::NSysView::Schema::PrimaryIndexSchemaStats>(storagesManager, schemaObjectsCache);
}

std::shared_ptr<ITableMetadataAccessor> TSchemaAdapter::BuildMetadataAccessor(
    const TString& tableName, const NColumnShard::TUnifiedOptionalPathId pathId) const {
    return std::make_shared<TAccessor>(tableName, pathId);
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NSchemas
