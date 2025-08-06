#include "metadata.h"
#include "schema.h"

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NSchemas {

NArrow::TSimpleRowContent TSchemaAdapter::GetPKSimpleRow(const ui64 tabletId, const ui64 presetId, const ui64 schemaVersion) {
    NArrow::TSimpleRowViewV0::TWriter writer(sizeof(ui64) * 2 + 16);
    writer.Append<ui64>(tabletId);
    writer.Append<ui64>(presetId);
    writer.Append<ui64>(schemaVersion);
    return NArrow::TSimpleRowContent(writer.Finish());
}

const std::shared_ptr<arrow::Schema>& TSchemaAdapter::GetPKSchema() {
    static const std::shared_ptr<arrow::Schema> schema = []() {
        arrow::FieldVector fields = { std::make_shared<arrow::Field>("TabletId", arrow::uint64()),
            std::make_shared<arrow::Field>("PresetId", arrow::uint64()), std::make_shared<arrow::Field>("SchemaVersion", arrow::uint64()) };
        return std::make_shared<arrow::Schema>(std::move(fields));
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
