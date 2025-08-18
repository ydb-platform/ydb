#include "metadata.h"
#include "schema.h"

#include <ydb/core/sys_view/common/registry.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NGranules {

NArrow::TSimpleRow TSchemaAdapter::GetPKSimpleRow(const NColumnShard::TSchemeShardLocalPathId& pathId, const ui64 tabletId) {
    NArrow::TSimpleRowViewV0::TWriter writer(sizeof(ui64) * 2);
    writer.Append<ui64>(pathId.GetRawValue());
    writer.Append<ui64>(tabletId);
    return NArrow::TSimpleRow(writer.Finish(), GetPKSchema());
}

const std::shared_ptr<arrow::Schema>& TSchemaAdapter::GetPKSchema() {
    static std::shared_ptr<arrow::Schema> schema = []() {
        arrow::FieldVector fields = {
            std::make_shared<arrow::Field>("PathId", arrow::uint64()),
            std::make_shared<arrow::Field>("TabletId", arrow::uint64()) };
        return std::make_shared<arrow::Schema>(std::move(fields));
    }();
    return schema;
}

TIndexInfo TSchemaAdapter::GetIndexInfo(
    const std::shared_ptr<IStoragesManager>& storagesManager, const std::shared_ptr<TSchemaObjectsCache>& schemaObjectsCache) const {
    return TBase::GetIndexInfo<NKikimr::NSysView::Schema::PrimaryIndexGranuleStats>(storagesManager, schemaObjectsCache);
}

std::shared_ptr<ITableMetadataAccessor> TSchemaAdapter::BuildMetadataAccessor(
    const TString& tableName, const NColumnShard::TUnifiedOptionalPathId pathId) const {
    return std::make_shared<TAccessor>(tableName, pathId);
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NGranules
