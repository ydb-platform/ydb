#include "metadata.h"
#include "schema.h"

#include <ydb/core/sys_view/common/registry.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NPortions {

NArrow::TSimpleRow TSchemaAdapter::GetPKSimpleRow(const NColumnShard::TUnifiedPathId pathId, const ui64 tabletId, const ui64 portionId) {
    NArrow::TSimpleRowViewV0::TWriter writer(sizeof(ui64) * 3);
    writer.Append<ui64>(pathId.SchemeShardLocalPathId.GetRawValue());
    writer.Append<ui64>(tabletId);
    writer.Append<ui64>(portionId);

    return NArrow::TSimpleRow(writer.Finish(), GetPKSchema());
}

const std::shared_ptr<arrow20::Schema>& TSchemaAdapter::GetPKSchema() {
    static std::shared_ptr<arrow20::Schema> schema = []() {
        arrow20::FieldVector fields = { std::make_shared<arrow20::Field>("PathId", arrow20::uint64()),
            std::make_shared<arrow20::Field>("TabletId", arrow20::uint64()), std::make_shared<arrow20::Field>("PortionId", arrow20::uint64()) };
        return std::make_shared<arrow20::Schema>(std::move(fields));
    }();
    return schema;
}

TIndexInfo TSchemaAdapter::GetIndexInfo(
    const std::shared_ptr<IStoragesManager>& storagesManager, const std::shared_ptr<TSchemaObjectsCache>& schemaObjectsCache) const {
    return TBase::GetIndexInfo<NKikimr::NSysView::Schema::PrimaryIndexPortionStats>(storagesManager, schemaObjectsCache);
}

std::shared_ptr<ITableMetadataAccessor> TSchemaAdapter::BuildMetadataAccessor(
    const TString& tableName, const NColumnShard::TUnifiedOptionalPathId pathId) const {
    return std::make_shared<TAccessor>(tableName, pathId);
}

NTable::TScheme::TTableSchema TSchemaAdapter::GetStatsSchema() {
    NTable::TScheme::TTableSchema schema;
    NIceDb::NHelpers::TStaticSchemaFiller<NKikimr::NSysView::Schema::PrimaryIndexPortionStats>::Fill(schema);
    return schema;
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NPortions
