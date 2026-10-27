#include "metadata.h"
#include "schema.h"

#include <ydb/core/sys_view/common/registry.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/common/sys_view_schema.h>

namespace NKikimr::NOlap::NReader::NTrivial::NSysView::NPortions {

NArrow::TSimpleRow TSchemaAdapter::GetPKTrivialRow(const NColumnShard::TUnifiedPathId pathId, const ui64 tabletId, const ui64 portionId) {
    NArrow::TSimpleRowViewV0::TWriter writer(sizeof(ui64) * 3);
    writer.Append<ui64>(pathId.SchemeShardLocalPathId.GetRawValue());
    writer.Append<ui64>(tabletId);
    writer.Append<ui64>(portionId);

    return NCommon::BuildSysViewPKRow<NKikimr::NSysView::Schema::PrimaryIndexPortionStats>(writer.Finish());
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

}   // namespace NKikimr::NOlap::NReader::NTrivial::NSysView::NPortions
