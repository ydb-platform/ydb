#include "metadata.h"
#include "schema.h"

#include <ydb/core/sys_view/common/registry.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/common/sys_view_schema.h>

namespace NKikimr::NOlap::NReader::NTrivial::NSysView::NGranules {

NArrow::TSimpleRow TSchemaAdapter::GetPKTrivialRow(const NColumnShard::TSchemeShardLocalPathId& pathId, const ui64 tabletId) {
    NArrow::TSimpleRowViewV0::TWriter writer(sizeof(ui64) * 2);
    writer.Append<ui64>(pathId.GetRawValue());
    writer.Append<ui64>(tabletId);
    return NCommon::BuildSysViewPKRow<NKikimr::NSysView::Schema::PrimaryIndexGranuleStats>(writer.Finish());
}

TIndexInfo TSchemaAdapter::GetIndexInfo(
    const std::shared_ptr<IStoragesManager>& storagesManager, const std::shared_ptr<TSchemaObjectsCache>& schemaObjectsCache) const {
    return TBase::GetIndexInfo<NKikimr::NSysView::Schema::PrimaryIndexGranuleStats>(storagesManager, schemaObjectsCache);
}

std::shared_ptr<ITableMetadataAccessor> TSchemaAdapter::BuildMetadataAccessor(
    const TString& tableName, const NColumnShard::TUnifiedOptionalPathId pathId) const {
    return std::make_shared<TAccessor>(tableName, pathId);
}

}   // namespace NKikimr::NOlap::NReader::NTrivial::NSysView::NGranules
