#include "metadata.h"
#include "schema.h"

#include <ydb/core/sys_view/common/registry.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/common/sys_view_schema.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NSchemas {

NArrow::TSimpleRow TSchemaAdapter::GetPKSimpleRow(const ui64 tabletId, const ui64 presetId, const ui64 schemaVersion) {
    NArrow::TSimpleRowViewV0::TWriter writer(sizeof(ui64) * 2);
    writer.Append<ui64>(tabletId);
    writer.Append<ui64>(presetId);
    writer.Append<ui64>(schemaVersion);
    return NCommon::BuildSysViewPKRow<NKikimr::NSysView::Schema::PrimaryIndexSchemaStats>(writer.Finish());
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
