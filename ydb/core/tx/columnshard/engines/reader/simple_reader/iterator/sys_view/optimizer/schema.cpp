#include "metadata.h"
#include "schema.h"

#include <ydb/core/sys_view/common/registry.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/common/sys_view_schema.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NOptimizer {

NArrow::TSimpleRow TSchemaAdapter::GetPKSimpleRow(const NColumnShard::TSchemeShardLocalPathId& pathId, const ui64 tabletId, const ui64 taskId) {
    NArrow::TSimpleRowViewV0::TWriter writer(sizeof(ui64) * 3);
    writer.Append<ui64>(pathId.GetRawValue());
    writer.Append<ui64>(tabletId);
    writer.Append<ui64>(taskId);
    return NCommon::BuildSysViewPKRow<NKikimr::NSysView::Schema::PrimaryIndexOptimizerStats>(writer.Finish());
}

TIndexInfo TSchemaAdapter::GetIndexInfo(
    const std::shared_ptr<IStoragesManager>& storagesManager, const std::shared_ptr<TSchemaObjectsCache>& schemaObjectsCache) const {
    return TBase::GetIndexInfo<NKikimr::NSysView::Schema::PrimaryIndexOptimizerStats>(storagesManager, schemaObjectsCache);
}

std::shared_ptr<ITableMetadataAccessor> TSchemaAdapter::BuildMetadataAccessor(
    const TString& tableName, const NColumnShard::TUnifiedOptionalPathId pathId) const {
    return std::make_shared<TAccessor>(tableName, pathId);
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NOptimizer
