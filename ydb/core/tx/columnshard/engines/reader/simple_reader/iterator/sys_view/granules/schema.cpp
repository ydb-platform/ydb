#include "metadata.h"
#include "schema.h"

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NGranules {

NArrow::TSimpleRow TSchemaAdapter::GetPKSimpleRow(const NColumnShard::TSchemeShardLocalPathId& pathId, const ui64 tabletId) {
    NArrow::TSimpleRowViewV0::TWriter writer(sizeof(ui64) * 2);
    writer.Append<ui64>(pathId.GetRawValue());
    writer.Append<ui64>(tabletId);
    return NArrow::TSimpleRow(writer.Finish(), GetPKSchema());
}

std::shared_ptr<arrow::Schema> TSchemaAdapter::GetPKSchema() {
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
    //PrimaryIndexGranuleStats

    static NKikimrSchemeOp::TColumnTableSchema proto = []() {
        NKikimrSchemeOp::TColumnTableSchema proto;
        ui32 currentId = 0;
        const auto pred = [&](const TString& name, const NScheme::TTypeId typeId, const std::optional<ui32> entityId = std::nullopt) {
            auto* col = proto.AddColumns();
            col->SetId(entityId.value_or(++currentId));
            col->SetName(name);
            col->SetTypeId(typeId);
        };
        pred("PathId", NScheme::NTypeIds::Uint64);
        pred("TabletId", NScheme::NTypeIds::Uint64);
        pred("PortionsCount", NScheme::NTypeIds::Uint64);
        pred("HostName", NScheme::NTypeIds::Utf8);
        pred("NodeId", NScheme::NTypeIds::Uint64);
        pred("InternalPathId", NScheme::NTypeIds::Uint64);

        proto.AddKeyColumnNames("PathId");
        proto.AddKeyColumnNames("TabletId");
        return proto;
    }();

    auto indexInfo = TIndexInfo::BuildFromProto(GetPresetId(), proto, storagesManager, schemaObjectsCache);
    AFL_VERIFY(indexInfo);
    return std::move(*indexInfo);
}

std::shared_ptr<ITableMetadataAccessor> TSchemaAdapter::BuildMetadataAccessor(const TString& tableName,
    const NColumnShard::TSchemeShardLocalPathId externalPathId, const std::optional<NColumnShard::TInternalPathId> internalPathId) const {
    return std::make_shared<TAccessor>(tableName, externalPathId, internalPathId);
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NGranules
