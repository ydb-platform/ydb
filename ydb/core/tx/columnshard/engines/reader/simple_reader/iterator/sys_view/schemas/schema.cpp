#include "metadata.h"
#include "schema.h"

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NSchemas {

NArrow::TSimpleRow TSchemaAdapter::GetPKSimpleRow(const ui64 tabletId, const ui64 presetId, const ui64 schemaVersion) {
    NArrow::TSimpleRowViewV0::TWriter writer(sizeof(ui64) * 2);
    writer.Append<ui64>(tabletId);
    writer.Append<ui64>(presetId);
    writer.Append<ui64>(schemaVersion);
    return NArrow::TSimpleRow(writer.Finish(), GetPKSchema());
}

std::shared_ptr<arrow::Schema> TSchemaAdapter::GetPKSchema() {
    static std::shared_ptr<arrow::Schema> schema = []() {
        arrow::FieldVector fields = { std::make_shared<arrow::Field>("TabletId", arrow::uint64()),
            std::make_shared<arrow::Field>("PresetId", arrow::uint64()), std::make_shared<arrow::Field>("SchemaVersion", arrow::uint64()) };
        return std::make_shared<arrow::Schema>(std::move(fields));
    }();
    return schema;
}

TIndexInfo TSchemaAdapter::GetIndexInfo(
    const std::shared_ptr<IStoragesManager>& storagesManager, const std::shared_ptr<TSchemaObjectsCache>& schemaObjectsCache) const {
    //PrimaryIndexSchemaStats

    static NKikimrSchemeOp::TColumnTableSchema proto = []() {
        NKikimrSchemeOp::TColumnTableSchema proto;
        ui32 currentId = 0;
        const auto pred = [&](const TString& name, const NScheme::TTypeId typeId, const std::optional<ui32> entityId = std::nullopt) {
            auto* col = proto.AddColumns();
            col->SetId(entityId.value_or(++currentId));
            col->SetName(name);
            col->SetTypeId(typeId);
        };
        pred("TabletId", NScheme::NTypeIds::Uint64);
        pred("PresetId", NScheme::NTypeIds::Uint64);
        pred("SchemaVersion", NScheme::NTypeIds::Uint64);
        pred("SchemaSnapshotPlanStep", NScheme::NTypeIds::Uint64);
        pred("SchemaSnapshotTxId", NScheme::NTypeIds::Uint64);
        pred("SchemaDetails", NScheme::NTypeIds::Utf8);

        proto.AddKeyColumnNames("TabletId");
        proto.AddKeyColumnNames("PresetId");
        proto.AddKeyColumnNames("SchemaVersion");
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

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NSchemas
