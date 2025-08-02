#include "metadata.h"
#include "schema.h"

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NOptimizer {

NArrow::TSimpleRow TSchemaAdapter::GetPKSimpleRow(const NColumnShard::TSchemeShardLocalPathId& pathId, const ui64 tabletId, const ui64 taskId) {
    NArrow::TSimpleRowViewV0::TWriter writer(sizeof(ui64) * 3);
    writer.Append<ui64>(pathId.GetRawValue());
    writer.Append<ui64>(tabletId);
    writer.Append<ui64>(taskId);
    return NArrow::TSimpleRow(writer.Finish(), GetPKSchema());
}

std::shared_ptr<arrow::Schema> TSchemaAdapter::GetPKSchema() {
    static std::shared_ptr<arrow::Schema> schema = []() {
        arrow::FieldVector fields = { std::make_shared<arrow::Field>("PathId", arrow::uint64()),
            std::make_shared<arrow::Field>("TabletId", arrow::uint64()), std::make_shared<arrow::Field>("TaskId", arrow::uint64()) };
        return std::make_shared<arrow::Schema>(std::move(fields));
    }();
    return schema;
}

TIndexInfo TSchemaAdapter::GetIndexInfo(
    const std::shared_ptr<IStoragesManager>& storagesManager, const std::shared_ptr<TSchemaObjectsCache>& schemaObjectsCache) const {
    //PrimaryIndexOptimizerStats

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
        pred("TaskId", NScheme::NTypeIds::Uint64);
        pred("HostName", NScheme::NTypeIds::Utf8);
        pred("NodeId", NScheme::NTypeIds::Uint64);
        pred("Start", NScheme::NTypeIds::Utf8);
        pred("Finish", NScheme::NTypeIds::Utf8);
        pred("Details", NScheme::NTypeIds::Utf8);
        pred("Category", NScheme::NTypeIds::Uint64);
        pred("Weight", NScheme::NTypeIds::Int64);
        
        proto.AddKeyColumnNames("PathId");
        proto.AddKeyColumnNames("TabletId");
        proto.AddKeyColumnNames("TaskId");
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

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NOptimizer
