#include "metadata.h"
#include "schema.h"

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NPortions {

NArrow::TSimpleRow TSchemaAdapter::GetPKSimpleRow(const NColumnShard::TUnifiedPathId pathId, const ui64 tabletId, const ui64 portionId) {
    NArrow::TSimpleRowViewV0::TWriter writer(sizeof(ui64) * 3);
    writer.Append<ui64>(pathId.SchemeShardLocalPathId.GetRawValue());
    writer.Append<ui64>(tabletId);
    writer.Append<ui64>(portionId);

    return NArrow::TSimpleRow(writer.Finish(), GetPKSchema());
}

std::shared_ptr<arrow::Schema> TSchemaAdapter::GetPKSchema() {
    static std::shared_ptr<arrow::Schema> schema = []() {
        arrow::FieldVector fields = { std::make_shared<arrow::Field>("PathId", arrow::uint64()),
            std::make_shared<arrow::Field>("TabletId", arrow::uint64()), std::make_shared<arrow::Field>("PortionId", arrow::uint64()) };
        return std::make_shared<arrow::Schema>(std::move(fields));
    }();
    return schema;
}

TIndexInfo TSchemaAdapter::GetIndexInfo(
    const std::shared_ptr<IStoragesManager>& storagesManager, const std::shared_ptr<TSchemaObjectsCache>& schemaObjectsCache) const {
    //PrimaryIndexPortionStats
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
        pred("Kind", NScheme::NTypeIds::Utf8);
        pred("TabletId", NScheme::NTypeIds::Uint64);
        pred("Rows", NScheme::NTypeIds::Uint64);
        pred("ColumnRawBytes", NScheme::NTypeIds::Uint64);
        pred("IndexRawBytes", NScheme::NTypeIds::Uint64);
        pred("ColumnBlobBytes", NScheme::NTypeIds::Uint64);
        pred("IndexBlobBytes", NScheme::NTypeIds::Uint64);
        pred("PortionId", NScheme::NTypeIds::Uint64);
        pred("Activity", NScheme::NTypeIds::Uint8);
        pred("TierName", NScheme::NTypeIds::Utf8);
        pred("Stats", NScheme::NTypeIds::Utf8);
        pred("Optimized", NScheme::NTypeIds::Uint8);
        pred("CompactionLevel", NScheme::NTypeIds::Uint64);
        pred("Details", NScheme::NTypeIds::Utf8);

        proto.AddKeyColumnNames("PathId");
        proto.AddKeyColumnNames("TabletId");
        proto.AddKeyColumnNames("PortionId");
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

NTable::TScheme::TTableSchema TSchemaAdapter::GetStatsSchema() {
    NTable::TScheme::TTableSchema schema;
    NIceDb::NHelpers::TStaticSchemaFiller<NKikimr::NSysView::Schema::PrimaryIndexPortionStats>::Fill(schema);
    return schema;
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NPortions
