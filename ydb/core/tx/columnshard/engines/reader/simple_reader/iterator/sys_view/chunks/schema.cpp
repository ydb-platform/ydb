#include "metadata.h"
#include "schema.h"

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NChunks {

std::shared_ptr<ITableMetadataAccessor> TSchemaAdapter::BuildMetadataAccessor(const TString& tableName,
    const NColumnShard::TSchemeShardLocalPathId externalPathId,
    const std::optional<NColumnShard::TInternalPathId> internalPathId) const {
    return std::make_shared<TAccessor>(tableName, externalPathId, internalPathId);
}

NArrow::TSimpleRow TSchemaAdapter::GetPKSimpleRow(
    const NColumnShard::TUnifiedPathId pathId, const ui64 tabletId, const ui64 portionId, const ui32 entityId, const ui64 chunkIdx) {
    NArrow::TSimpleRowViewV0::TWriter writer(sizeof(ui64) * 4 + sizeof(ui32));
    writer.Append<ui64>(pathId.SchemeShardLocalPathId.GetRawValue());
    writer.Append<ui64>(tabletId);
    writer.Append<ui64>(portionId);
    writer.Append<ui32>(entityId);
    writer.Append<ui64>(chunkIdx);
    return NArrow::TSimpleRow(writer.Finish(), GetPKSchema());
}

std::shared_ptr<arrow::Schema> TSchemaAdapter::GetPKSchema() {
    static std::shared_ptr<arrow::Schema> schema = []() {
        arrow::FieldVector fields = { std::make_shared<arrow::Field>("PathId", arrow::uint64()),
            std::make_shared<arrow::Field>("TabletId", arrow::uint64()), std::make_shared<arrow::Field>("PortionId", arrow::uint64()),
            std::make_shared<arrow::Field>("InternalEntityId", arrow::uint32()), std::make_shared<arrow::Field>("ChunkIdx", arrow::uint64()) };
        return std::make_shared<arrow::Schema>(std::move(fields));
    }();
    return schema;
}

TIndexInfo TSchemaAdapter::GetIndexInfo(
    const std::shared_ptr<IStoragesManager>& storagesManager, const std::shared_ptr<TSchemaObjectsCache>& schemaObjectsCache) const {
    //PrimaryIndexStats
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
        pred("RawBytes", NScheme::NTypeIds::Uint64);
        pred("PortionId", NScheme::NTypeIds::Uint64);
        pred("ChunkIdx", NScheme::NTypeIds::Uint64);
        pred("EntityName", NScheme::NTypeIds::Utf8);
        pred("InternalEntityId", NScheme::NTypeIds::Uint32);
        pred("BlobId", NScheme::NTypeIds::Utf8);
        pred("BlobRangeOffset", NScheme::NTypeIds::Uint64);
        pred("BlobRangeSize", NScheme::NTypeIds::Uint64);
        pred("Activity", NScheme::NTypeIds::Uint8);
        pred("TierName", NScheme::NTypeIds::Utf8);
        pred("EntityType", NScheme::NTypeIds::Utf8);
        proto.AddKeyColumnNames("PathId");
        proto.AddKeyColumnNames("TabletId");
        proto.AddKeyColumnNames("PortionId");
        proto.AddKeyColumnNames("InternalEntityId");
        proto.AddKeyColumnNames("ChunkIdx");
        return proto;
    }();

    auto indexInfo = TIndexInfo::BuildFromProto(GetPresetId(), proto, storagesManager, schemaObjectsCache);
    AFL_VERIFY(indexInfo);
    return std::move(*indexInfo);
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NChunks
