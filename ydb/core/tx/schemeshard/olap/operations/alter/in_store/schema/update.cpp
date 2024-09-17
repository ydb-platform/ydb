#include "update.h"

#include <ydb/core/tx/schemeshard/olap/operations/alter/abstract/converter.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/tiering/rule/object.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

NKikimr::TConclusionStatus TInStoreSchemaUpdate::DoInitializeImpl(const TUpdateInitializationContext& context) {
    auto alter = TConverterModifyToAlter().Convert(*context.GetModification());
    if (alter.IsFail()) {
        return alter;
    }
    auto alterCS = alter.DetachResult();
    if (alterCS.HasAlterSchema()) {
        return TConclusionStatus::Fail("cannot modify scheme for table in store");
    }
    if (!alterCS.HasAlterTtlSettings()) {
        return TConclusionStatus::Fail("no data for update");
    }
    AlterTTL.emplace(alterCS.GetAlterTtlSettings());
    const auto& originalTable = context.GetOriginalEntityAsVerified<TInStoreTable>();
    auto description = originalTable.GetTableInfoVerified().Description;

    {
        const auto& originalTableInfo = originalTable.GetTableInfoVerified();
        const auto& storeInfo = originalTable.GetStoreInfo();
        AFL_VERIFY(!!storeInfo)("problem", "Unexpected schema preset without olap store");
        AFL_VERIFY(originalTableInfo.Description.HasSchemaPresetId());
        const ui32 presetId = originalTableInfo.Description.GetSchemaPresetId();
        AFL_VERIFY(storeInfo->SchemaPresets.contains(presetId))("problem", "Failed to find schema preset in an olap store")("id", presetId);
        auto& preset = storeInfo->SchemaPresets.at(presetId);
        size_t presetIndex = preset.GetProtoIndex();
        SchemaPreset = storeInfo->GetDescription().GetSchemaPresets(presetIndex);
    }

    if (AlterTTL) {
        TOlapSchema originalSchema;
        originalSchema.ParseFromLocalDB(SchemaPreset->GetSchema());

        auto ttl = originalTable.GetTableTTLOptional() ? *originalTable.GetTableTTLOptional() : TOlapTTL();
        auto patch = ttl.Update(*AlterTTL);
        if (patch.IsFail()) {
            return patch;
        }
        TSimpleErrorCollector collector;
        if (!originalSchema.ValidateTtlSettings(ttl.GetData(), collector)) {
            return TConclusionStatus::Fail("ttl update error: " + collector->GetErrorMessage() + ". in alter constructor STANDALONE_UPDATE");
        }

        // TODO: Consider moving to a function
        if (AlterTTL->GetPatch().HasUseTiering() && !ttl.GetData().GetUseTiering().empty()) {
            const TString& tiering = ttl.GetData().GetUseTiering();
            const TObjectsInfo& objects = context.GetSSOperationContext()->SS->Objects;
            if (!objects.IsInitialized<NColumnShard::NTiers::TTieringRule>()) {
                return TConclusionStatus::Fail("Tiering metadata hasn't been initialized at Scheme Shard yet.");
            }
            if (objects.IsModificationInFly<NColumnShard::NTiers::TTieringRule>(tiering)) {
                return TConclusionStatus::Fail("Modification of tiering is in progress: " + tiering);
            }
            auto findTiering = objects.GetMetadataVerified<NColumnShard::NTiers::TTieringRule>()->FindObject(tiering);
            if (!findTiering) {
                return TConclusionStatus::Fail("Unknown tiering: " + tiering);
            }
            const TString evictionColumn = findTiering->GetMetadata().GetDefaultColumn();
            const auto* findColumn = originalSchema.GetColumns().GetByName(evictionColumn);
            // TODO: Add unit-test: check that DROP COLUMN under tiering doesn't work
            // TODO: Add unit-test: check that DROP COLUMN and SET tiering on that column doesn't work
            if (!findColumn) {
                return TConclusionStatus::Fail("Can't set tiering rule with invalid default column: " + evictionColumn);
            }
            if (!findColumn->IsNotNull()) {
                return TConclusionStatus::Fail("Can't set tiering rule with nullable default column: " + evictionColumn);
            }
            // TODO: validate column type
        }

        *description.MutableTtlSettings() = ttl.SerializeToProto();
    }

    auto targetInfo = std::make_shared<TColumnTableInfo>(context.GetOriginalEntityAsVerified<TInStoreTable>().GetTableInfoVerified().AlterVersion + 1,
        std::move(description), TMaybe<NKikimrSchemeOp::TColumnStoreSharding>(), std::move(alterCS));
    TEntityInitializationContext eContext(context.GetSSOperationContext());
    TargetInStoreTable = std::make_shared<TInStoreTable>(context.GetOriginalEntity().GetPathId(), targetInfo, eContext);
    return TConclusionStatus::Success();
}

void TInStoreSchemaUpdate::FillToShardTx(NKikimrTxColumnShard::TAlterTable& shardAlter) const {
    auto& alterBody = TargetInStoreTable->GetTableInfoVerified();
    *shardAlter.MutableTtlSettings() = alterBody.Description.GetTtlSettings();

    AFL_VERIFY(!!SchemaPreset);
    *shardAlter.MutableSchemaPreset() = *SchemaPreset;

    if (alterBody.Description.HasSchemaPresetVersionAdj()) {
        shardAlter.SetSchemaPresetVersionAdj(alterBody.Description.GetSchemaPresetVersionAdj());
    }
}

}