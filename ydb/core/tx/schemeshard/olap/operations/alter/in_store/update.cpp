#include "evolution.h"
#include "update.h"
#include <ydb/core/tx/schemeshard/olap/operations/alter/abstract/converter.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

NKikimr::TConclusionStatus TInStoreSchemaUpdate::DoInitialize(const TUpdateInitializationContext& context) {
    auto alter = TConverterModifyToAlter().Convert(*context.GetModification());
    if (alter.IsFail()) {
        return alter;
    }
    auto alterCS = alter.DetachResult();
    {
        auto conclusion = InitializeDiff(alterCS);
        if (conclusion.IsFail()) {
            return conclusion;
        }
    }
    auto description = OriginalInStoreTable->GetTableInfoVerified().Description;
    {
        auto ttlConclusion = GetTtlPatched();
        if (ttlConclusion.IsFail()) {
            return ttlConclusion;
        }
        *description.MutableTtlSettings() = ttlConclusion->SerializeToProto();
    }

    auto targetInfo = std::make_shared<TColumnTableInfo>(OriginalInStoreTable->GetTableInfoVerified().AlterVersion + 1, std::move(description), TMaybe<NKikimrSchemeOp::TColumnStoreSharding>(), std::move(alterCS));
    TEntityInitializationContext eContext(context.GetSSOperationContext());
    TargetInStoreTable = std::make_shared<TInStoreTable>(GetOriginalEntity()->GetPathId(), targetInfo, eContext);

    return TConclusionStatus::Success();
}

NKikimr::TConclusion<std::shared_ptr<NKikimr::NSchemeShard::NOlap::NAlter::ISSEntityEvolution>> TInStoreSchemaUpdate::DoBuildEvolution(const std::shared_ptr<ISSEntityUpdate>& updateSelfPtr) const {
    auto update = dynamic_pointer_cast<TInStoreSchemaUpdate>(updateSelfPtr);
    AFL_VERIFY(!!update);
    return std::make_shared<TInStoreSchemaEvolution>(OriginalInStoreTable, TargetInStoreTable, update);
}

NKikimr::TConclusionStatus TInStoreSchemaUpdate::InitializeDiff(const NKikimrSchemeOp::TAlterColumnTable& alterCS) {
    if (alterCS.HasAlterSchema()) {
        return TConclusionStatus::Fail("cannot modify scheme for table in store");
    }
    AlterTTL = alterCS.GetAlterTtlSettings();
    if (!AlterTTL) {
        return TConclusionStatus::Fail("no data for update");
    }
    auto ttlConclusion = GetTtlPatched();
    if (ttlConclusion.IsFail()) {
        return ttlConclusion;
    }
    return TConclusionStatus::Success();
}

NKikimr::TConclusion<NKikimr::NSchemeShard::NOlap::NAlter::TOlapTTL> TInStoreSchemaUpdate::GetTtlPatched() const {
    const TOlapSchema& originalSchema = OriginalInStoreTable->GetTableSchemaVerified();
    TOlapTTL ttl = OriginalInStoreTable->GetTableTTLOptional() ? *OriginalInStoreTable->GetTableTTLOptional() : TOlapTTL();
    auto patch = ttl.Update(*AlterTTL);
    if (patch.IsFail()) {
        return patch;
    }
    TSimpleErrorCollector collector;
    if (!originalSchema.ValidateTtlSettings(ttl.GetData(), collector)) {
        return TConclusionStatus::Fail("ttl update error: " + collector->GetErrorMessage() + ". in alter constructor IN_STORE_TABLE");
    }
    return ttl;
}

void TInStoreSchemaUpdate::FillToShardTx(NKikimrTxColumnShard::TAlterTable& shardAlter, const std::shared_ptr<TInStoreTable>& target) const {
    if (AlterTTL) {
        *shardAlter.MutableTtlSettings() = target->GetTableTTLProto();
    }
    auto& alterBody = OriginalInStoreTable->GetTableInfoVerified();

    AFL_VERIFY(alterBody.Description.HasSchemaPresetId());
    const ui32 presetId = alterBody.Description.GetSchemaPresetId();
    Y_ABORT_UNLESS(!!TargetInStoreTable->GetStoreInfo(),
        "Unexpected schema preset without olap store");
    Y_ABORT_UNLESS(TargetInStoreTable->GetStoreInfo()->SchemaPresets.contains(presetId),
        "Failed to find schema preset %" PRIu32
        " in an olap store",
        presetId);
    auto& preset = TargetInStoreTable->GetStoreInfo()->SchemaPresets.at(presetId);
    size_t presetIndex = preset.GetProtoIndex();
    *shardAlter.MutableSchemaPreset() =
        TargetInStoreTable->GetStoreInfo()->GetDescription().GetSchemaPresets(presetIndex);

    if (alterBody.Description.HasSchemaPresetVersionAdj()) {
        shardAlter.SetSchemaPresetVersionAdj(alterBody.Description.GetSchemaPresetVersionAdj());
    }
}

}