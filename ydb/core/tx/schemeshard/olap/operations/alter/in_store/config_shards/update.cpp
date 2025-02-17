#include "update.h"
#include <ydb/core/tx/schemeshard/olap/operations/alter/abstract/converter.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

TConclusionStatus TInStoreShardsUpdate::DoInitializeImpl(const TUpdateInitializationContext& context) {
    const auto& original = context.GetOriginalEntityAsVerified<TInStoreTable>();
    if (!context.GetModification()->GetAlterColumnTable().HasAlterShards()) {
        return TConclusionStatus::Fail("no data about shards altering");
    }
    auto& alterShards = context.GetModification()->GetAlterColumnTable().GetAlterShards();
    Alter = alterShards;
    AFL_VERIFY(Alter.HasModification());

    for (auto&& i : Alter.GetModification().GetNewShardIds()) {
        AFL_VERIFY(ShardIds.emplace(i).second);
        AFL_VERIFY(NewShardIds.emplace(i).second);
    }

    for (auto&& i : Alter.GetModification().GetDeleteShardIds()) {
        AFL_VERIFY(ShardIds.emplace(i).second);
        AFL_VERIFY(DeleteShardIds.emplace(i).second);
    }

    auto tableInfo = original.GetTableInfoPtrVerified();

    auto description = tableInfo->Description;

    Sharding = tableInfo->GetShardingVerified(original.GetTableSchemaVerified());
    Sharding->ApplyModification(Alter.GetModification()).Validate();
    *description.MutableSharding() = Sharding->SerializeToProto();
    for (auto&& i : Sharding->GetModifiedShardIds(Alter.GetModification())) {
        ShardIds.emplace(i);
        AFL_VERIFY(ModifiedShardIds.emplace(i).second);
    }

    auto targetInfo = std::make_shared<TColumnTableInfo>(tableInfo->AlterVersion + 1, std::move(description),
        TMaybe<NKikimrSchemeOp::TColumnStoreSharding>(), context.GetModification()->GetAlterColumnTable());

    TEntityInitializationContext eContext(context.GetSSOperationContext());
    TargetInStoreTable = std::make_shared<TInStoreTable>(original.GetPathId(), targetInfo, eContext);

    return TConclusionStatus::Success();
}

void TInStoreShardsUpdate::FillToShardTx(NKikimrTxColumnShard::TCreateTable& info) const {
    info.SetPathId(TargetInStoreTable->GetPathId().LocalPathId);
    auto& alterBody = TargetInStoreTable->GetTableInfoVerified();

    AFL_VERIFY(alterBody.Description.HasSchemaPresetId());
    const ui32 presetId = alterBody.Description.GetSchemaPresetId();
    Y_ABORT_UNLESS(!!TargetInStoreTable->GetStoreInfo(), "Unexpected schema preset without olap store");
    Y_ABORT_UNLESS(TargetInStoreTable->GetStoreInfo()->SchemaPresets.contains(presetId), "Failed to find schema preset %" PRIu32 " in an olap store", presetId);
    auto& preset = TargetInStoreTable->GetStoreInfo()->SchemaPresets.at(presetId);
    size_t presetIndex = preset.GetProtoIndex();
    *info.MutableSchemaPreset() = TargetInStoreTable->GetStoreInfo()->GetDescription().GetSchemaPresets(presetIndex);

    if (alterBody.Description.HasSchemaPresetVersionAdj()) {
        info.SetSchemaPresetVersionAdj(alterBody.Description.GetSchemaPresetVersionAdj());
    }
    if (alterBody.Description.HasTtlSettings()) {
        *info.MutableTtlSettings() = alterBody.Description.GetTtlSettings();
    }
}

NKikimr::TConclusionStatus TInStoreShardsUpdate::DoFinishImpl(const TUpdateFinishContext& context) {
    auto conclusion = TBase::DoFinishImpl(context);
    if (conclusion.IsFail()) {
        return conclusion;
    }
    auto alter = context.GetSSOperationContext()->SS->ColumnTables.GetVerifiedPtr(TargetInStoreTable->GetPathId())->AlterData;
    for (auto&& i : Alter.GetModification().GetOpenWriteIds()) {
        AFL_VERIFY(!!alter);
        Sharding->SetShardingOpenSnapshotVerified(i, context.GetSnapshotVerified());
    }
    AFL_VERIFY(!!alter->AlterBody);
    *alter->Description.MutableSharding() = Sharding->SerializeToProto();
    return TConclusionStatus::Success();
}

}