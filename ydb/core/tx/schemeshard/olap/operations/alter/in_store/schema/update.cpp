#include "evolution.h"
#include "update.h"
#include <ydb/core/tx/schemeshard/olap/operations/alter/abstract/converter.h>
#include <ydb/core/tx/schemeshard/olap/operations/alter/operations/start.h>
#include <ydb/core/tx/schemeshard/olap/operations/alter/operations/continue.h>
#include <ydb/core/tx/schemeshard/olap/operations/alter/operations/publish.h>
#include <ydb/core/tx/schemeshard/olap/operations/alter/operations/finish.h>

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
    auto& alterBody = OriginalInStoreTable->GetTableInfoVerified();

    AFL_VERIFY(alterBody.Description.HasSchemaPresetId());
    const ui32 presetId = alterBody.Description.GetSchemaPresetId();
    Y_ABORT_UNLESS(!!OriginalInStoreTable->GetStoreInfo(),
        "Unexpected schema preset without olap store");
    Y_ABORT_UNLESS(OriginalInStoreTable->GetStoreInfo()->SchemaPresets.contains(presetId),
        "Failed to find schema preset %" PRIu32
        " in an olap store",
        presetId);
    auto& preset = OriginalInStoreTable->GetStoreInfo()->SchemaPresets.at(presetId);
    size_t presetIndex = preset.GetProtoIndex();
    *AlterToShard.MutableSchemaPreset() =
        OriginalInStoreTable->GetStoreInfo()->GetDescription().GetSchemaPresets(presetIndex);
    if (alterBody.Description.HasSchemaPresetVersionAdj()) {
        AlterToShard.SetSchemaPresetVersionAdj(alterBody.Description.GetSchemaPresetVersionAdj());
    }

    return TConclusionStatus::Success();
}

TConclusion<TEvolutions> TInStoreSchemaUpdate::DoBuildEvolutions() const {
    return TEvolutions(GetRequest(), { std::make_shared<TInStoreSchemaEvolution>(AlterToShard, OriginalInStoreTable->GetTableInfoVerified().GetShardIdsSet()) });
}

TConclusionStatus TInStoreSchemaUpdate::InitializeDiff(const NKikimrSchemeOp::TAlterColumnTable& alterCS) {
    if (alterCS.HasAlterSchema()) {
        return TConclusionStatus::Fail("cannot modify scheme for table in store");
    }
    if (!alterCS.HasAlterTtlSettings()) {
        return TConclusionStatus::Fail("no data for update");
    }

    const TOlapSchema& originalSchema = OriginalInStoreTable->GetTableSchemaVerified();
    TOlapTTL ttl = OriginalInStoreTable->GetTableTTLOptional() ? *OriginalInStoreTable->GetTableTTLOptional() : TOlapTTL();
    auto patch = ttl.Update(TOlapTTLUpdate(alterCS.GetAlterTtlSettings()));
    if (patch.IsFail()) {
        return patch;
    }
    TSimpleErrorCollector collector;
    if (!originalSchema.ValidateTtlSettings(ttl.GetData(), collector)) {
        return TConclusionStatus::Fail("ttl update error: " + collector->GetErrorMessage() + ". in alter constructor IN_STORE_TABLE");
    }
    *AlterToShard.MutableTtlSettings() = ttl.GetData();
    return TConclusionStatus::Success();
}

TVector<NKikimr::NSchemeShard::ISubOperation::TPtr> TInStoreSchemaUpdate::DoBuildOperations(const TOperationId& id, const NKikimrSchemeOp::TModifyScheme& request) const {
    TVector<NKikimr::NSchemeShard::ISubOperation::TPtr> result;
    result.emplace_back(new TStartAlterColumnTable(id, request));
    result.emplace_back(new TEvoluteAlterColumnTable(id, request));
    result.emplace_back(new TPublishAlterColumnTable(id, request));
    result.emplace_back(new TFinishAlterColumnTable(id, request));
    return result;
}

}