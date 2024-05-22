#include "update.h"
#include "evolution.h"
#include <ydb/core/tx/schemeshard/olap/operations/alter/abstract/converter.h>
#include <ydb/core/tx/schemeshard/olap/common/common.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

NKikimr::TConclusion<std::shared_ptr<NKikimr::NSchemeShard::NOlap::NAlter::ISSEntityEvolution>> TStandaloneSchemaUpdate::DoBuildEvolution(const std::shared_ptr<ISSEntityUpdate>& updateSelfPtr) const {
    auto originalInfo = dynamic_pointer_cast<TStandaloneTable>(GetOriginalEntity());
    if (!originalInfo) {
        return TConclusionStatus::Fail("incorrect incoming type for patch: " + GetOriginalEntity()->GetClassName() + " in STANDALONE_UPDATE");
    }

    auto updateSelfCast = dynamic_pointer_cast<TStandaloneSchemaUpdate>(updateSelfPtr);
    AFL_VERIFY(!!updateSelfCast);
    return std::make_shared<TStandaloneSchemaEvolution>(OriginalStandalone, TargetStandalone, updateSelfCast);
}

NKikimr::TConclusionStatus TStandaloneSchemaUpdate::DoInitialize(const TUpdateInitializationContext& context) {
    auto alter = TConverterModifyToAlter().Convert(*context.GetModification());
    if (alter.IsFail()) {
        return alter;
    }
    auto alterCS = alter.DetachResult();
    if (alterCS.HasAlterSchema()) {
        TSimpleErrorCollector collector;
        TOlapSchemaUpdate schemaUpdate;
        if (!schemaUpdate.Parse(alterCS.GetAlterSchema(), collector)) {
            return TConclusionStatus::Fail("update parse error: " + collector->GetErrorMessage() + ". in alter constructor STANDALONE_UPDATE");
        }
        AlterSchema = std::move(schemaUpdate);
    }
    AlterTTL = alterCS.GetAlterTtlSettings();
    if (!AlterSchema && !AlterTTL) {
        return TConclusionStatus::Fail("no data for update");
    }

    TOlapSchema originalSchema;
    originalSchema.ParseFromLocalDB(OriginalStandalone->GetTableInfoVerified().Description.GetSchema());

    TSimpleErrorCollector collector;
    TOlapSchema targetSchema = originalSchema;
    if (AlterSchema) {
        if (!targetSchema.Update(*AlterSchema, collector)) {
            return TConclusionStatus::Fail("schema update error: " + collector->GetErrorMessage() + ". in alter constructor STANDALONE_UPDATE");
        }
    }
    auto description = OriginalStandalone->GetTableInfoVerified().Description;
    targetSchema.Serialize(*description.MutableSchema());
    std::optional<TOlapTTL> ttl;
    if (AlterTTL) {
        ttl = OriginalStandalone->GetTableTTLOptional() ? *OriginalStandalone->GetTableTTLOptional() : TOlapTTL();
        auto patch = ttl->Update(*AlterTTL);
        if (patch.IsFail()) {
            return patch;
        }
        if (!targetSchema.ValidateTtlSettings(ttl->GetData(), collector)) {
            return TConclusionStatus::Fail("ttl update error: " + collector->GetErrorMessage() + ". in alter constructor STANDALONE_UPDATE");
        }
        *description.MutableTtlSettings() = ttl->SerializeToProto();
    }
    auto saSharding = OriginalStandalone->GetTableInfoVerified().GetStandaloneShardingVerified();

    auto targetInfo = std::make_shared<TColumnTableInfo>(OriginalStandalone->GetTableInfoVerified().AlterVersion + 1, std::move(description), std::move(saSharding), alterCS);
    TargetStandalone = std::make_shared<TStandaloneTable>(GetOriginalEntity()->GetPathId(), targetInfo);

    return TConclusionStatus::Success();
}

}