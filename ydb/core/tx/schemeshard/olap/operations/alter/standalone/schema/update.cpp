#include "update.h"
#include "evolution.h"
#include <ydb/core/tx/schemeshard/olap/operations/alter/abstract/converter.h>
#include <ydb/core/tx/schemeshard/olap/common/common.h>
#include <ydb/core/tx/schemeshard/olap/operations/alter/operations/start.h>
#include <ydb/core/tx/schemeshard/olap/operations/alter/operations/continue.h>
#include <ydb/core/tx/schemeshard/olap/operations/alter/operations/publish.h>
#include <ydb/core/tx/schemeshard/olap/operations/alter/operations/finish.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

TConclusion<TEvolutions> TStandaloneSchemaUpdate::DoBuildEvolutions() const {
    auto originalInfo = dynamic_pointer_cast<TStandaloneTable>(GetOriginalEntity());
    if (!originalInfo) {
        return TConclusionStatus::Fail("incorrect incoming type for patch: " + GetOriginalEntity()->GetClassName() + " in STANDALONE_UPDATE");
    }

    TEvolutions result(GetRequest(), {std::make_shared<TStandaloneSchemaEvolution>(AlterToShard, ShardIds)});
    return result;
}

NKikimr::TConclusionStatus TStandaloneSchemaUpdate::DoInitialize(const TUpdateInitializationContext& /*context*/) {
    auto alter = TConverterModifyToAlter().Convert(GetRequest());
    if (alter.IsFail()) {
        return alter;
    }
    auto alterCS = alter.DetachResult();
    std::optional<TOlapSchemaUpdate> alterSchema;
    std::optional<TOlapTTLUpdate> alterTTL;
    if (alterCS.HasAlterSchema()) {
        TSimpleErrorCollector collector;
        TOlapSchemaUpdate schemaUpdate;
        if (!schemaUpdate.Parse(alterCS.GetAlterSchema(), collector)) {
            return TConclusionStatus::Fail("update parse error: " + collector->GetErrorMessage() + ". in alter constructor STANDALONE_UPDATE");
        }
        alterSchema = std::move(schemaUpdate);
    }
    if (alterCS.HasAlterTtlSettings()) {
        alterTTL = alterCS.GetAlterTtlSettings();
    }
    if (!alterSchema && !alterTTL) {
        return TConclusionStatus::Fail("no data for update");
    }

    TOlapSchema originalSchema;
    originalSchema.ParseFromLocalDB(OriginalStandalone->GetTableInfoVerified().Description.GetSchema());

    TSimpleErrorCollector collector;
    TOlapSchema targetSchema = originalSchema;
    if (alterSchema) {
        if (!targetSchema.Update(*alterSchema, collector)) {
            return TConclusionStatus::Fail("schema update error: " + collector->GetErrorMessage() + ". in alter constructor STANDALONE_UPDATE");
        }
    }
    auto description = OriginalStandalone->GetTableInfoVerified().Description;
    targetSchema.Serialize(*description.MutableSchema());
    std::optional<TOlapTTL> ttl;
    if (alterTTL) {
        ttl = OriginalStandalone->GetTableTTLOptional() ? *OriginalStandalone->GetTableTTLOptional() : TOlapTTL();
        auto patch = ttl->Update(*alterTTL);
        if (patch.IsFail()) {
            return patch;
        }
        if (!targetSchema.ValidateTtlSettings(ttl->GetData(), collector)) {
            return TConclusionStatus::Fail("ttl update error: " + collector->GetErrorMessage() + ". in alter constructor STANDALONE_UPDATE");
        }
        *description.MutableTtlSettings() = ttl->SerializeToProto();
    }
    auto saSharding = OriginalStandalone->GetTableInfoVerified().GetStandaloneShardingVerified();

    if (alterSchema) {
        *AlterToShard.MutableSchema() = description.GetSchema();
    }
    if (alterTTL) {
        *AlterToShard.MutableTtlSettings() = description.GetTtlSettings();
    }

    ShardIds = OriginalStandalone->GetTableInfoVerified().GetShardIdsSet();

    return TConclusionStatus::Success();
}

TVector<NKikimr::NSchemeShard::ISubOperation::TPtr> TStandaloneSchemaUpdate::DoBuildOperations(const TOperationId& id, const NKikimrSchemeOp::TModifyScheme& request) const {
    TVector<NKikimr::NSchemeShard::ISubOperation::TPtr> result;
    result.emplace_back(new TStartAlterColumnTable(id, request));
    result.emplace_back(new TEvoluteAlterColumnTable(id, request));
    result.emplace_back(new TPublishAlterColumnTable(id, request));
    result.emplace_back(new TFinishAlterColumnTable(id, request));
    return result;
}

}