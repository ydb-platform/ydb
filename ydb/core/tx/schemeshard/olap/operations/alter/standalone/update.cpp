#include "update.h"
#include <ydb/core/tx/schemeshard/olap/operations/alter/abstract/converter.h>
#include <ydb/core/tx/schemeshard/olap/common/common.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

NKikimr::TConclusionStatus TStandaloneSchemaUpdate::DoInitializeImpl(const TUpdateInitializationContext& context) {
    const auto& originalTable = context.GetOriginalEntityAsVerified<TStandaloneTable>();
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
    if (alterCS.HasAlterTtlSettings()) {
        AlterTTL = alterCS.GetAlterTtlSettings();
    }
    if (!AlterSchema && !AlterTTL) {
        return TConclusionStatus::Fail("no data for update");
    }

    TOlapSchema originalSchema;
    originalSchema.ParseFromLocalDB(originalTable.GetTableInfoVerified().Description.GetSchema());

    TSimpleErrorCollector collector;
    TOlapSchema targetSchema = originalSchema;
    if (AlterSchema) {
        if (!targetSchema.Update(*AlterSchema, collector)) {
            return TConclusionStatus::Fail("schema update error: " + collector->GetErrorMessage() + ". in alter constructor STANDALONE_UPDATE");
        }
    }
    auto description = originalTable.GetTableInfoVerified().Description;
    targetSchema.Serialize(*description.MutableSchema());
    auto ttl = originalTable.GetTableTTLOptional() ? *originalTable.GetTableTTLOptional() : TOlapTTL();
    if (AlterTTL) {
        auto patch = ttl.Update(*AlterTTL);
        if (patch.IsFail()) {
            return patch;
        }
        *description.MutableTtlSettings() = ttl.SerializeToProto();
    }
    if (!targetSchema.ValidateTtlSettings(ttl.GetData(), collector)) {
        return TConclusionStatus::Fail("ttl update error: " + collector->GetErrorMessage() + ". in alter constructor STANDALONE_UPDATE");
    }
    auto saSharding = originalTable.GetTableInfoVerified().GetStandaloneShardingVerified();

    auto targetInfo = std::make_shared<TColumnTableInfo>(originalTable.GetTableInfoVerified().AlterVersion + 1, std::move(description), std::move(saSharding), alterCS);
    TargetStandalone = std::make_shared<TStandaloneTable>(context.GetOriginalEntity().GetPathId(), targetInfo);

    return TConclusionStatus::Success();
}

}