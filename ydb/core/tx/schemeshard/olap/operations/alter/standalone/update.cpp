#include "update.h"
#include <ydb/core/tx/schemeshard/olap/operations/alter/abstract/converter.h>
#include <ydb/core/tx/schemeshard/olap/common/common.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

NKikimr::TConclusionStatus TStandaloneSchemaUpdate::DoInitializeImpl(const TUpdateInitializationContext& context) {
    const auto& originalTable = context.GetOriginalEntityAsVerified<TStandaloneTable>();
    if (const auto& alterData = originalTable.GetTableInfoVerified().AlterData) {
        UpdateSchema = alterData->AlterBody->HasAlterSchema();
        UpdateTTL = alterData->AlterBody->HasAlterTtlSettings();
        TargetStandalone = std::make_shared<TStandaloneTable>(originalTable.GetPathId(), alterData);
        return TConclusionStatus::Success();
    }

    auto alter = TConverterModifyToAlter().Convert(*context.GetModification());
    if (alter.IsFail()) {
        return alter;
    }
    auto alterCS = alter.DetachResult();
    UpdateSchema = alterCS.HasAlterSchema();
    UpdateTTL = alterCS.HasAlterTtlSettings();
    std::optional<TOlapSchemaUpdate> alterSchema;
    std::optional<TOlapTTLUpdate> alterTTL;
    std::optional<TOlapMultiColumnStatisticsUpdate> alterMultiColumnStatistics;
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
    if (alterCS.UpsertMultiColumnStatisticsSize() || alterCS.DropMultiColumnStatisticsSize()) {
        TOlapMultiColumnStatisticsUpdate statisticsUpdate;
        TSimpleErrorCollector statisticsCollector;
        if (!statisticsUpdate.Parse(alterCS, statisticsCollector)) {
            return TConclusionStatus::Fail("statistics update error: " + statisticsCollector->GetErrorMessage() + ". in alter constructor STANDALONE_UPDATE");
        }
        alterMultiColumnStatistics = std::move(statisticsUpdate);
    }
    if (!alterSchema && !alterTTL && !alterMultiColumnStatistics) {
        return TConclusionStatus::Fail("no data for update");
    }

    TOlapSchema originalSchema;
    originalSchema.ParseFromLocalDB(originalTable.GetTableInfoVerified().Description.GetSchema());

    TSimpleErrorCollector collector;
    TOlapSchema targetSchema = originalSchema;
    if (alterSchema) {
        if (!targetSchema.Update(*alterSchema, collector)) {
            return TConclusionStatus::Fail("schema update error: " + collector->GetErrorMessage() + ". in alter constructor STANDALONE_UPDATE");
        }
    }

    TOlapMultiColumnStatisticsDescription targetMultiColumnStatistics = originalTable.GetTableMultiColumnStatisticsVerified();
    if (alterMultiColumnStatistics) {
        if (!targetMultiColumnStatistics.ApplyUpdate(targetSchema, *alterMultiColumnStatistics, collector)) {
            return TConclusionStatus::Fail("statistics update error: " + collector->GetErrorMessage() + ". in alter constructor STANDALONE_UPDATE");
        }
    }

    const TString& parentPathStr = context.GetModification()->GetWorkingDir();
    if (parentPathStr) { // Not empty only if called from Propose, not from ProgressState
        NSchemeShard::TPath parentPath = NSchemeShard::TPath::Resolve(parentPathStr, context.GetSSOperationContext()->SS);
        auto domainInfo = parentPath.DomainInfo();
        const TSchemeLimits& limits = domainInfo->GetSchemeLimits();
        if (targetSchema.GetColumns().GetColumns().size() > limits.MaxColumnTableColumns) {
            TString errStr = TStringBuilder()
                << "Too many columns"
                << ": new: " << targetSchema.GetColumns().GetColumns().size()
                << ". Limit: " << limits.MaxColumnTableColumns;
            return TConclusionStatus::Fail(errStr);
        }
    }

    if (!CheckTargetSchema(targetSchema)) {
        return TConclusionStatus::Fail("schema update error: sparsed columns are disabled");
    }
    auto description = originalTable.GetTableInfoVerified().Description;
    targetSchema.Serialize(*description.MutableSchema());
    targetMultiColumnStatistics.Serialize(description);
    auto ttl = originalTable.GetTableTTLOptional() ? *originalTable.GetTableTTLOptional() : TOlapTTL();
    if (alterTTL) {
        auto patch = ttl.Update(*alterTTL);
        if (patch.IsFail()) {
            return patch;
        }
        *description.MutableTtlSettings() = ttl.SerializeToProto();
    }
    if (!targetSchema.ValidateTtlSettings(ttl.GetData(), *context.GetSSOperationContext(), collector)) {
        return TConclusionStatus::Fail("ttl update error: " + collector->GetErrorMessage() + ". in alter constructor STANDALONE_UPDATE");
    }
    auto saSharding = originalTable.GetTableInfoVerified().GetStandaloneShardingVerified();

    auto targetInfo = std::make_shared<TColumnTableInfo>(originalTable.GetTableInfoVerified().AlterVersion + 1, std::move(description), std::move(saSharding), alterCS);
    TargetStandalone = std::make_shared<TStandaloneTable>(context.GetOriginalEntity().GetPathId(), targetInfo);

    return TConclusionStatus::Success();
}

}
