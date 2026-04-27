#include <ydb/core/tx/schemeshard/olap/common/common.h>
#include <ydb/core/tx/schemeshard/olap/operations/local_index_helpers.h>
#include <ydb/core/tx/schemeshard/olap/schema/schema.h>

namespace NKikimr::NSchemeShard {

TVector<ISubOperation::TPtr> CreateColumnTableWithLocalIndexes(TOperationId nextId, const TTxTransaction& tx, TOperationContext&) {
    TVector<ISubOperation::TPtr> result;

    const auto& createDescription = tx.GetCreateColumnTable();
    const TString& tableName = createDescription.GetName();
    const TString& workingDir = tx.GetWorkingDir();

    // Validate schema and indexes before pushing any parts to result
    // This ensures that if validation fails, we return a reject with the correct part ID (subTxId=0)
    // rather than a reject with subTxId=1 after already pushing the table creation part
    if (!createDescription.HasSchemaPresetName() && createDescription.HasSchema()) {
        if (AppData()->FeatureFlags.GetEnableLocalIndexAsSchemeObject()) {
            const auto& schema = createDescription.GetSchema();
            if (schema.IndexesSize()) {
                TSimpleErrorCollector errors;
                TOlapSchema tableSchema;
                if (!tableSchema.ParseFromProto(schema, errors, AppData()->ColumnShardConfig.GetAllowNullableColumnsInPK())) {
                    TString msg = errors->Ok() ? TString("Failed to parse column table schema") : errors->GetErrorMessage();
                    return {CreateReject(nextId, NKikimrScheme::StatusSchemeError, msg)};
                }

                NKikimrSchemeOp::TColumnTableSchema normalizedSchema;
                tableSchema.Serialize(normalizedSchema);

                auto columnIdToName = NOlap::BuildColumnIdToNameMap(normalizedSchema);

                // Validate all indexes before creating any operations
                for (const auto& indexProto : normalizedSchema.GetIndexes()) {
                    NKikimrSchemeOp::TIndexCreationConfig indexConfig;
                    if (!NOlap::ConvertOlapIndexToCreationConfig(indexProto, columnIdToName, indexConfig)) {
                        return {CreateReject(nextId, NKikimrScheme::StatusSchemeError,
                            TStringBuilder() << "Failed to convert index '" << indexProto.GetName() << "' to creation config")};
                    }
                }
            }
        }
    }

    // Now that validation has passed, push the table creation operation
    result.push_back(CreateNewColumnTable(NextPartId(nextId, result), tx));

    if (createDescription.HasSchemaPresetName() || !createDescription.HasSchema()) {
        return result;
    }

    if (!AppData()->FeatureFlags.GetEnableLocalIndexAsSchemeObject()) {
        return result;
    }

    const auto& schema = createDescription.GetSchema();
    if (!schema.IndexesSize()) {
        return result;
    }

    // Re-parse the schema (we already validated it above, so this should succeed)
    TSimpleErrorCollector errors;
    TOlapSchema tableSchema;
    if (!tableSchema.ParseFromProto(schema, errors, AppData()->ColumnShardConfig.GetAllowNullableColumnsInPK())) {
        TString msg = errors->Ok() ? TString("Failed to parse column table schema after validation") : errors->GetErrorMessage();
        return {CreateReject(NextPartId(nextId, result), NKikimrScheme::StatusSchemeError, msg)};
    }

    NKikimrSchemeOp::TColumnTableSchema normalizedSchema;
    tableSchema.Serialize(normalizedSchema);

    auto columnIdToName = NOlap::BuildColumnIdToNameMap(normalizedSchema);

    for (const auto& indexProto : normalizedSchema.GetIndexes()) {
        NKikimrSchemeOp::TIndexCreationConfig indexConfig;
        if (!NOlap::ConvertOlapIndexToCreationConfig(indexProto, columnIdToName, indexConfig)) {
            return {CreateReject(NextPartId(nextId, result), NKikimrScheme::StatusSchemeError,
                TStringBuilder() << "Failed to convert index '" << indexProto.GetName() << "' to creation config after validation")};
        }

        auto scheme = TransactionTemplate(
            workingDir + "/" + tableName,
            NKikimrSchemeOp::EOperationType::ESchemeOpCreateTableIndex);
        scheme.SetInternal(true);
        *scheme.MutableCreateTableIndex() = std::move(indexConfig);

        result.push_back(CreateNewLocalIndex(NextPartId(nextId, result), scheme));
    }

    return result;
}

} // namespace NKikimr::NSchemeShard
