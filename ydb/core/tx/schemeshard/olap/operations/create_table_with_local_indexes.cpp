#include <ydb/core/tx/schemeshard/olap/common/common.h>
#include <ydb/core/tx/schemeshard/olap/operations/local_index_helpers.h>
#include <ydb/core/tx/schemeshard/olap/schema/schema.h>

namespace NKikimr::NSchemeShard {

TVector<ISubOperation::TPtr> CreateColumnTableWithLocalIndexes(TOperationId nextId, const TTxTransaction& tx, TOperationContext&) {
    TVector<ISubOperation::TPtr> result;

    const auto& createDescription = tx.GetCreateColumnTable();
    const TString& tableName = createDescription.GetName();
    const TString& workingDir = tx.GetWorkingDir();

    // Local-index scheme-object children are needed only when the table carries an inline
    // schema with indexes and the feature flag is on.
    const bool createLocalIndexes = !createDescription.HasSchemaPresetName()
        && createDescription.HasSchema()
        && AppData()->FeatureFlags.GetEnableLocalIndexAsSchemeObject()
        && createDescription.GetSchema().IndexesSize() > 0;

    // Parse + validate up-front so any reject is reported as part 0 (before pushing the
    // table-creation part). The validated configs are reused below to build the sub-ops.
    TVector<NKikimrSchemeOp::TIndexCreationConfig> indexConfigs;
    if (createLocalIndexes) {
        const auto& schema = createDescription.GetSchema();
        TSimpleErrorCollector errors;
        TOlapSchema tableSchema;
        if (!tableSchema.ParseFromProto(schema, errors, AppData()->ColumnShardConfig.GetAllowNullableColumnsInPK())) {
            TString msg = errors->Ok() ? TString("Failed to parse column table schema") : errors->GetErrorMessage();
            return {CreateReject(nextId, NKikimrScheme::StatusSchemeError, msg)};
        }

        NKikimrSchemeOp::TColumnTableSchema normalizedSchema;
        tableSchema.Serialize(normalizedSchema);
        const auto columnIdToName = NOlap::BuildColumnIdToNameMap(normalizedSchema);

        indexConfigs.reserve(normalizedSchema.GetIndexes().size());
        for (const auto& indexProto : normalizedSchema.GetIndexes()) {
            NKikimrSchemeOp::TIndexCreationConfig indexConfig;
            if (!NOlap::ConvertOlapIndexToCreationConfig(indexProto, columnIdToName, indexConfig)) {
                return {CreateReject(nextId, NKikimrScheme::StatusSchemeError,
                    TStringBuilder() << "Failed to convert index '" << indexProto.GetName() << "' to creation config")};
            }
            indexConfigs.push_back(std::move(indexConfig));
        }
    }

    result.push_back(CreateNewColumnTable(NextPartId(nextId, result), tx));

    for (auto& indexConfig : indexConfigs) {
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
