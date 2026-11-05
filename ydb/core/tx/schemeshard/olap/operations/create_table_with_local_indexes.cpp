#include <ydb/core/tx/schemeshard/olap/common/common.h>
#include <ydb/core/tx/schemeshard/olap/operations/local_index_helpers.h>
#include <ydb/core/tx/schemeshard/olap/schema/schema.h>

namespace NKikimr::NSchemeShard {

TVector<ISubOperation::TPtr> CreateColumnTableWithLocalIndexes(TOperationId nextId, const TTxTransaction& tx, TOperationContext& context) {
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
            if (indexProto.GetImplementationCase() == NKikimrSchemeOp::TOlapIndexDescription::kMaxIndex) {
                continue;
            }
            NKikimrSchemeOp::TIndexCreationConfig indexConfig;
            if (!NOlap::ConvertOlapIndexToCreationConfig(indexProto, columnIdToName, indexConfig)) {
                return {CreateReject(nextId, NKikimrScheme::StatusSchemeError,
                    TStringBuilder() << "Failed to convert index '" << indexProto.GetName() << "' to creation config")};
            }
            indexConfigs.push_back(std::move(indexConfig));
        }
    }

    const auto& sequences = createDescription.GetSerialSequences();
    if (!sequences.empty()) {
        const auto parentPath = TPath::Resolve(workingDir, context.SS);
        const auto tablePath = parentPath.Child(tableName);
        if (!parentPath.IsResolved() || (tablePath.IsResolved() && !tablePath.IsDeleted())) {
            // Let the table operation handle an existing destination, including IF NOT EXISTS.
            return {CreateNewColumnTable(nextId, tx)};
        }

        if (!AppData()->FeatureFlags.GetEnableColumnshardSerial()) {
            return {CreateReject(nextId, NKikimrScheme::StatusSchemeError, "Serial columns are disabled for column tables")};
        }

        THashSet<TString> childNames;
        for (const auto& config : indexConfigs) {
            childNames.insert(config.GetName());
        }

        for (const auto& [_, sequence] : sequences) {
            TString error;
            const auto& name = sequence.GetName();
            if (!tablePath.Child(name).IsValidLeafName(context.UserToken.Get(), error) ||
                !TSequenceInfo::ValidateCreate(sequence, error)) {
                return {CreateReject(nextId, NKikimrScheme::StatusSchemeError, error)};
            }

            if (!childNames.insert(name).second) {
                return {CreateReject(nextId, NKikimrScheme::StatusSchemeError, "Duplicate child name: " + name)};
            }
        }

        auto checks = parentPath.Check();
        checks.FailOnRestrictedCreateInTempZone(tx.GetAllowCreateInTempDir())
            .PathsLimit(1 + childNames.size());
        if (!checks) {
            return {CreateReject(nextId, checks.GetStatus(), checks.GetError())};
        }

        if (childNames.size() > parentPath.DomainInfo()->GetSchemeLimits().MaxChildrenInDir) {
            return {CreateReject(nextId, NKikimrScheme::StatusResourceExhausted, "Too many child paths for column table")};
        }
    }

    result.push_back(CreateNewColumnTable(NextPartId(nextId, result), tx));

    for (auto& indexConfig : indexConfigs) {
        auto scheme = TransactionTemplate(
            workingDir + "/" + tableName,
            NKikimrSchemeOp::EOperationType::ESchemeOpCreateTableIndex);
        scheme.SetInternal(true);
        *scheme.MutableCreateTableIndex() = std::move(indexConfig);

        result.push_back(CreateNewColumnTableLocalIndex(NextPartId(nextId, result), scheme));
    }

    for (const auto& key : createDescription.GetSchema().GetKeyColumnNames()) {
        const auto it = sequences.find(key);
        if (it != sequences.end()) {
            auto scheme = TransactionTemplate(workingDir + "/" + tableName, NKikimrSchemeOp::ESchemeOpCreateSequence);
            scheme.SetFailOnExist(tx.GetFailOnExist());
            scheme.SetAllowCreateInTempDir(tx.GetAllowCreateInTempDir());
            *scheme.MutableSequence() = it->second;
            result.push_back(CreateNewSequence(NextPartId(nextId, result), scheme));
        }
    }

    return result;
}

} // namespace NKikimr::NSchemeShard
