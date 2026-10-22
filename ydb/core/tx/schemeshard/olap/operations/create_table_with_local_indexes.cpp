#include <ydb/core/tx/schemeshard/olap/common/common.h>
#include <ydb/core/tx/schemeshard/olap/operations/local_index_helpers.h>
#include <ydb/core/tx/schemeshard/olap/schema/schema.h>
#include <ydb/core/tx/schemeshard/index/index_utils.h>

namespace NKikimr::NSchemeShard {

namespace {

NKikimrSchemeOp::TTableDescription ColumnSchemaAsRowTable(const NKikimrSchemeOp::TColumnTableDescription& columnTable) {
    NKikimrSchemeOp::TTableDescription base;
    base.SetName(columnTable.GetName());
    for (const auto& column : columnTable.GetSchema().GetColumns()) {
        auto* dst = base.AddColumns();
        dst->SetName(column.GetName());
        if (column.HasType()) {
            dst->SetType(column.GetType());
        }
        if (column.HasTypeId()) {
            dst->SetTypeId(column.GetTypeId());
        }
        if (column.HasTypeInfo()) {
            *dst->MutableTypeInfo() = column.GetTypeInfo();
        }
        dst->SetNotNull(column.GetNotNull());
    }
    for (const auto& key : columnTable.GetSchema().GetKeyColumnNames()) {
        base.AddKeyColumnNames(key);
    }
    return base;
}

NKikimrSchemeOp::TIndexCreationConfig GlobalSyncIndexConfig(const NKikimrSchemeOp::TIndexDescription& indexDesc) {
    NKikimrSchemeOp::TIndexCreationConfig config;
    config.SetName(indexDesc.GetName());
    config.SetType(NKikimrSchemeOp::EIndexTypeGlobal);
    config.SetState(NKikimrSchemeOp::EIndexStateReady);
    for (const auto& column : indexDesc.GetKeyColumnNames()) {
        config.AddKeyColumnNames(column);
    }
    for (const auto& column : indexDesc.GetDataColumnNames()) {
        config.AddDataColumnNames(column);
    }
    return config;
}

} // namespace

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

    struct TGlobalSyncIndex {
        NKikimrSchemeOp::TIndexCreationConfig Config;
        NTableIndex::TTableColumns ImplColumns;
    };
    TVector<TGlobalSyncIndex> globalIndexes;
    if (createDescription.TableIndexesSize() > 0) {
        if (!createDescription.HasSchema() || createDescription.GetSchema().ColumnsSize() == 0) {
            return {CreateReject(nextId, NKikimrScheme::StatusInvalidParameter,
                "A synchronous index on a column table requires an inline schema")};
        }

        TPath workingDirPath = TPath::Resolve(workingDir, context.SS);
        if (workingDirPath.IsEmpty()) {
            return {CreateReject(nextId, NKikimrScheme::StatusPathDoesNotExist, "parent path hasn't been resolved")};
        }

        const auto baseTableDescription = ColumnSchemaAsRowTable(createDescription);
        THashSet<TString> indexNames;
        for (const auto& local : indexConfigs) {
            indexNames.insert(local.GetName());
        }

        globalIndexes.reserve(createDescription.TableIndexesSize());
        for (const auto& indexDesc : createDescription.GetTableIndexes()) {
            if (indexDesc.GetType() != NKikimrSchemeOp::EIndexTypeGlobal) {
                return {CreateReject(nextId, NKikimrScheme::StatusInvalidParameter,
                    "Only global synchronous indexes are supported for column tables")};
            }
            if (indexNames.contains(indexDesc.GetName())) {
                return {CreateReject(nextId, NKikimrScheme::StatusInvalidParameter,
                    TStringBuilder() << "Can't create indexes with not unique names for table, for example: " << indexDesc.GetName())};
            }
            indexNames.insert(indexDesc.GetName());

            auto config = GlobalSyncIndexConfig(indexDesc);
            NTableIndex::TTableColumns implColumns;
            NKikimrScheme::EStatus status;
            TString error;
            if (!NTableIndex::CommonCheck(baseTableDescription, config, workingDirPath.DomainInfo()->GetSchemeLimits(),
                    false, implColumns, status, error)) {
                return {CreateReject(nextId, status, error)};
            }
            globalIndexes.push_back(TGlobalSyncIndex{std::move(config), std::move(implColumns)});
        }
    }

    // TableIndexes is the create-time request. Describe fills the same field from children.
    auto tableTx = tx;
    tableTx.MutableCreateColumnTable()->ClearTableIndexes();
    result.push_back(CreateNewColumnTable(NextPartId(nextId, result), tableTx));

    for (auto& indexConfig : indexConfigs) {
        auto scheme = TransactionTemplate(
            workingDir + "/" + tableName,
            NKikimrSchemeOp::EOperationType::ESchemeOpCreateTableIndex);
        scheme.SetInternal(true);
        *scheme.MutableCreateTableIndex() = std::move(indexConfig);

        result.push_back(CreateNewColumnTableLocalIndex(NextPartId(nextId, result), scheme));
    }

    for (auto& index : globalIndexes) {
        const TString indexName = index.Config.GetName();
        auto indexScheme = TransactionTemplate(
            workingDir + "/" + tableName,
            NKikimrSchemeOp::EOperationType::ESchemeOpCreateTableIndex);
        indexScheme.SetInternal(true);
        *indexScheme.MutableCreateTableIndex() = std::move(index.Config);
        result.push_back(CreateNewTableIndex(NextPartId(nextId, result), indexScheme));

        NKikimrSchemeOp::TTableDescription userIndexDesc;
        auto implScheme = TransactionTemplate(
            workingDir + "/" + tableName + "/" + indexName,
            NKikimrSchemeOp::EOperationType::ESchemeOpCreateTable);
        implScheme.SetInternal(true);
        *implScheme.MutableCreateTable() = NTableIndex::CalcImplTableDesc(
            ColumnSchemaAsRowTable(createDescription), index.ImplColumns, userIndexDesc, 0);
        result.push_back(CreateNewTable(NextPartId(nextId, result), implScheme));
    }

    return result;
}

} // namespace NKikimr::NSchemeShard
