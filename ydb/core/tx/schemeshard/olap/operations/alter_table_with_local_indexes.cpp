#include <ydb/core/tx/schemeshard/schemeshard__operation_part.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_common.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

#include <ydb/core/tx/schemeshard/olap/operations/local_index_helpers.h>

namespace NKikimr::NSchemeShard {

TVector<ISubOperation::TPtr> AlterColumnTableWithLocalIndexes(TOperationId nextId, const TTxTransaction& tx, TOperationContext& context) {
    TVector<ISubOperation::TPtr> result;

    {
        TString errStr;
        if (!context.SS->CheckApplyIf(tx, errStr)) {
            return {CreateReject(nextId, NKikimrScheme::EStatus::StatusPreconditionFailed, errStr)};
        }
    }

    const TString& parentPathStr = tx.GetWorkingDir();
    TString tableName;
    if (tx.HasAlterColumnTable()) {
        tableName = tx.GetAlterColumnTable().GetName();
    } else if (tx.HasAlterTable()) {
        tableName = tx.GetAlterTable().GetName();
    }

    TPath tablePath = TPath::Resolve(parentPathStr, context.SS).Dive(tableName);
    if (!tablePath.IsResolved() || tablePath.IsDeleted() || !tablePath->IsColumnTable()) {
        result.push_back(CreateAlterColumnTable(NextPartId(nextId, result), tx));
        return result;
    }

    THashSet<TString> existingIndexNames;
    for (const auto& [childName, childPathId] : tablePath.Base()->GetChildren()) {
        auto it = context.SS->PathsById.find(childPathId);
        if (it != context.SS->PathsById.end() && it->second->IsTableIndex() && !it->second->Dropped()) {
            existingIndexNames.insert(childName);
        }
    }

    bool hasAlterSchema = tx.HasAlterColumnTable() && tx.GetAlterColumnTable().HasAlterSchema();
    THashSet<TString> newIndexNames;
    THashSet<TString> droppedIndexNames;
    THashSet<std::pair<TString, TString>> movedIndexNames; // source -> destination

    if (hasAlterSchema) {
        const auto& alterSchema = tx.GetAlterColumnTable().GetAlterSchema();

        for (const auto& upsertIdx : alterSchema.GetUpsertIndexes()) {
            newIndexNames.insert(upsertIdx.GetName());
        }

        for (const auto& dropIdx : alterSchema.GetDropIndexes()) {
            droppedIndexNames.insert(dropIdx);
        }

        for (const auto& moveIdx : alterSchema.GetMoveIndex()) {
            movedIndexNames.insert({moveIdx.GetSourceName(), moveIdx.GetDestinationName()});
        }

        // Check for conflicts: same index name in both UpsertIndexes and DropIndexes
        for (const auto& indexName : newIndexNames) {
            if (droppedIndexNames.contains(indexName)) {
                return {CreateReject(NextPartId(nextId, result), NKikimrScheme::StatusSchemeError,
                    TStringBuilder() << "Index '" << indexName << "' appears in both UpsertIndexes and DropIndexes")};
            }
        }
    }

    bool hasIndexChanges = !newIndexNames.empty() || !droppedIndexNames.empty() || !movedIndexNames.empty();

    if (!hasIndexChanges || !AppData()->FeatureFlags.GetEnableLocalIndexAsSchemeObject()) {
        result.push_back(CreateAlterColumnTable(NextPartId(nextId, result), tx));
        return result;
    }

    // Validate all index upserts and drops before pushing any operation parts
    TVector<std::pair<TString, NKikimrSchemeOp::TIndexCreationConfig>> validatedIndexConfigs;
    THashSet<TString> validatedDropIndexes;

    if (hasAlterSchema) {
        const auto& alterSchema = tx.GetAlterColumnTable().GetAlterSchema();

        // Validate all upsert indexes first
        for (const auto& upsertIdx : alterSchema.GetUpsertIndexes()) {
            const TString& indexName = upsertIdx.GetName();

            NKikimrSchemeOp::TIndexCreationConfig indexConfig;
            if (!NOlap::ConvertRequestedIndexToCreationConfig(upsertIdx, indexConfig)) {
                return {CreateReject(nextId, NKikimrScheme::StatusSchemeError,
                    TStringBuilder() << "Failed to convert index '" << indexName << "' to creation config")};
            }

            validatedIndexConfigs.emplace_back(indexName, std::move(indexConfig));
        }

        // Validate all drop indexes
        for (const auto& dropIdx : alterSchema.GetDropIndexes()) {
            if (!existingIndexNames.contains(dropIdx)) {
                return {CreateReject(nextId, NKikimrScheme::StatusSchemeError,
                    TStringBuilder() << "Cannot drop index '" << dropIdx << "' because it does not exist")};
            }

            validatedDropIndexes.insert(dropIdx);
        }

        // Validate all move indexes (RENAME INDEX operations)
        for (const auto& moveIdx : alterSchema.GetMoveIndex()) {
            const TString& sourceName = moveIdx.GetSourceName();
            const TString& destinationName = moveIdx.GetDestinationName();

            // Check if source index exists
            if (!existingIndexNames.contains(sourceName)) {
                return {CreateReject(nextId, NKikimrScheme::StatusSchemeError,
                    TStringBuilder() << "Cannot rename index '" << sourceName << "' because it does not exist")};
            }

            // Check if destination index already exists (unless replace_destination is set)
            if (existingIndexNames.contains(destinationName) && !moveIdx.GetReplaceDestination()) {
                return {CreateReject(nextId, NKikimrScheme::StatusSchemeError,
                    TStringBuilder() << "Cannot rename index '" << sourceName << "' to '" << destinationName
                    << "' because destination index already exists")};
            }

            // Get the source index configuration
            TPath srcIndexPath = tablePath.Child(sourceName);
            if (!srcIndexPath.IsResolved() || srcIndexPath.IsDeleted()) {
                return {CreateReject(nextId, NKikimrScheme::StatusSchemeError,
                    TStringBuilder() << "Source index '" << sourceName << "' not found")};
            }

            auto it = context.SS->PathsById.find(srcIndexPath->PathId);
            if (it == context.SS->PathsById.end() || !it->second->IsTableIndex()) {
                return {CreateReject(nextId, NKikimrScheme::StatusSchemeError,
                    TStringBuilder() << "Source index '" << sourceName << "' is not a table index")};
            }

            // Get the index configuration from the column table schema
            TPathId tablePathId = tablePath->PathId;
            if (!context.SS->ColumnTables.contains(tablePathId)) {
                return {CreateReject(nextId, NKikimrScheme::StatusSchemeError,
                    TStringBuilder() << "Column table not found")};
            }

            auto tableInfo = context.SS->ColumnTables.GetVerifiedPtr(tablePathId);
            const auto& schema = tableInfo->Description.GetSchema();

            // Find the index in the schema
            const NKikimrSchemeOp::TOlapIndexDescription* sourceIndexProto = nullptr;
            for (const auto& index : schema.GetIndexes()) {
                if (index.GetName() == sourceName) {
                    sourceIndexProto = &index;
                    break;
                }
            }

            if (!sourceIndexProto) {
                return {CreateReject(nextId, NKikimrScheme::StatusSchemeError,
                    TStringBuilder() << "Source index '" << sourceName << "' not found in table schema")};
            }

            // Build column ID to name map for conversion
            auto columnIdToName = NOlap::BuildColumnIdToNameMap(schema);

            // Create a new index config with the destination name
            NKikimrSchemeOp::TIndexCreationConfig indexConfig;
            if (!NOlap::ConvertOlapIndexToCreationConfig(*sourceIndexProto, columnIdToName, indexConfig)) {
                return {CreateReject(nextId, NKikimrScheme::StatusSchemeError,
                    TStringBuilder() << "Failed to convert index '" << sourceName << "' to creation config")};
            }

            // Override the name with the destination name
            indexConfig.SetName(destinationName);

            validatedIndexConfigs.emplace_back(destinationName, std::move(indexConfig));
            validatedDropIndexes.insert(sourceName);
        }
    }

    // All validations passed, now push the operation parts
    result.push_back(CreateAlterColumnTable(NextPartId(nextId, result), tx));

    if (hasAlterSchema) {
        // Create index operations for validated upsert indexes
        for (const auto& [indexName, indexConfig] : validatedIndexConfigs) {
            if (existingIndexNames.contains(indexName)) {
                auto scheme = TransactionTemplate(
                    parentPathStr + "/" + tableName,
                    NKikimrSchemeOp::EOperationType::ESchemeOpCreateTableIndex);
                scheme.SetInternal(true);
                *scheme.MutableCreateTableIndex() = indexConfig;

                result.push_back(CreateAlterLocalIndex(NextPartId(nextId, result), scheme));
            } else {
                auto scheme = TransactionTemplate(
                    parentPathStr + "/" + tableName,
                    NKikimrSchemeOp::EOperationType::ESchemeOpCreateTableIndex);
                scheme.SetInternal(true);
                *scheme.MutableCreateTableIndex() = indexConfig;

                result.push_back(CreateNewLocalIndex(NextPartId(nextId, result), scheme));
            }
        }

        // Create drop operations for validated drop indexes
        for (const auto& dropIdx : validatedDropIndexes) {
            auto scheme = TransactionTemplate(
                parentPathStr + "/" + tableName,
                NKikimrSchemeOp::EOperationType::ESchemeOpDropTableIndex);
            scheme.SetInternal(true);
            scheme.MutableDrop()->SetName(dropIdx);

            result.push_back(CreateDropLocalIndex(NextPartId(nextId, result), scheme));
        }
    }

    return result;
}

} // namespace NKikimr::NSchemeShard
