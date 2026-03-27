#include "schemeshard__operation_part.h"
#include "schemeshard__operation.h"
#include "schemeshard_impl.h"

#include <ydb/core/tx/schemeshard/olap/common/common.h>
#include <ydb/core/tx/schemeshard/olap/operations/local_index_helpers.h>
#include <ydb/core/tx/schemeshard/olap/schema/schema.h>
#include <ydb/core/tx/schemeshard/olap/schema/update.h>

namespace NKikimr::NSchemeShard {

TVector<ISubOperation::TPtr> CreateColumnTableWithIndexes(TOperationId nextId, const TTxTransaction& tx, TOperationContext&) {
    TVector<ISubOperation::TPtr> result;

    const auto& createDescription = tx.GetCreateColumnTable();
    const TString& tableName = createDescription.GetName();
    const TString& workingDir = tx.GetWorkingDir();

    {
        result.push_back(CreateNewColumnTable(NextPartId(nextId, result), tx));
    }

    if (createDescription.HasSchemaPresetName() || !createDescription.HasSchema()) {
        return result;
    }

    const auto& schema = createDescription.GetSchema();
    if (!schema.IndexesSize()) {
        return result;
    }

    TSimpleErrorCollector errors;
    TOlapSchemaUpdate schemaDiff;
    const bool allowNullKeys = AppData()->ColumnShardConfig.GetAllowNullableColumnsInPK();
    if (!schemaDiff.Parse(schema, errors, allowNullKeys)) {
        TString msg = errors->Ok() ? TString("Failed to parse column table schema for local indexes")
                                   : errors->GetErrorMessage();
        return {CreateReject(NextPartId(nextId, result), NKikimrScheme::StatusSchemeError, msg)};
    }

    TOlapSchema tableSchema;
    if (!tableSchema.Update(schemaDiff, errors)) {
        TString msg = errors->Ok() ? TString("Failed to build column table schema for local indexes")
                                   : errors->GetErrorMessage();
        return {CreateReject(NextPartId(nextId, result), NKikimrScheme::StatusSchemeError, msg)};
    }
    tableSchema.ParseIndexesFromFullSchema(schema);

    NKikimrSchemeOp::TColumnTableSchema normalizedSchema;
    tableSchema.Serialize(normalizedSchema);

    auto columnIdToName = NOlap::BuildColumnIdToNameMap(normalizedSchema);

    for (const auto& indexProto : normalizedSchema.GetIndexes()) {
        NKikimrSchemeOp::TIndexCreationConfig indexConfig;
        if (!NOlap::ConvertOlapIndexToCreationConfig(indexProto, columnIdToName, indexConfig)) {
            continue;
        }

        auto scheme = TransactionTemplate(
            workingDir + "/" + tableName,
            NKikimrSchemeOp::EOperationType::ESchemeOpCreateTableIndex);
        scheme.SetInternal(true);
        *scheme.MutableCreateTableIndex() = std::move(indexConfig);

        result.push_back(CreateNewColumnTableIndex(NextPartId(nextId, result), scheme));
    }

    return result;
}

TVector<ISubOperation::TPtr> AlterColumnTableWithIndexes(TOperationId nextId, const TTxTransaction& tx, TOperationContext& context) {
    TVector<ISubOperation::TPtr> result;

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

    if (hasAlterSchema) {
        const auto& alterSchema = tx.GetAlterColumnTable().GetAlterSchema();

        for (const auto& upsertIdx : alterSchema.GetUpsertIndexes()) {
            newIndexNames.insert(upsertIdx.GetName());
        }

        for (const auto& dropIdx : alterSchema.GetDropIndexes()) {
            droppedIndexNames.insert(dropIdx);
        }
    }

    bool hasIndexChanges = !newIndexNames.empty() || !droppedIndexNames.empty();

    if (!hasIndexChanges) {
        result.push_back(CreateAlterColumnTable(NextPartId(nextId, result), tx));
        return result;
    }

    {
        result.push_back(CreateAlterColumnTable(NextPartId(nextId, result), tx));
    }

    if (hasAlterSchema) {
        const auto& alterSchema = tx.GetAlterColumnTable().GetAlterSchema();

        for (const auto& upsertIdx : alterSchema.GetUpsertIndexes()) {
            const TString& indexName = upsertIdx.GetName();

            NKikimrSchemeOp::TIndexCreationConfig indexConfig;
            if (!NOlap::ConvertRequestedIndexToCreationConfig(upsertIdx, indexConfig)) {
                continue;
            }

            if (existingIndexNames.contains(indexName)) {
                auto scheme = TransactionTemplate(
                    parentPathStr + "/" + tableName,
                    NKikimrSchemeOp::EOperationType::ESchemeOpCreateTableIndex);
                scheme.SetInternal(true);
                *scheme.MutableCreateTableIndex() = std::move(indexConfig);

                result.push_back(CreateAlterColumnTableIndex(NextPartId(nextId, result), scheme));
            } else {
                auto scheme = TransactionTemplate(
                    parentPathStr + "/" + tableName,
                    NKikimrSchemeOp::EOperationType::ESchemeOpCreateTableIndex);
                scheme.SetInternal(true);
                *scheme.MutableCreateTableIndex() = std::move(indexConfig);

                result.push_back(CreateNewColumnTableIndex(NextPartId(nextId, result), scheme));
            }
        }

        for (const auto& dropIdx : alterSchema.GetDropIndexes()) {
            if (!existingIndexNames.contains(dropIdx)) {
                continue;
            }

            auto scheme = TransactionTemplate(
                parentPathStr + "/" + tableName,
                NKikimrSchemeOp::EOperationType::ESchemeOpDropTableIndex);
            scheme.SetInternal(true);
            scheme.MutableDrop()->SetName(dropIdx);

            result.push_back(CreateDropColumnTableIndex(NextPartId(nextId, result), scheme));
        }
    }

    return result;
}

} // namespace NKikimr::NSchemeShard
