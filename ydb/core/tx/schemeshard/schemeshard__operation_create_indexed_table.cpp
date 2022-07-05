#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_path_element.h"
#include "schemeshard_utils.h"

#include "schemeshard_impl.h"

#include <ydb/core/base/table_index.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NTableIndex;

}

namespace NKikimr {
namespace NSchemeShard {

TVector<ISubOperationBase::TPtr> CreateIndexedTable(TOperationId nextId, const TTxTransaction& tx, TOperationContext& context) {
    Y_VERIFY(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpCreateIndexedTable);

    auto indexedTable = tx.GetCreateIndexedTable();
    const NKikimrSchemeOp::TTableDescription& baseTableDescription = indexedTable.GetTableDescription();

    ui32 indexesCount = indexedTable.IndexDescriptionSize();
    ui32 indexedTableShards = 0;
    for (const auto& desc : indexedTable.GetIndexDescription()) {
        if (desc.HasIndexImplTableDescription()) {
            indexedTableShards += TTableInfo::ShardsToCreate(desc.GetIndexImplTableDescription());
        } else {
            indexedTableShards += 1;
        }
    }
    ui32 sequencesCount = indexedTable.SequenceDescriptionSize();
    ui32 baseShards = TTableInfo::ShardsToCreate(baseTableDescription);
    ui32 shardsToCreate = baseShards + indexedTableShards;
    ui32 pathToCreate = 1 + indexesCount * 2 + sequencesCount;

    TPath workingDir = TPath::Resolve(tx.GetWorkingDir(), context.SS);
    if (workingDir.IsEmpty()) {
        TString msg = "parent path hasn't been resolved";
        return {CreateReject(nextId, NKikimrScheme::EStatus::StatusPathDoesNotExist, msg)};
    }

    TPath baseTablePath = workingDir.Child(baseTableDescription.GetName());
    {
        TString msg = "invalid table name: ";
        if(!baseTablePath.IsValidLeafName(msg)) {
            return {CreateReject(nextId, NKikimrScheme::EStatus::StatusSchemeError, msg)};
        }
    }

    if (baseTableDescription.GetIsBackup()) {
        return {CreateReject(nextId, NKikimrScheme::StatusInvalidParameter, "Cannot create table with explicit 'IsBackup' property")};
    }

    TSubDomainInfo::TPtr domainInfo = baseTablePath.DomainInfo();

    if (sequencesCount > 0 && domainInfo->GetSequenceShards().empty()) {
        ++shardsToCreate;
    }

    LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TCreateTableIndex construct operation "
                    << " table path: " << baseTablePath.PathString()
                    << " domain path id: " << baseTablePath.GetPathIdForDomain()
                    << " domain path: " << TPath::Init(baseTablePath.GetPathIdForDomain(), context.SS).PathString()
                    << " shardsToCreate: " << shardsToCreate
                    << " GetShardsInside: " << domainInfo->GetShardsInside()
                    << " MaxShards: " << domainInfo->GetSchemeLimits().MaxShards);

    if (domainInfo->GetShardsInside() + shardsToCreate > domainInfo->GetSchemeLimits().MaxShards) {
        auto msg = TStringBuilder() << "shards count has reached maximum value in the domain"
                                    << ", paths limit for domain: " << domainInfo->GetSchemeLimits().MaxShards
                                    << ", paths count inside domain: " << domainInfo->GetShardsInside()
                                    << ", intention to create new paths: " << shardsToCreate;
        return {CreateReject(nextId, NKikimrScheme::EStatus::StatusResourceExhausted, msg)};
    }

    if (indexesCount > domainInfo->GetSchemeLimits().MaxTableIndices) {
        auto msg = TStringBuilder() << "indexes count has reached maximum value in the table"
                                    << ", children limit for dir in domain: " << domainInfo->GetSchemeLimits().MaxTableIndices
                                    << ", intention to create new children: " << indexesCount;
        return {CreateReject(nextId, NKikimrScheme::EStatus::StatusResourceExhausted, msg)};
    }

    if (domainInfo->GetPathsInside() + pathToCreate > domainInfo->GetSchemeLimits().MaxPaths) {
        auto msg = TStringBuilder() << "paths count has reached maximum value in the domain"
                                    << ", paths limit for domain: " << domainInfo->GetSchemeLimits().MaxPaths
                                    << ", paths count inside domain: " << domainInfo->GetPathsInside()
                                    << ", intention to create new paths: " << pathToCreate;
        return {CreateReject(nextId, NKikimrScheme::EStatus::StatusResourceExhausted, msg)};
    }

    if (baseShards > domainInfo->GetSchemeLimits().MaxShardsInPath) {
        auto msg = TStringBuilder()  << "shards count has reached maximum value in the path"
                                    << ", shards limit for path: " << domainInfo->GetSchemeLimits().MaxShardsInPath
                                    << ", intention to create new shards: " << baseShards;
        return {CreateReject(nextId, NKikimrScheme::EStatus::StatusResourceExhausted, msg)};
    }

    THashMap<TString, TTableColumns> indexes;

    TTableColumns baseTableColumns = ExtractInfo(baseTableDescription);
    for (auto& indexDescription: indexedTable.GetIndexDescription()) {
        const auto& indexName = indexDescription.GetName();
        bool uniformIndexTable = false;
        if (indexDescription.HasIndexImplTableDescription()) {
            if (indexDescription.GetIndexImplTableDescription().HasUniformPartitionsCount()) {
                uniformIndexTable = true;
            }
        }

        TPath indexPath = baseTablePath.Child(indexName);
        {
            TString msg = "invalid table index name: ";
            if (!indexPath.IsValidLeafName(msg)) {
                return {CreateReject(nextId, NKikimrScheme::EStatus::StatusSchemeError, msg)};
            }
        }

        if (indexes.contains(indexName)) {
            TString msg = TStringBuilder() << "Can't create indexes with not unique names for table, for example: " << indexDescription.GetName();
            return {CreateReject(nextId, NKikimrScheme::EStatus::StatusInvalidParameter, msg)};
        }

        switch (indexDescription.GetType()) {
        case NKikimrSchemeOp::EIndexType::EIndexTypeGlobalAsync:
            if (!context.SS->EnableAsyncIndexes) {
                TString msg = TStringBuilder() << "It is not allowed to create async indexes";
                return {CreateReject(nextId, NKikimrScheme::EStatus::StatusPreconditionFailed, msg)};
            } else if (baseTableDescription.HasTTLSettings() && !AppData()->FeatureFlags.GetEnableTtlOnAsyncIndexedTables()) {
                TString msg = TStringBuilder() << "TTL is not currently supported on tables with async indices";
                return {CreateReject(nextId, NKikimrScheme::EStatus::StatusPreconditionFailed, msg)};
            }
            break;

        default:
            break;
        }

        TIndexColumns indexKeys = ExtractInfo(indexDescription);
        if (indexKeys.KeyColumns.empty()) {
            TString msg = TStringBuilder() << "no key colums in index creation config";
            return {CreateReject(nextId, NKikimrScheme::EStatus::StatusInvalidParameter, msg)};
        }

        if (!indexKeys.DataColumns.empty() && !AppData()->FeatureFlags.GetEnableDataColumnForIndexTable()) {
            TString msg = TStringBuilder() << "It is not allowed to create index with data column";
            return {CreateReject(nextId, NKikimrScheme::EStatus::StatusPreconditionFailed, msg)};
        }

        TString explainErr;
        if (!IsCompatibleIndex(baseTableColumns, indexKeys, explainErr)) {
            TString msg = TStringBuilder() << "IsCompatibleIndex fail with explain: " << explainErr;
            return {CreateReject(nextId, NKikimrScheme::EStatus::StatusInvalidParameter, msg)};
        }

        TTableColumns impTableColumns = CalcTableImplDescription(baseTableColumns, indexKeys);

        TColumnTypes columnsTypes;
        if (!ExtractTypes(baseTableDescription, columnsTypes, explainErr)) {
            TString msg = TStringBuilder() << "ExtractTypes fail with explain: " << explainErr;
            return {CreateReject(nextId, NKikimrScheme::EStatus::StatusInvalidParameter, msg)};
        }

        if (!IsCompatibleKeyTypes(columnsTypes, impTableColumns, uniformIndexTable, explainErr)) {
            TString msg = TStringBuilder() << "IsCompatibleKeyTypes fail with explain: " << explainErr;
            return {CreateReject(nextId, NKikimrScheme::EStatus::StatusInvalidParameter, msg)};
        }

        if (impTableColumns.Keys.size() > domainInfo->GetSchemeLimits().MaxTableKeyColumns) {
            TString msg = TStringBuilder()
                << "Too many key indexed, index table reaches the limit of the maximum keys colums count"
                << ": indexing colums: " << indexKeys.KeyColumns.size()
                << ": requested keys colums for index table: " << impTableColumns.Keys.size()
                << ". Limit: " << domainInfo->GetSchemeLimits().MaxTableKeyColumns;
            return {CreateReject(nextId, NKikimrScheme::EStatus::StatusSchemeError, msg)};
        }

        indexes.emplace(indexName, std::move(impTableColumns));
    }

    THashSet<TString> sequences;
    for (auto& sequenceDescription : indexedTable.GetSequenceDescription()) {
        const auto& sequenceName = sequenceDescription.GetName();

        TPath sequencePath = baseTablePath.Child(sequenceName);
        {
            TString msg = "invalid sequence name: ";
            if (!sequencePath.IsValidLeafName(msg)) {
                return {CreateReject(nextId, NKikimrScheme::EStatus::StatusSchemeError, msg)};
            }
        }

        if (indexes.contains(sequenceName) || sequences.contains(sequenceName)) {
            TString msg = TStringBuilder() << "Can't create sequences with non-unique names for table, for example: " << sequenceName;
            return {CreateReject(nextId, NKikimrScheme::EStatus::StatusInvalidParameter, msg)};
        }

        TString errStr;
        if (!TSequenceInfo::ValidateCreate(sequenceDescription, errStr)) {
            return {CreateReject(nextId, NKikimrScheme::EStatus::StatusSchemeError, errStr)};
        }

        sequences.emplace(sequenceName);
    }

    THashSet<TString> keys;
    for (const TString& key : baseTableColumns.Keys) {
        keys.insert(key);
    }

    for (auto& column : baseTableDescription.GetColumns()) {
        if (column.GetNotNull() && !AppData()->FeatureFlags.GetEnableNotNullColumns()) {
            TString msg = TStringBuilder() << "It is not allowed to create not null column";
            return {CreateReject(nextId, NKikimrScheme::EStatus::StatusPreconditionFailed, msg)};
        }

        if (column.HasDefaultFromSequence()) {
            const TString& sequenceName = column.GetDefaultFromSequence();

            if (sequenceName.StartsWith('/')) {
                TString msg = TStringBuilder()
                    << "Using non-local sequences in tables not supported, e.g. column '"
                    << column.GetName() << "' using sequence '" << sequenceName << "'";
                return {CreateReject(nextId, NKikimrScheme::EStatus::StatusInvalidParameter, msg)};
            }

            if (!sequences.contains(sequenceName)) {
                TString msg = TStringBuilder()
                    << "Cannot specify default from an unknown sequence, e.g. column '"
                    << column.GetName() << "' using sequence '" << sequenceName << "'";
                return {CreateReject(nextId, NKikimrScheme::EStatus::StatusInvalidParameter, msg)};
            }

            if (!keys.contains(column.GetName())) {
                TString msg = TStringBuilder()
                    << "Cannot specify default from sequence from non-key columns, e.g. column'"
                    << column.GetName() << "' using sequence '" << sequenceName << "'";
                return {CreateReject(nextId, NKikimrScheme::EStatus::StatusInvalidParameter, msg)};
            }
        }
    }

    TVector<ISubOperationBase::TPtr> result;

    {
        auto scheme = TransactionTemplate(tx.GetWorkingDir(), NKikimrSchemeOp::EOperationType::ESchemeOpCreateTable);
        scheme.SetFailOnExist(tx.GetFailOnExist());

        scheme.MutableCreateTable()->CopyFrom(baseTableDescription);
        if (tx.HasAlterUserAttributes()) {
            scheme.MutableAlterUserAttributes()->CopyFrom(tx.GetAlterUserAttributes());
        }

        result.push_back(CreateNewTable(NextPartId(nextId, result), scheme, sequences));
    }

    for (auto& indexDescription: indexedTable.GetIndexDescription()) {
        {
            auto scheme = TransactionTemplate(
                tx.GetWorkingDir() + "/" + baseTableDescription.GetName(),
                NKikimrSchemeOp::EOperationType::ESchemeOpCreateTableIndex);
            scheme.SetFailOnExist(tx.GetFailOnExist());

            scheme.MutableCreateTableIndex()->CopyFrom(indexDescription);
            if (!indexDescription.HasType()) {
                scheme.MutableCreateTableIndex()->SetType(NKikimrSchemeOp::EIndexTypeGlobal);
            }

            result.push_back(CreateNewTableIndex(NextPartId(nextId, result), scheme));
        }

        {
            auto scheme = TransactionTemplate(
                tx.GetWorkingDir() + "/" + baseTableDescription.GetName() + "/" + indexDescription.GetName(),
                NKikimrSchemeOp::EOperationType::ESchemeOpCreateTable);
            scheme.SetFailOnExist(tx.GetFailOnExist());

            TTableColumns impTableColumns = indexes.at(indexDescription.GetName());

            auto& indexImplTableDescription = *scheme.MutableCreateTable();
            // This description provided by user to override partition policy
            const auto& userIndexDesc = indexDescription.GetIndexImplTableDescription();
            indexImplTableDescription = CalcImplTableDesc(baseTableDescription, impTableColumns, userIndexDesc);

            result.push_back(CreateNewTable(NextPartId(nextId, result), scheme));
        }
    }

    for (auto& sequenceDescription : indexedTable.GetSequenceDescription()) {
        auto scheme = TransactionTemplate(
            tx.GetWorkingDir() + "/" + baseTableDescription.GetName(),
            NKikimrSchemeOp::EOperationType::ESchemeOpCreateSequence);
        scheme.SetFailOnExist(tx.GetFailOnExist());

        *scheme.MutableSequence() = sequenceDescription;

        result.push_back(CreateNewSequence(NextPartId(nextId, result), scheme));
    }

    return result;
}

}
}
