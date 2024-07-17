#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"
#include "schemeshard_path_element.h"
#include "schemeshard_utils.h"

#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

namespace NKikimr::NSchemeShard {

using namespace NTableIndex;

TVector<ISubOperation::TPtr> CreateIndexedTable(TOperationId nextId, const TTxTransaction& tx, TOperationContext& context) {
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpCreateIndexedTable);

    auto indexedTable = tx.GetCreateIndexedTable();
    const NKikimrSchemeOp::TTableDescription& baseTableDescription = indexedTable.GetTableDescription();

    ui32 indexesCount = indexedTable.IndexDescriptionSize();
    ui32 indexedTableShards = 0;
    for (const auto& desc : indexedTable.GetIndexDescription()) {
        if (desc.IndexImplTableDescriptionsSize()) {
            indexedTableShards += TTableInfo::ShardsToCreate(desc.GetIndexImplTableDescriptions(0));
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

    {
        auto checks = workingDir.Check();
        checks
            .IsResolved()
            .FailOnRestrictedCreateInTempZone(tx.GetAllowCreateInTempDir());

        if (!checks) {
            return {CreateReject(nextId, checks.GetStatus(), checks.GetError())};
        }
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

    if (indexesCount > domainInfo->GetSchemeLimits().MaxTableIndices) {
        auto msg = TStringBuilder() << "indexes count has reached maximum value in the table"
                                    << ", children limit for dir in domain: " << domainInfo->GetSchemeLimits().MaxTableIndices
                                    << ", intention to create new children: " << indexesCount;
        return {CreateReject(nextId, NKikimrScheme::EStatus::StatusResourceExhausted, msg)};
    }

    {
        auto checks = baseTablePath.Check();
        checks
            .PathShardsLimit(baseShards)
            .PathsLimit(pathToCreate)
            .ShardsLimit(shardsToCreate);

        if (!checks) {
            return {CreateReject(nextId, NKikimrScheme::EStatus::StatusResourceExhausted, checks.GetError())};
        }
    }

    THashMap<TString, TTableColumns> indexes;

    TTableColumns baseTableColumns = ExtractInfo(baseTableDescription);
    for (auto& indexDescription: indexedTable.GetIndexDescription()) {
        const auto& indexName = indexDescription.GetName();
        bool uniformIndexTable = false;
        if (indexDescription.IndexImplTableDescriptionsSize()) {
            if (indexDescription.GetIndexImplTableDescriptions(0).HasUniformPartitionsCount()) {
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

        TTableColumns implTableColumns;
        NKikimrScheme::EStatus status;
        TString errStr;
        if (!CommonCheck(baseTableDescription, indexDescription, domainInfo->GetSchemeLimits(), uniformIndexTable, implTableColumns, status, errStr)) {
            return {CreateReject(nextId, status, errStr)};
        }

        indexes.emplace(indexName, std::move(implTableColumns));
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
        if (column.GetNotNull()) {
            bool isPrimaryKey =  keys.contains(column.GetName());

            if (isPrimaryKey && !AppData()->FeatureFlags.GetEnableNotNullColumns()) {
                TString msg = TStringBuilder() << "It is not allowed to create not null pk: " << column.GetName();
                return {CreateReject(nextId, NKikimrScheme::EStatus::StatusPreconditionFailed, msg)};
            }

            if (!isPrimaryKey && !AppData()->FeatureFlags.GetEnableNotNullDataColumns()) {
                TString msg = TStringBuilder() << "It is not allowed to create not null data column: " << column.GetName();
                return {CreateReject(nextId, NKikimrScheme::EStatus::StatusPreconditionFailed, msg)};
            }
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

    TVector<ISubOperation::TPtr> result;

    {
        auto scheme = TransactionTemplate(tx.GetWorkingDir(), NKikimrSchemeOp::EOperationType::ESchemeOpCreateTable);
        scheme.SetFailOnExist(tx.GetFailOnExist());
        scheme.SetAllowCreateInTempDir(tx.GetAllowCreateInTempDir());

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
            scheme.SetAllowCreateInTempDir(tx.GetAllowCreateInTempDir());
            scheme.SetInternal(tx.GetInternal());

            scheme.MutableCreateTableIndex()->CopyFrom(indexDescription);
            if (!indexDescription.HasType()) {
                scheme.MutableCreateTableIndex()->SetType(NKikimrSchemeOp::EIndexTypeGlobal);
            } else if (!AppData()->FeatureFlags.GetEnableUniqConstraint()) {
                if (indexDescription.GetType() == NKikimrSchemeOp::EIndexTypeGlobalUnique) {
                    TString msg = TStringBuilder() << "Unique constraint feature is disabled";
                    return {CreateReject(nextId, NKikimrScheme::EStatus::StatusPreconditionFailed, msg)};
                }
            }

            result.push_back(CreateNewTableIndex(NextPartId(nextId, result), scheme));
        }

        auto createIndexImplTable = [&] (const NKikimrSchemeOp::TTableDescription&& implTableDesc) {
            auto scheme = TransactionTemplate(
                tx.GetWorkingDir() + "/" + baseTableDescription.GetName() + "/" + indexDescription.GetName(),
                NKikimrSchemeOp::EOperationType::ESchemeOpCreateTable);
            scheme.SetFailOnExist(tx.GetFailOnExist());
            scheme.SetAllowCreateInTempDir(tx.GetAllowCreateInTempDir());

            *scheme.MutableCreateTable() = implTableDesc;

            return CreateNewTable(NextPartId(nextId, result), scheme);    
        };

        const auto& implTableColumns = indexes.at(indexDescription.GetName());
        if (indexDescription.GetType() == NKikimrSchemeOp::EIndexType::EIndexTypeGlobalVectorKmeansTree) {
            NKikimrSchemeOp::TTableDescription userLevelDesc, userPostingDesc;
            if (indexDescription.IndexImplTableDescriptionsSize() == 2) {
                // This description provided by user to override partition policy
                userLevelDesc = indexDescription.GetIndexImplTableDescriptions(0);
                userPostingDesc = indexDescription.GetIndexImplTableDescriptions(1);
            }
                
            result.push_back(createIndexImplTable(CalcVectorKmeansTreeLevelImplTableDesc(baseTableDescription.GetPartitionConfig(), userLevelDesc)));
            result.push_back(createIndexImplTable(CalcVectorKmeansTreePostingImplTableDesc(baseTableDescription, baseTableDescription.GetPartitionConfig(), implTableColumns, userPostingDesc)));
        } else {
            NKikimrSchemeOp::TTableDescription userIndexDesc;
            if (indexDescription.IndexImplTableDescriptionsSize()) {
                // This description provided by user to override partition policy
                userIndexDesc = indexDescription.GetIndexImplTableDescriptions(0);
            }

            result.push_back(createIndexImplTable(CalcImplTableDesc(baseTableDescription, implTableColumns, userIndexDesc)));
        }
    }

    for (auto& sequenceDescription : indexedTable.GetSequenceDescription()) {
        auto scheme = TransactionTemplate(
            tx.GetWorkingDir() + "/" + baseTableDescription.GetName(),
            NKikimrSchemeOp::EOperationType::ESchemeOpCreateSequence);
        scheme.SetFailOnExist(tx.GetFailOnExist());
        scheme.SetAllowCreateInTempDir(tx.GetAllowCreateInTempDir());

        *scheme.MutableSequence() = sequenceDescription;

        result.push_back(CreateNewSequence(NextPartId(nextId, result), scheme));
    }

    return result;
}

}
