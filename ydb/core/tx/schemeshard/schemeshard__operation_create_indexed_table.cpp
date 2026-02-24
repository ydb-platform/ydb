#include "schemeshard__op_traits.h"
#include "schemeshard__operation_common.h"
#include "schemeshard__operation_part.h"
#include "schemeshard_index_utils.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>

namespace NKikimr::NSchemeShard {

using namespace NTableIndex;

using TTag = TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreateIndexedTable>;

namespace NOperation {

template <>
std::optional<TString> GetTargetName<TTag>(
    TTag,
    const TTxTransaction& tx)
{
    return tx.GetCreateIndexedTable().GetTableDescription().GetName();
}

template <>
bool SetName<TTag>(
    TTag,
    TTxTransaction& tx,
    const TString& name)
{
    tx.MutableCreateIndexedTable()->MutableTableDescription()->SetName(name);
    return true;
}

} // namespace NOperation

TVector<ISubOperation::TPtr> CreateIndexedTable(TOperationId nextId, const TTxTransaction& tx, TOperationContext& context) {
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpCreateIndexedTable);

    auto indexedTable = tx.GetCreateIndexedTable();
    const NKikimrSchemeOp::TTableDescription& baseTableDescription = indexedTable.GetTableDescription();

    TIndexObjectCounts totalCounts;
    ui32 indexCount = indexedTable.IndexDescriptionSize();
    for (const auto& indexDesc : indexedTable.GetIndexDescription()) {
        auto counts = GetIndexObjectCounts(indexDesc);
        totalCounts.IndexTableCount += counts.IndexTableCount;
        totalCounts.SequenceCount += counts.SequenceCount;
        totalCounts.IndexTableShards += counts.IndexTableShards;
        if (totalCounts.ShardsPerPath < counts.ShardsPerPath) {
            totalCounts.ShardsPerPath = counts.ShardsPerPath;
        }
    }

    ui32 baseShards = TTableInfo::ShardsToCreate(baseTableDescription);
    if (totalCounts.ShardsPerPath < baseShards) {
        totalCounts.ShardsPerPath = baseShards;
    }
    ui32 shardsToCreate = baseShards + totalCounts.IndexTableShards;
    ui32 pathToCreate = 1 + indexCount + totalCounts.IndexTableCount + totalCounts.SequenceCount;

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
        if(!baseTablePath.IsValidLeafName(context.UserToken.Get(), msg)) {
            return {CreateReject(nextId, NKikimrScheme::EStatus::StatusSchemeError, msg)};
        }
    }

    if (baseTableDescription.GetIsBackup()) {
        return {CreateReject(nextId, NKikimrScheme::StatusInvalidParameter, "Cannot create table with explicit 'IsBackup' property")};
    }

    TSubDomainInfo::TPtr domainInfo = baseTablePath.DomainInfo();

    if (totalCounts.SequenceCount > 0 && domainInfo->GetSequenceShards().empty()) {
        ++shardsToCreate;
    }

    LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TCreateTableIndex construct operation "
                    << " table path: " << baseTablePath.PathString()
                    << " domain path id: " << baseTablePath.GetPathIdForDomain()
                    << " domain path: " << TPath::Init(baseTablePath.GetPathIdForDomain(), context.SS).PathString()
                    << " shardsToCreate: " << shardsToCreate
                    << " shardsPerPath: " << totalCounts.ShardsPerPath
                    << " GetShardsInside: " << domainInfo->GetShardsInside()
                    << " MaxShards: " << domainInfo->GetSchemeLimits().MaxShards);

    if (indexCount > domainInfo->GetSchemeLimits().MaxTableIndices) {
        auto msg = TStringBuilder() << "indexes count has reached maximum value in the table"
                                    << ", children limit for dir in domain: " << domainInfo->GetSchemeLimits().MaxTableIndices
                                    << ", intention to create new children: " << indexCount;
        return {CreateReject(nextId, NKikimrScheme::EStatus::StatusResourceExhausted, msg)};
    }

    {
        auto checks = baseTablePath.Check();
        checks
            .PathsLimit(pathToCreate);

        if (!tx.GetInternal()) {
            checks
                .PathShardsLimit(totalCounts.ShardsPerPath)
                .ShardsLimit(shardsToCreate);
        }

        if (!checks) {
            return {CreateReject(nextId, NKikimrScheme::EStatus::StatusResourceExhausted, checks.GetError())};
        }
    }

    THashMap<TString, TTableColumns> indexes;

    TTableColumns baseTableColumns = ExtractInfo(baseTableDescription);
    for (auto& indexDescription: indexedTable.GetIndexDescription()) {
        const auto& indexName = indexDescription.GetName();

        switch (GetIndexType(indexDescription)) {
            case NKikimrSchemeOp::EIndexTypeGlobal:
            case NKikimrSchemeOp::EIndexTypeGlobalAsync:
                // no feature flag, everything is fine
                break;
            case NKikimrSchemeOp::EIndexTypeGlobalUnique:
                if (!context.SS->EnableInitialUniqueIndex) {
                    return {CreateReject(nextId, NKikimrScheme::EStatus::StatusPreconditionFailed, "Unique constraint feature is disabled")};
                }
                break;
            case NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree: {
                TString msg;
                if (!NKikimr::NKMeans::ValidateSettings(indexDescription.GetVectorIndexKmeansTreeDescription().GetSettings(), msg)) {
                    return {CreateReject(nextId, NKikimrScheme::EStatus::StatusInvalidParameter, msg)};
                }
                break;
            }
            case NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain:
            case NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance: {
                if (!context.SS->EnableFulltextIndex) {
                    return {CreateReject(nextId, NKikimrScheme::EStatus::StatusPreconditionFailed, "Fulltext index support is disabled")};
                }
                TString msg;
                if (!NKikimr::NFulltext::ValidateSettings(indexDescription.GetFulltextIndexDescription().GetSettings(), msg)) {
                    return {CreateReject(nextId, NKikimrScheme::EStatus::StatusInvalidParameter, msg)};
                }
                break;
            }
            default:
                return {CreateReject(nextId, NKikimrScheme::EStatus::StatusPreconditionFailed, InvalidIndexType(indexDescription.GetType()))};
        }

        bool uniformIndexTable = false;
        if (indexDescription.IndexImplTableDescriptionsSize()) {
            if (indexDescription.GetIndexImplTableDescriptions(0).HasUniformPartitionsCount()) {
                uniformIndexTable = true;
            }
        }

        TPath indexPath = baseTablePath.Child(indexName);
        {
            TString msg = "invalid table index name: ";
            if (!indexPath.IsValidLeafName(context.UserToken.Get(), msg)) {
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
            if (!sequencePath.IsValidLeafName(context.UserToken.Get(), msg)) {
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
        }
    }

    TVector<ISubOperation::TPtr> result;

    {
        auto scheme = TransactionTemplate(tx.GetWorkingDir(), NKikimrSchemeOp::EOperationType::ESchemeOpCreateTable);
        scheme.SetFailOnExist(tx.GetFailOnExist());
        scheme.SetAllowCreateInTempDir(tx.GetAllowCreateInTempDir());
        scheme.SetInternal(tx.GetInternal());

        scheme.MutableCreateTable()->CopyFrom(baseTableDescription);
        if (tx.HasAlterUserAttributes()) {
            scheme.MutableAlterUserAttributes()->CopyFrom(tx.GetAlterUserAttributes());
        }
        if (tx.HasModifyACL()) {
            scheme.MutableModifyACL()->CopyFrom(tx.GetModifyACL());
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
            scheme.MutableCreateTableIndex()->SetType(GetIndexType(indexDescription));

            result.push_back(CreateNewTableIndex(NextPartId(nextId, result), scheme));
        }

        auto createIndexImplTable = [&] (NKikimrSchemeOp::TTableDescription&& implTableDesc, const THashSet<TString>& localSequences = {}) {
            auto scheme = TransactionTemplate(
                tx.GetWorkingDir() + "/" + baseTableDescription.GetName() + "/" + indexDescription.GetName(),
                NKikimrSchemeOp::EOperationType::ESchemeOpCreateTable);
            scheme.SetFailOnExist(tx.GetFailOnExist());
            scheme.SetAllowCreateInTempDir(tx.GetAllowCreateInTempDir());
            scheme.SetInternal(tx.GetInternal());

            *scheme.MutableCreateTable() = std::move(implTableDesc);

            return CreateNewTable(NextPartId(nextId, result), scheme, localSequences);
        };

        const auto& implTableColumns = indexes.at(indexDescription.GetName());
        switch (GetIndexType(indexDescription)) {
            case NKikimrSchemeOp::EIndexTypeGlobal:
            case NKikimrSchemeOp::EIndexTypeGlobalAsync:
            case NKikimrSchemeOp::EIndexTypeGlobalUnique: {
                NKikimrSchemeOp::TTableDescription userIndexDesc;
                if (indexDescription.IndexImplTableDescriptionsSize()) {
                    // This description provided by user to override partition policy
                    userIndexDesc = indexDescription.GetIndexImplTableDescriptions(0);
                }
                result.push_back(createIndexImplTable(CalcImplTableDesc(baseTableDescription, implTableColumns, userIndexDesc)));
                break;
            }
            case NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree: {
                const bool prefixVectorIndex = indexDescription.GetKeyColumnNames().size() > 1;
                NKikimrSchemeOp::TTableDescription userLevelDesc, userPostingDesc, userPrefixDesc;
                if (indexDescription.IndexImplTableDescriptionsSize() == 2 + prefixVectorIndex) {
                    // This description provided by user to override partition policy
                    userLevelDesc = indexDescription.GetIndexImplTableDescriptions(NTableIndex::NKMeans::LevelTablePosition);
                    userPostingDesc = indexDescription.GetIndexImplTableDescriptions(NTableIndex::NKMeans::PostingTablePosition);
                    if (prefixVectorIndex) {
                        userPrefixDesc = indexDescription.GetIndexImplTableDescriptions(NTableIndex::NKMeans::PrefixTablePosition);
                    }
                }
                const THashSet<TString> indexDataColumns{indexDescription.GetDataColumnNames().begin(), indexDescription.GetDataColumnNames().end()};
                result.push_back(createIndexImplTable(CalcVectorKmeansTreeLevelImplTableDesc(baseTableDescription.GetPartitionConfig(), userLevelDesc)));
                result.push_back(createIndexImplTable(CalcVectorKmeansTreePostingImplTableDesc(baseTableDescription, baseTableDescription.GetPartitionConfig(), indexDataColumns, userPostingDesc)));
                if (prefixVectorIndex) {
                    const THashSet<TString> prefixColumns{indexDescription.GetKeyColumnNames().begin(), indexDescription.GetKeyColumnNames().end() - 1};
                    result.push_back(createIndexImplTable(CalcVectorKmeansTreePrefixImplTableDesc(
                        prefixColumns, baseTableDescription, baseTableDescription.GetPartitionConfig(), implTableColumns, userPrefixDesc),
                        THashSet<TString>{NTableIndex::NKMeans::IdColumnSequence}));
                    // Create the sequence
                    auto outTx = TransactionTemplate(tx.GetWorkingDir() + "/" + baseTableDescription.GetName() + "/" +
                        indexDescription.GetName() + "/" + NTableIndex::NKMeans::PrefixTable,
                        NKikimrSchemeOp::EOperationType::ESchemeOpCreateSequence);
                    outTx.MutableSequence()->SetName(NTableIndex::NKMeans::IdColumnSequence);
                    outTx.MutableSequence()->SetMinValue(-0x7FFFFFFFFFFFFFFF);
                    outTx.MutableSequence()->SetMaxValue(-1);
                    outTx.MutableSequence()->SetStartValue(NTableIndex::NKMeans::SetPostingParentFlag(1));
                    outTx.MutableSequence()->SetRestart(true);
                    outTx.SetFailOnExist(tx.GetFailOnExist());
                    outTx.SetAllowCreateInTempDir(tx.GetAllowCreateInTempDir());
                    outTx.SetInternal(tx.GetInternal());
                    result.push_back(CreateNewSequence(NextPartId(nextId, result), outTx));
                }
                break;
            }
            case NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain: {
                NKikimrSchemeOp::TTableDescription userIndexDesc;
                if (indexDescription.IndexImplTableDescriptionsSize() == 1) {
                    userIndexDesc = indexDescription.GetIndexImplTableDescriptions(0);
                }
                const THashSet<TString> indexDataColumns{indexDescription.GetDataColumnNames().begin(), indexDescription.GetDataColumnNames().end()};
                result.push_back(createIndexImplTable(CalcFulltextImplTableDesc(baseTableDescription, baseTableDescription.GetPartitionConfig(), indexDataColumns, userIndexDesc, indexDescription.GetFulltextIndexDescription(), /*withRelevance=*/false)));
                break;
            }
            case NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance: {
                NKikimrSchemeOp::TTableDescription userIndexDesc, docsTableDesc, dictTableDesc, statsTableDesc;
                if (indexDescription.IndexImplTableDescriptionsSize() == 4) {
                    dictTableDesc = indexDescription.GetIndexImplTableDescriptions(NTableIndex::NFulltext::DictTablePosition);
                    docsTableDesc = indexDescription.GetIndexImplTableDescriptions(NTableIndex::NFulltext::DocsTablePosition);
                    statsTableDesc = indexDescription.GetIndexImplTableDescriptions(NTableIndex::NFulltext::StatsTablePosition);
                    userIndexDesc = indexDescription.GetIndexImplTableDescriptions(NTableIndex::NFulltext::PostingTablePosition);
                }
                const THashSet<TString> indexDataColumns{indexDescription.GetDataColumnNames().begin(), indexDescription.GetDataColumnNames().end()};
                result.push_back(createIndexImplTable(CalcFulltextImplTableDesc(baseTableDescription, baseTableDescription.GetPartitionConfig(), indexDataColumns, userIndexDesc, indexDescription.GetFulltextIndexDescription(), /*withRelevance=*/true)));
                result.push_back(createIndexImplTable(CalcFulltextDocsImplTableDesc(baseTableDescription, baseTableDescription.GetPartitionConfig(), indexDataColumns, docsTableDesc)));
                result.push_back(createIndexImplTable(CalcFulltextDictImplTableDesc(baseTableDescription, baseTableDescription.GetPartitionConfig(), dictTableDesc, indexDescription.GetFulltextIndexDescription())));
                result.push_back(createIndexImplTable(CalcFulltextStatsImplTableDesc(baseTableDescription, baseTableDescription.GetPartitionConfig(), statsTableDesc)));
                break;
            }
            default:
                Y_DEBUG_ABORT_S(NTableIndex::InvalidIndexType(indexDescription.GetType()));
                break;
        }
    }

    for (auto& sequenceDescription : indexedTable.GetSequenceDescription()) {
        auto scheme = TransactionTemplate(
            tx.GetWorkingDir() + "/" + baseTableDescription.GetName(),
            NKikimrSchemeOp::EOperationType::ESchemeOpCreateSequence);
        scheme.SetFailOnExist(tx.GetFailOnExist());
        scheme.SetAllowCreateInTempDir(tx.GetAllowCreateInTempDir());
        scheme.SetInternal(tx.GetInternal());

        *scheme.MutableSequence() = sequenceDescription;

        result.push_back(CreateNewSequence(NextPartId(nextId, result), scheme));
    }

    return result;
}

}
