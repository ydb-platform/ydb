#include <ydb/core/tx/schemeshard/schemeshard__op_traits.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_common.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_part.h>
#include <ydb/core/tx/schemeshard/index/index_utils.h>

#include <ydb/core/base/kmeans_clusters.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

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

// Detects fulltext/JSON indexes in an indexed-table create that need a synthetic __ydb_row_id doc_id
// and rewrites the request in place: appends the __ydb_row_id Uint64 NOT NULL column (defaulted from a
// sequence) and its backing sequence, appends the __ydb_unique_row_id GlobalUnique index over it, and
// sets UseRowIdAsDocId on the affected indexes. Returns a reject sub-operation on a malformed request
// or a disabled feature, otherwise nullptr (including when there is nothing to provision). Mirrors the
// build-index auto-provisioning in build_index__create.cpp / ClassifyFulltextRowId.
ISubOperation::TPtr MaybeProvisionFulltextRowId(
    TOperationId nextId,
    NKikimrSchemeOp::TIndexedTableCreationConfig& indexedTable,
    TOperationContext& context)
{
    bool needRowIdColumn = false;
    bool needUniqueIndex = false;
    TVector<int> rowIdModeIndexes;

    for (int i = 0; i < static_cast<int>(indexedTable.IndexDescriptionSize()); ++i) {
        TString error;
        const auto classification = ClassifyFulltextRowIdForCreate(
            indexedTable, indexedTable.GetIndexDescription(i), error);
        switch (classification.Plan) {
            case EFulltextRowIdPlan::NotApplicable:
            case EFulltextRowIdPlan::LegacyIntegerPk:
                break;
            case EFulltextRowIdPlan::Error:
                return CreateReject(nextId, NKikimrScheme::EStatus::StatusInvalidParameter, error);
            case EFulltextRowIdPlan::Reuse:
                rowIdModeIndexes.push_back(i);
                break;
            case EFulltextRowIdPlan::Provision:
                if (!context.SS->EnableInitialUniqueIndex) {
                    return CreateReject(nextId, NKikimrScheme::EStatus::StatusPreconditionFailed,
                        TStringBuilder() << "Auto-provisioning '" << NTableIndex::NFulltext::RowIdColumn
                            << "' for a fulltext/JSON index on a non-integer-PK table requires the unique-index feature");
                }
                needRowIdColumn |= classification.NeedColumn;
                needUniqueIndex |= classification.NeedUniqueIndex;
                rowIdModeIndexes.push_back(i);
                break;
        }
    }

    if (needRowIdColumn) {
        // Reached only when no __ydb_row_id column was supplied (else the plan is Reuse or a
        // unique-index-only Provision), so this never duplicates a user column.
        auto* column = indexedTable.MutableTableDescription()->AddColumns();
        column->SetName(NTableIndex::NFulltext::RowIdColumn);
        column->SetType("Uint64");
        column->SetNotNull(true);
        // Table-local leaf name, matching the create-table sequence convention.
        column->SetDefaultFromSequence(NTableIndex::NFulltext::RowIdSequenceName);

        auto* sequence = indexedTable.AddSequenceDescription();
        sequence->SetName(NTableIndex::NFulltext::RowIdSequenceName);
    }

    if (needUniqueIndex) {
        auto* uniqueIndex = indexedTable.AddIndexDescription();
        uniqueIndex->SetName(NTableIndex::NFulltext::RowIdUniqueIndexName);
        uniqueIndex->SetType(NKikimrSchemeOp::EIndexTypeGlobalUnique);
        uniqueIndex->AddKeyColumnNames(NTableIndex::NFulltext::RowIdColumn);
    }

    for (const int i : rowIdModeIndexes) {
        indexedTable.MutableIndexDescription(i)->MutableFulltextIndexDescription()->SetUseRowIdAsDocId(true);
    }

    return nullptr;
}

TVector<ISubOperation::TPtr> CreateIndexedTable(TOperationId nextId, const TTxTransaction& tx, TOperationContext& context) {
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpCreateIndexedTable);

    auto indexedTable = tx.GetCreateIndexedTable();

    // __ydb_row_id auto-provisioning. A fulltext/JSON index over a table whose PK is not a single
    // integer needs a synthetic Uint64 doc_id; mirror the build-index path (ClassifyFulltextRowId) by
    // injecting the __ydb_row_id column (+ its backing sequence) and a unique secondary index over it,
    // and switching the affected indexes to rowid mode. Done before the object counts below so the
    // injected paths/shards are charged against the scheme limits like any other create.
    if (auto reject = MaybeProvisionFulltextRowId(nextId, indexedTable, context)) {
        return {reject};
    }

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

    YDB_LOG_DEBUG_CTX(context.Ctx, "TCreateTableIndex construct operation table domain path domain",
        {"tablePath", baseTablePath.PathString()},
        {"id", baseTablePath.GetPathIdForDomain()},
        {"domainPath", TPath::Init(baseTablePath.GetPathIdForDomain(), context.SS).PathString()},
        {"shardsToCreate", shardsToCreate},
        {"shardsPerPath", totalCounts.ShardsPerPath},
        {"getShardsInside", domainInfo->GetShardsInside()},
        {"maxShards", domainInfo->GetSchemeLimits().MaxShards});

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
        const auto indexType = GetIndexType(indexDescription);

        if (TTableIndexInfo::IsLocalIndex(indexType)) {
            // Row-table local indexes (prefix bloom filters) have no impl table. Full column
            // validation is done in KQP; here we enforce a valid, unique name and a non-empty
            // primary-key prefix - drop logic later treats IndexKeys.size() as the prefix length.
            if (indexType != NKikimrSchemeOp::EIndexTypeLocalBloomFilter || indexDescription.KeyColumnNamesSize() == 0) {
                return {CreateReject(nextId, NKikimrScheme::EStatus::StatusInvalidParameter,
                    TStringBuilder() << "Local index '" << indexName
                        << "' must be a local bloom filter over a non-empty primary-key prefix")};
            }
            TPath indexPath = baseTablePath.Child(indexName);
            TString msg = "invalid table index name: ";
            if (!indexPath.IsValidLeafName(context.UserToken.Get(), msg)) {
                return {CreateReject(nextId, NKikimrScheme::EStatus::StatusSchemeError, msg)};
            }
            if (indexes.contains(indexName)) {
                return {CreateReject(nextId, NKikimrScheme::EStatus::StatusInvalidParameter,
                    TStringBuilder() << "Can't create indexes with not unique names for table, for example: " << indexName)};
            }
            indexes.emplace(indexName, TTableColumns{});
            continue;
        }
        switch (indexType) {
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
                if (!NKikimr::NKMeans::ValidateSettingsPartial(indexDescription.GetVectorIndexKmeansTreeDescription().GetSettings(), msg)) {
                    return {CreateReject(nextId, NKikimrScheme::EStatus::StatusInvalidParameter, msg)};
                }
                if (NKikimr::NKMeans::NeedsVectorSettingsAutoSelect(indexDescription.GetVectorIndexKmeansTreeDescription().GetSettings().settings())) {
                    return {CreateReject(nextId, NKikimrScheme::EStatus::StatusPreconditionFailed,
                        "Cannot build vector index: table is empty and vector_type/vector_dimension were not specified")};
                }
                break;
            }
            case NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain:
            case NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance:
            case NKikimrSchemeOp::EIndexTypeGlobalFulltextCompact:
            case NKikimrSchemeOp::EIndexTypeGlobalFulltextCompactRelevance: {
                if (!context.SS->EnableFulltextIndex) {
                    return {CreateReject(nextId, NKikimrScheme::EStatus::StatusPreconditionFailed, "Fulltext index support is disabled")};
                }
                TString msg;
                if (!NKikimr::NFulltext::ValidateSettings(indexDescription.GetFulltextIndexDescription().GetSettings(), msg)) {
                    return {CreateReject(nextId, NKikimrScheme::EStatus::StatusInvalidParameter, msg)};
                }
                break;
            }
            case NKikimrSchemeOp::EIndexTypeGlobalJson:
            case NKikimrSchemeOp::EIndexTypeGlobalJsonCompact: {
                if (!context.SS->EnableJsonIndex) {
                    return {CreateReject(nextId, NKikimrScheme::EStatus::StatusPreconditionFailed, "JSON index support is disabled")};
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

        if (baseTableDescription.HasTTLSettings() && !DoesIndexSupportTTL(indexType)) {
            auto msg = TStringBuilder() << "Table with " << indexType << " index doesn't support TTL";
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

        if (TTableIndexInfo::IsLocalIndex(GetIndexType(indexDescription))) {
            // Local index (e.g. row-table prefix bloom filter) has no impl table
            continue;
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
        const auto indexType = GetIndexType(indexDescription);
        bool compact = false;
        switch (indexType) {
            case NKikimrSchemeOp::EIndexTypeGlobal:
            case NKikimrSchemeOp::EIndexTypeGlobalAsync:
            case NKikimrSchemeOp::EIndexTypeGlobalUnique: {
                NKikimrSchemeOp::TTableDescription userIndexDesc;
                if (indexDescription.IndexImplTableDescriptionsSize()) {
                    // This description provided by user to override partition policy
                    userIndexDesc = indexDescription.GetIndexImplTableDescriptions(0);
                }
                const auto uniqueKeySize = indexType == NKikimrSchemeOp::EIndexTypeGlobalUnique
                    ? indexDescription.GetKeyColumnNames().size() : 0;
                result.push_back(createIndexImplTable(CalcImplTableDesc(baseTableDescription, implTableColumns, userIndexDesc, uniqueKeySize)));
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
            case NKikimrSchemeOp::EIndexTypeGlobalJsonCompact:
            case NKikimrSchemeOp::EIndexTypeGlobalFulltextCompact:
                compact = true;
                [[fallthrough]];
            case NKikimrSchemeOp::EIndexTypeGlobalJson:
            case NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain: {
                NKikimrSchemeOp::TTableDescription userIndexDesc;
                if (indexDescription.IndexImplTableDescriptionsSize() == 1) {
                    userIndexDesc = indexDescription.GetIndexImplTableDescriptions(0);
                }
                const THashSet<TString> indexDataColumns{indexDescription.GetDataColumnNames().begin(), indexDescription.GetDataColumnNames().end()};
                result.push_back(createIndexImplTable(compact
                    ? CalcFulltextCompactImplTableDesc(baseTableDescription, baseTableDescription.GetPartitionConfig(),
                        userIndexDesc, &indexDescription.GetFulltextIndexDescription(), indexType,
                        NTableIndex::GetFulltextPrefixColumns(indexDescription.GetKeyColumnNames()))
                    : CalcFulltextImplTableDesc(baseTableDescription, baseTableDescription.GetPartitionConfig(),
                        indexDataColumns, userIndexDesc, indexDescription.GetFulltextIndexDescription(), indexType,
                        NTableIndex::GetFulltextPrefixColumns(indexDescription.GetKeyColumnNames()))));
                break;
            }
            case NKikimrSchemeOp::EIndexTypeGlobalFulltextCompactRelevance:
                compact = true;
                [[fallthrough]];
            case NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance: {
                NKikimrSchemeOp::TTableDescription userIndexDesc, docsTableDesc, dictTableDesc, statsTableDesc;
                if (indexDescription.IndexImplTableDescriptionsSize() == 4) {
                    dictTableDesc = indexDescription.GetIndexImplTableDescriptions(NTableIndex::NFulltext::DictTablePosition);
                    docsTableDesc = indexDescription.GetIndexImplTableDescriptions(NTableIndex::NFulltext::DocsTablePosition);
                    statsTableDesc = indexDescription.GetIndexImplTableDescriptions(NTableIndex::NFulltext::StatsTablePosition);
                    userIndexDesc = indexDescription.GetIndexImplTableDescriptions(NTableIndex::NFulltext::PostingTablePosition);
                }
                const THashSet<TString> indexDataColumns{indexDescription.GetDataColumnNames().begin(), indexDescription.GetDataColumnNames().end()};
                result.push_back(createIndexImplTable(compact
                    ? CalcFulltextCompactImplTableDesc(baseTableDescription, baseTableDescription.GetPartitionConfig(),
                        userIndexDesc, &indexDescription.GetFulltextIndexDescription(), indexType,
                        NTableIndex::GetFulltextPrefixColumns(indexDescription.GetKeyColumnNames()))
                    : CalcFulltextImplTableDesc(baseTableDescription, baseTableDescription.GetPartitionConfig(),
                        indexDataColumns, userIndexDesc, indexDescription.GetFulltextIndexDescription(), indexType,
                        NTableIndex::GetFulltextPrefixColumns(indexDescription.GetKeyColumnNames()))));
                result.push_back(createIndexImplTable(CalcFulltextDocsImplTableDesc(baseTableDescription, baseTableDescription.GetPartitionConfig(), indexDataColumns, docsTableDesc, indexDescription.GetFulltextIndexDescription())));
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
