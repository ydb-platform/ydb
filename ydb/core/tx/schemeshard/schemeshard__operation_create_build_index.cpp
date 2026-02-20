#include "schemeshard__operation_common.h"
#include "schemeshard__operation_part.h"
#include "schemeshard_index_utils.h"

#include <ydb/core/base/table_index.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/ydb_convert/table_description.h>

namespace NKikimr::NSchemeShard {

using namespace NTableIndex;

ISubOperation::TPtr CreateBuildColumn(TOperationId opId, const TTxTransaction& tx, TOperationContext& context) {
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpCreateColumnBuild);

    if (!context.SS->EnableAddColumsWithDefaults) {
        return CreateReject(opId, NKikimrScheme::EStatus::StatusPreconditionFailed, "Adding columns with defaults is disabled");
    }

    const auto& op = tx.GetInitiateColumnBuild();

    const auto tablePath = TPath::Resolve(op.GetTable(), context.SS);
    {
        const auto checks = tablePath.Check();
        checks
            .IsAtLocalSchemeShard()
            .NotEmpty()
            .IsResolved()
            .NotDeleted()
            .IsTable()
            .NotUnderDeleting()
            .NotUnderOperation()
            .NotAsyncReplicaTable()
            .IsCommonSensePath();

        if (!checks) {
            return CreateReject(opId, checks.GetStatus(), checks.GetError());
        }
    }

    // altering version of the table.
    {
        auto outTx = TransactionTemplate(tablePath.Parent().PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpInitiateBuildIndexMainTable);
        *outTx.MutableLockGuard() = tx.GetLockGuard();
        outTx.SetInternal(tx.GetInternal());

        auto& snapshot = *outTx.MutableInitiateBuildIndexMainTable();
        snapshot.SetTableName(tablePath.LeafName());

        return CreateInitializeBuildIndexMainTable(opId, outTx);
    }
}

TVector<ISubOperation::TPtr> CreateBuildIndex(TOperationId opId, const TTxTransaction& tx, TOperationContext& context) {
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpCreateIndexBuild);

    const auto& op = tx.GetInitiateIndexBuild();
    const auto& indexDesc = op.GetIndex();

    switch (GetIndexType(indexDesc)) {
        case NKikimrSchemeOp::EIndexTypeGlobal:
        case NKikimrSchemeOp::EIndexTypeGlobalAsync:
            // no feature flag, everything is fine
            break;
        case NKikimrSchemeOp::EIndexTypeGlobalUnique:
            if (!context.SS->EnableInitialUniqueIndex) {
                return {CreateReject(opId, NKikimrScheme::EStatus::StatusPreconditionFailed, "Adding a unique index to an existing table is disabled")};
            }
            break;
        case NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree: {
            break;
        }
        case NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain:
        case NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance: {
            if (!context.SS->EnableFulltextIndex) {
                return {CreateReject(opId, NKikimrScheme::EStatus::StatusPreconditionFailed, "Fulltext index support is disabled")};
            }
            break;
        }
        default:
            return {CreateReject(opId, NKikimrScheme::EStatus::StatusPreconditionFailed, InvalidIndexType(indexDesc.GetType()))};
    }

    auto counts = GetIndexObjectCounts(indexDesc);

    const auto table = TPath::Resolve(op.GetTable(), context.SS);
    auto tableInfo = context.SS->Tables.at(table.Base()->PathId);
    auto domainInfo = table.DomainInfo();

    if (counts.SequenceCount > 0 && domainInfo->GetSequenceShards().empty()) {
        ++counts.IndexTableShards;
    }

    const auto index = table.Child(indexDesc.GetName());
    {
        const auto checks = index.Check();
        checks
            .IsAtLocalSchemeShard();

        if (index.IsResolved()) {
            checks
                .IsResolved()
                .NotUnderDeleting()
                .FailOnExist(TPathElement::EPathType::EPathTypeTableIndex, false);
        } else {
            checks
                .NotEmpty()
                .NotResolved();
        }

        checks
            .IsValidLeafName(context.UserToken.Get())
            .PathsLimit(1 + counts.IndexTableCount + counts.SequenceCount)
            .DirChildrenLimit();

        if (!tx.GetInternal()) {
            checks
                .ShardsLimit(counts.IndexTableShards)
                .PathShardsLimit(counts.ShardsPerPath);
        }

        if (!checks) {
            return {CreateReject(opId, checks.GetStatus(), checks.GetError())};
        }
    }

    const ui64 aliveIndices = context.SS->GetAliveChildren(table.Base(), NKikimrSchemeOp::EPathTypeTableIndex);
    if (aliveIndices + 1 > domainInfo->GetSchemeLimits().MaxTableIndices) {
        return {CreateReject(opId, NKikimrScheme::EStatus::StatusPreconditionFailed, TStringBuilder()
            << "indexes count has reached maximum value in the table"
            << ", children limit for dir in domain: " << domainInfo->GetSchemeLimits().MaxTableIndices
            << ", intention to create new children: " << aliveIndices + 1)};
    }

    NTableIndex::TTableColumns implTableColumns;
    NKikimrScheme::EStatus status;
    TString errStr;
    if (!NTableIndex::CommonCheck(tableInfo, indexDesc, domainInfo->GetSchemeLimits(), false, implTableColumns, status, errStr)) {
        return {CreateReject(opId, status, errStr)};
    }

    TVector<ISubOperation::TPtr> result;

    {
        auto outTx = TransactionTemplate(table.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpCreateTableIndex);
        *outTx.MutableLockGuard() = tx.GetLockGuard();
        outTx.MutableCreateTableIndex()->CopyFrom(indexDesc);
        outTx.MutableCreateTableIndex()->SetType(GetIndexType(indexDesc));
        outTx.MutableCreateTableIndex()->SetState(NKikimrSchemeOp::EIndexStateWriteOnly);
        outTx.SetInternal(tx.GetInternal());

        result.push_back(CreateNewTableIndex(NextPartId(opId, result), outTx));
    }

    {
        auto outTx = TransactionTemplate(table.Parent().PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpInitiateBuildIndexMainTable);
        *outTx.MutableLockGuard() = tx.GetLockGuard();
        outTx.SetInternal(tx.GetInternal());

        auto& snapshot = *outTx.MutableInitiateBuildIndexMainTable();
        snapshot.SetTableName(table.LeafName());

        result.push_back(CreateInitializeBuildIndexMainTable(NextPartId(opId, result), outTx));
    }

    auto createImplTable = [&](NKikimrSchemeOp::TTableDescription&& implTableDesc, const THashSet<TString>& localSequences = {}) {
        if (GetIndexType(indexDesc) != NKikimrSchemeOp::EIndexTypeGlobalUnique) {
            implTableDesc.MutablePartitionConfig()->SetShadowData(true);
        }

        auto outTx = TransactionTemplate(index.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpInitiateBuildIndexImplTable);
        *outTx.MutableCreateTable() = std::move(implTableDesc);
        outTx.SetInternal(tx.GetInternal());

        return CreateInitializeBuildIndexImplTable(NextPartId(opId, result), outTx, localSequences);
    };

    switch (GetIndexType(indexDesc)) {
        case NKikimrSchemeOp::EIndexTypeGlobal:
        case NKikimrSchemeOp::EIndexTypeGlobalAsync:
        case NKikimrSchemeOp::EIndexTypeGlobalUnique: {
            NKikimrSchemeOp::TTableDescription indexTableDesc;
            // TODO After IndexImplTableDescriptions are persisted, this should be replaced with Y_ABORT_UNLESS
            if (indexDesc.IndexImplTableDescriptionsSize() == 1) {
                indexTableDesc = indexDesc.GetIndexImplTableDescriptions(0);
            }
            auto implTableDesc = CalcImplTableDesc(tableInfo, implTableColumns, indexTableDesc);
            // TODO if keep erase markers also speedup compaction or something else we can enable it for other impl tables too
            implTableDesc.MutablePartitionConfig()->MutableCompactionPolicy()->SetKeepEraseMarkers(true);
            result.push_back(createImplTable(std::move(implTableDesc)));
            break;
        }
        case NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree: {
            const bool prefixVectorIndex = indexDesc.GetKeyColumnNames().size() > 1;
            NKikimrSchemeOp::TTableDescription indexLevelTableDesc, indexPostingTableDesc, indexPrefixTableDesc;
            // TODO After IndexImplTableDescriptions are persisted, this should be replaced with Y_ABORT_UNLESS
            if (indexDesc.IndexImplTableDescriptionsSize() == 2 + prefixVectorIndex) {
                indexLevelTableDesc = indexDesc.GetIndexImplTableDescriptions(NTableIndex::NKMeans::LevelTablePosition);
                indexPostingTableDesc = indexDesc.GetIndexImplTableDescriptions(NTableIndex::NKMeans::PostingTablePosition);
                if (prefixVectorIndex) {
                    indexPrefixTableDesc = indexDesc.GetIndexImplTableDescriptions(NTableIndex::NKMeans::PrefixTablePosition);
                }
            }
            const THashSet<TString> indexDataColumns{indexDesc.GetDataColumnNames().begin(), indexDesc.GetDataColumnNames().end()};
            result.push_back(createImplTable(CalcVectorKmeansTreeLevelImplTableDesc(tableInfo->PartitionConfig(), indexLevelTableDesc)));
            result.push_back(createImplTable(CalcVectorKmeansTreePostingImplTableDesc(tableInfo, tableInfo->PartitionConfig(), indexDataColumns, indexPostingTableDesc)));
            if (prefixVectorIndex) {
                const THashSet<TString> prefixColumns{indexDesc.GetKeyColumnNames().begin(), indexDesc.GetKeyColumnNames().end() - 1};
                result.push_back(createImplTable(CalcVectorKmeansTreePrefixImplTableDesc(
                    prefixColumns, tableInfo, tableInfo->PartitionConfig(), implTableColumns, indexPrefixTableDesc),
                    THashSet<TString>{NTableIndex::NKMeans::IdColumnSequence}));
                auto outTx = TransactionTemplate(index.PathString() + "/" + NTableIndex::NKMeans::PrefixTable, NKikimrSchemeOp::EOperationType::ESchemeOpCreateSequence);
                outTx.MutableSequence()->SetName(NTableIndex::NKMeans::IdColumnSequence);
                outTx.SetInternal(tx.GetInternal());
                result.push_back(CreateNewSequence(NextPartId(opId, result), outTx));
            }
            break;
        }
        case NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain: {
            NKikimrSchemeOp::TTableDescription indexTableDesc;
            if (indexDesc.IndexImplTableDescriptionsSize() == 1) {
                indexTableDesc = indexDesc.GetIndexImplTableDescriptions(0);
            }
            const THashSet<TString> indexDataColumns{indexDesc.GetDataColumnNames().begin(), indexDesc.GetDataColumnNames().end()};
            auto implTableDesc = CalcFulltextImplTableDesc(tableInfo, tableInfo->PartitionConfig(), indexDataColumns, indexTableDesc, indexDesc.GetFulltextIndexDescription(), /*withRelevance=*/false);
            implTableDesc.MutablePartitionConfig()->MutableCompactionPolicy()->SetKeepEraseMarkers(true);
            result.push_back(createImplTable(std::move(implTableDesc)));
            break;
        }
        case NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance: {
            NKikimrSchemeOp::TTableDescription indexTableDesc, docsTableDesc, dictTableDesc, statsTableDesc;
            if (indexDesc.IndexImplTableDescriptionsSize() == 4) {
                dictTableDesc = indexDesc.GetIndexImplTableDescriptions(NTableIndex::NFulltext::DictTablePosition);
                docsTableDesc = indexDesc.GetIndexImplTableDescriptions(NTableIndex::NFulltext::DocsTablePosition);
                statsTableDesc = indexDesc.GetIndexImplTableDescriptions(NTableIndex::NFulltext::StatsTablePosition);
                indexTableDesc = indexDesc.GetIndexImplTableDescriptions(NTableIndex::NFulltext::PostingTablePosition);
            }
            const THashSet<TString> indexDataColumns{indexDesc.GetDataColumnNames().begin(), indexDesc.GetDataColumnNames().end()};
            auto implTableDesc = CalcFulltextImplTableDesc(tableInfo, tableInfo->PartitionConfig(), indexDataColumns, indexTableDesc, indexDesc.GetFulltextIndexDescription(), /*withRelevance=*/true);
            implTableDesc.MutablePartitionConfig()->MutableCompactionPolicy()->SetKeepEraseMarkers(true);
            result.push_back(createImplTable(std::move(implTableDesc)));
            result.push_back(createImplTable(CalcFulltextDocsImplTableDesc(tableInfo, tableInfo->PartitionConfig(), indexDataColumns, docsTableDesc)));
            result.push_back(createImplTable(CalcFulltextDictImplTableDesc(tableInfo, tableInfo->PartitionConfig(), dictTableDesc, indexDesc.GetFulltextIndexDescription())));
            result.push_back(createImplTable(CalcFulltextStatsImplTableDesc(tableInfo, tableInfo->PartitionConfig(), statsTableDesc)));
            break;
        }
        default:
            Y_DEBUG_ABORT_S(NTableIndex::InvalidIndexType(indexDesc.GetType()));
            break;
    }

    return result;
}

}
