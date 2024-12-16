#include "schemeshard__operation_common.h"
#include "schemeshard__operation_part.h"
#include "schemeshard_utils.h"  // for TransactionTemplate

#include <ydb/core/base/table_vector_index.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/ydb_convert/table_description.h>

namespace NKikimr::NSchemeShard {

using namespace NTableIndex;

TVector<ISubOperation::TPtr> CreateBuildColumn(TOperationId opId, const TTxTransaction& tx, TOperationContext& context) {
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpCreateColumnBuild);

    const auto& op = tx.GetInitiateColumnBuild();

    const auto table = TPath::Resolve(op.GetTable(), context.SS);
    TVector<ISubOperation::TPtr> result;

    // altering version of the table.
    {
        auto outTx = TransactionTemplate(table.Parent().PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpInitiateBuildIndexMainTable);
        *outTx.MutableLockGuard() = tx.GetLockGuard();
        outTx.SetInternal(tx.GetInternal());

        auto& snapshot = *outTx.MutableInitiateBuildIndexMainTable();
        snapshot.SetTableName(table.LeafName());

        result.push_back(CreateInitializeBuildIndexMainTable(NextPartId(opId, result), outTx));
    }

    return result;
}

TVector<ISubOperation::TPtr> CreateBuildIndex(TOperationId opId, const TTxTransaction& tx, TOperationContext& context) {
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpCreateIndexBuild);

    const auto& op = tx.GetInitiateIndexBuild();
    const auto& indexDesc = op.GetIndex();

    const auto table = TPath::Resolve(op.GetTable(), context.SS);
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

        // TODO(mbkkt) less than necessary for vector index
        checks
            .IsValidLeafName(context.UserToken.Get())
            .PathsLimit(2) // index and impl-table
            .DirChildrenLimit();

        if (!tx.GetInternal()) {
            checks
                .ShardsLimit(1); // impl-table
        }

        if (!checks) {
            return {CreateReject(opId, checks.GetStatus(), checks.GetError())};
        }
    }

    auto tableInfo = context.SS->Tables.at(table.Base()->PathId);
    auto domainInfo = table.DomainInfo();

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
        outTx.MutableCreateTableIndex()->SetState(NKikimrSchemeOp::EIndexStateWriteOnly);
        outTx.SetInternal(tx.GetInternal());

        if (!indexDesc.HasType()) {
            outTx.MutableCreateTableIndex()->SetType(NKikimrSchemeOp::EIndexTypeGlobal);
        }

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

    auto createImplTable = [&](NKikimrSchemeOp::TTableDescription&& implTableDesc) {
        implTableDesc.MutablePartitionConfig()->SetShadowData(true);

        auto outTx = TransactionTemplate(index.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpInitiateBuildIndexImplTable);
        *outTx.MutableCreateTable() = std::move(implTableDesc);
        outTx.SetInternal(tx.GetInternal());

        return CreateInitializeBuildIndexImplTable(NextPartId(opId, result), outTx);
    };

    if (indexDesc.GetType() == NKikimrSchemeOp::EIndexType::EIndexTypeGlobalVectorKmeansTree) {
        const bool prefixVectorIndex = indexDesc.GetKeyColumnNames().size() > 1;
        NKikimrSchemeOp::TTableDescription indexLevelTableDesc, indexPostingTableDesc, indexPrefixTableDesc;
        // TODO After IndexImplTableDescriptions are persisted, this should be replaced with Y_ABORT_UNLESS
        if (indexDesc.IndexImplTableDescriptionsSize() == 2 + prefixVectorIndex) {
            indexLevelTableDesc = indexDesc.GetIndexImplTableDescriptions(0);
            indexPostingTableDesc = indexDesc.GetIndexImplTableDescriptions(1);
            if (prefixVectorIndex) {
                indexPrefixTableDesc = indexDesc.GetIndexImplTableDescriptions(2);
            }
        }
        const THashSet<TString> indexDataColumns{indexDesc.GetDataColumnNames().begin(), indexDesc.GetDataColumnNames().end()};
        result.push_back(createImplTable(CalcVectorKmeansTreeLevelImplTableDesc(tableInfo->PartitionConfig(), indexLevelTableDesc)));
        result.push_back(createImplTable(CalcVectorKmeansTreePostingImplTableDesc(tableInfo, tableInfo->PartitionConfig(), indexDataColumns, indexPostingTableDesc)));
        if (prefixVectorIndex) {
            const THashSet<TString> prefixColumns{indexDesc.GetKeyColumnNames().begin(), indexDesc.GetKeyColumnNames().end() - 1};
            result.push_back(createImplTable(CalcVectorKmeansTreePrefixImplTableDesc(prefixColumns, tableInfo, tableInfo->PartitionConfig(), implTableColumns, indexPrefixTableDesc)));
        }
    } else {
        NKikimrSchemeOp::TTableDescription indexTableDesc;
        // TODO After IndexImplTableDescriptions are persisted, this should be replaced with Y_ABORT_UNLESS
        if (indexDesc.IndexImplTableDescriptionsSize() == 1) {
            indexTableDesc = indexDesc.GetIndexImplTableDescriptions(0);
        }
        auto implTableDesc = CalcImplTableDesc(tableInfo, implTableColumns, indexTableDesc);
        // TODO if keep erase markers also speedup compaction or something else we can enable it for other impl tables too
        implTableDesc.MutablePartitionConfig()->MutableCompactionPolicy()->SetKeepEraseMarkers(true);
        result.push_back(createImplTable(std::move(implTableDesc)));
    }

    return result;
}

}
