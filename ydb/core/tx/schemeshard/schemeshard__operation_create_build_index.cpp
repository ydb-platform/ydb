#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"
#include "schemeshard_path_element.h"
#include "schemeshard_utils.h"

#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
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

        checks
            .IsValidLeafName()
            .PathsLimit(2) // index and impl-table
            .DirChildrenLimit()
            .ShardsLimit(1); // impl-table

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

        if (!indexDesc.HasType()) {
            outTx.MutableCreateTableIndex()->SetType(NKikimrSchemeOp::EIndexTypeGlobal);
        }

        result.push_back(CreateNewTableIndex(NextPartId(opId, result), outTx));
    }

    {
        auto outTx = TransactionTemplate(table.Parent().PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpInitiateBuildIndexMainTable);
        *outTx.MutableLockGuard() = tx.GetLockGuard();

        auto& snapshot = *outTx.MutableInitiateBuildIndexMainTable();
        snapshot.SetTableName(table.LeafName());

        result.push_back(CreateInitializeBuildIndexMainTable(NextPartId(opId, result), outTx));
    }

    auto createIndexImplTable = [&] (NKikimrSchemeOp::TTableDescription&& implTableDesc) {
        auto outTx = TransactionTemplate(index.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpInitiateBuildIndexImplTable);
        *outTx.MutableCreateTable() = implTableDesc;

        implTableDesc.MutablePartitionConfig()->SetShadowData(true);

        return CreateInitializeBuildIndexImplTable(NextPartId(opId, result), outTx);
    };

    if (indexDesc.GetType() == NKikimrSchemeOp::EIndexType::EIndexTypeGlobalVectorKmeansTree) {
        NKikimrSchemeOp::TTableDescription indexLevelTableDesc, indexPostingTableDesc;
        // TODO After IndexImplTableDescriptions are persisted, this should be replaced with Y_ABORT_UNLESS
        if (indexDesc.IndexImplTableDescriptionsSize() == 2) {
            indexLevelTableDesc = indexDesc.GetIndexImplTableDescriptions(0);
            indexPostingTableDesc = indexDesc.GetIndexImplTableDescriptions(0);
        }
        result.push_back(createIndexImplTable(CalcVectorKmeansTreeLevelImplTableDesc(tableInfo->PartitionConfig(), indexLevelTableDesc)));
        result.push_back(createIndexImplTable(CalcVectorKmeansTreePostingImplTableDesc(tableInfo, tableInfo->PartitionConfig(), implTableColumns, indexPostingTableDesc)));
    } else {
        NKikimrSchemeOp::TTableDescription indexTableDesc;
        // TODO After IndexImplTableDescriptions are persisted, this should be replaced with Y_ABORT_UNLESS
        if (indexDesc.IndexImplTableDescriptionsSize() == 1) {
            indexTableDesc = indexDesc.GetIndexImplTableDescriptions(0);
        }
        NKikimrSchemeOp::TTableDescription implTableDesc = CalcImplTableDesc(tableInfo, implTableColumns, indexTableDesc);
        implTableDesc.MutablePartitionConfig()->MutableCompactionPolicy()->SetKeepEraseMarkers(true);
        result.push_back(createIndexImplTable(std::move(implTableDesc)));
    }

    return result;
}

}
