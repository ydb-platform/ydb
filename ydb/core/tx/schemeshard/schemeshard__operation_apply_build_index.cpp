#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_path_element.h"
#include "schemeshard_utils.h"

#include "schemeshard_impl.h"

#include <ydb/core/base/table_index.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <ydb/library/yql/minikql/mkql_type_ops.h>

namespace NKikimr {
namespace NSchemeShard {

TVector<ISubOperation::TPtr> ApplyBuildIndex(TOperationId nextId, const TTxTransaction& tx, TOperationContext& context) {
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpApplyIndexBuild);

    auto config = tx.GetApplyIndexBuild();
    TString tablePath = config.GetTablePath();
    TString indexName = config.GetIndexName();

    TPath table = TPath::Resolve(tablePath, context.SS);
    TVector<ISubOperation::TPtr> result;
    {
        auto finalize = TransactionTemplate(table.Parent().PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpFinalizeBuildIndexMainTable);
        *finalize.MutableLockGuard() = tx.GetLockGuard();
        auto op = finalize.MutableFinalizeBuildIndexMainTable();
        op->SetTableName(table.LeafName());
        op->SetSnapshotTxId(config.GetSnaphotTxId());  //TODO: fix spelling error in flat_scheme_op.proto first
        op->SetBuildIndexId(config.GetBuildIndexId());
        if (!indexName.empty()) {
            TPath index = table.Child(indexName);
            PathIdFromPathId(index.Base()->PathId, op->MutableOutcome()->MutableApply()->MutableIndexPathId());
        }

        result.push_back(CreateFinalizeBuildIndexMainTable(NextPartId(nextId, result), finalize));
    }

    if (!indexName.empty())
    {
        TPath index = table.Child(indexName);
        auto tableIndexAltering = TransactionTemplate(table.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpAlterTableIndex);
        *tableIndexAltering.MutableLockGuard() = tx.GetLockGuard();
        auto alterIndex = tableIndexAltering.MutableAlterTableIndex();
        alterIndex->SetName(index.LeafName());
        alterIndex->SetState(NKikimrSchemeOp::EIndexState::EIndexStateReady);

        result.push_back(CreateAlterTableIndex(NextPartId(nextId, result), tableIndexAltering));
    }

    if (!indexName.empty())
    {
        auto alterImplTableTransactionTemplate = [] (TPath index, TPath implIndexTable, TTableInfo::TPtr implIndexTableInfo) {
            auto indexImplTableAltering = TransactionTemplate(index.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpFinalizeBuildIndexImplTable);
            auto alterTable = indexImplTableAltering.MutableAlterTable();
            alterTable->SetName(implIndexTable.LeafName());
            alterTable->MutablePartitionConfig()->MutableCompactionPolicy()->CopyFrom(implIndexTableInfo->PartitionConfig().GetCompactionPolicy());
            alterTable->MutablePartitionConfig()->MutableCompactionPolicy()->SetKeepEraseMarkers(false);
            alterTable->MutablePartitionConfig()->SetShadowData(false);
            return indexImplTableAltering;
        };

        TPath index = table.Child(indexName);
        for (const std::string_view implTable : NTableIndex::ImplTables) {
            TPath implIndexTable = index.Child(implTable.data());
            if (!implIndexTable.IsResolved()) {
                continue;
            }
            TTableInfo::TPtr implIndexTableInfo = context.SS->Tables.at(implIndexTable.Base()->PathId);
            result.push_back(CreateFinalizeBuildIndexImplTable(NextPartId(nextId, result), alterImplTableTransactionTemplate(index, implIndexTable, implIndexTableInfo)));
        }
    }

    return result;
}

TVector<ISubOperation::TPtr> CancelBuildIndex(TOperationId nextId, const TTxTransaction& tx, TOperationContext& context) {
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpCancelIndexBuild);

    auto config = tx.GetCancelIndexBuild();
    TString tablePath = config.GetTablePath();
    TString indexName = config.GetIndexName();

    TPath table = TPath::Resolve(tablePath, context.SS);
    
    TVector<ISubOperation::TPtr> result;
    {
        auto finalize = TransactionTemplate(table.Parent().PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpFinalizeBuildIndexMainTable);
        *finalize.MutableLockGuard() = tx.GetLockGuard();
        auto op = finalize.MutableFinalizeBuildIndexMainTable();
        op->SetTableName(table.LeafName());
        op->SetSnapshotTxId(config.GetSnaphotTxId());
        op->SetBuildIndexId(config.GetBuildIndexId());

        if (!indexName.empty()) {
            TPath index = table.Child(indexName);
            PathIdFromPathId(index.Base()->PathId, op->MutableOutcome()->MutableCancel()->MutableIndexPathId());
        }

        result.push_back(CreateFinalizeBuildIndexMainTable(NextPartId(nextId, result), finalize));
    }

    if (!indexName.empty())
    {
        TPath index = table.Child(indexName);
        auto tableIndexDropping = TransactionTemplate(table.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpDropTableIndex);
        auto operation = tableIndexDropping.MutableDrop();
        operation->SetName(ToString(index.Base()->Name));

        result.push_back(CreateDropTableIndex(NextPartId(nextId, result), tableIndexDropping));
    }

    if (!indexName.empty()) {
        TPath index = table.Child(indexName);
        Y_ABORT_UNLESS(index.Base()->GetChildren().size() >= 1);
        for (auto& indexChildItems: index.Base()->GetChildren()) {
            const TString& implTableName = indexChildItems.first;
            Y_ABORT_UNLESS(NTableIndex::IsImplTable(implTableName), "unexpected name %s", implTableName.c_str());

            TPath implTable = index.Child(implTableName);
            {
                TPath::TChecker checks = implTable.Check();
                checks.NotEmpty()
                    .IsResolved()
                    .NotDeleted()
                    .IsTable()
                    .IsInsideTableIndexPath()
                    .NotUnderDeleting()
                    .NotUnderOperation();

                if (!checks) {
                    return {CreateReject(nextId, checks.GetStatus(), checks.GetError())};
                }
            }
            Y_ABORT_UNLESS(implTable.Base()->PathId == indexChildItems.second);

            {
                auto implTableDropping = TransactionTemplate(index.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpDropTable);
                auto operation = implTableDropping.MutableDrop();
                operation->SetName(ToString(implTable.Base()->Name));

                result.push_back(CreateDropTable(NextPartId(nextId,result), implTableDropping));
            }
        }
    }

    return result;
}

}
}
