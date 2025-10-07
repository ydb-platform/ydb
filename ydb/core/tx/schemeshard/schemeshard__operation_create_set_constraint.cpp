#include "schemeshard__operation_common.h"
#include "schemeshard__operation_part.h"
#include "schemeshard_utils.h"  // for TransactionTemplate

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/ydb_convert/table_description.h>

namespace NKikimr::NSchemeShard {

TVector<ISubOperation::TPtr> CreateSetColumnConstraintsInitiate(TOperationId opId, const TTxTransaction& tx, TOperationContext& context) {
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpCreateSetColumnConstraintsInitiate);
    Y_UNUSED(context);
    auto tablePathStr = NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetSetColumnConstraintsInitiate().GetTableName()});

    if (!context.SS->EnableSetColumnConstraints) {
        TString error = "CreateSetColumnConstraintsInitiate is not implemented. TablePath = '" + tablePathStr + "'";
        return {CreateReject(opId, NKikimrScheme::EStatus::StatusPreconditionFailed, std::move(error))};
    }

    const auto tablePath = TPath::Resolve(tablePathStr, context.SS);

    {
        TPath::TChecker checks = tablePath.Check();
        checks
            .NotEmpty()
            .NotUnderDomainUpgrade()
            .IsAtLocalSchemeShard()
            .IsResolved()
            .NotDeleted()
            .IsTable()
            .NotUnderOperation();

        if (!checks) {
            return {CreateReject(opId, checks.GetStatus(), checks.GetError())};
        }
    }

    const auto& op = tx.GetSetColumnConstraintsInitiate();

    TTableInfo::TPtr table = context.SS->Tables.at(tablePath.Base()->PathId);
    std::vector<TString> tableColumns;
    tableColumns.reserve(table->Columns.size());
    {
        for (const auto& [colId, colInfo] : table->Columns) {
            Y_ENSURE(colId == colInfo.Id);

            if (colInfo.IsDropped()) {
                continue;
            }

            tableColumns.push_back(colInfo.Name);
        }
    }

    // check column's existence in O((|tableColumns| + |ConstraintSettings|) * log(|tableColumns|))
    std::sort(tableColumns.begin(), tableColumns.end());
    for (const auto& constraint : op.GetConstraintSettings()) {
        const auto& col = constraint.GetColumnName();

        if (auto it = std::lower_bound(tableColumns.begin(), tableColumns.end(), col); it == tableColumns.end() || *it != col) {
            TString error = "Table '" + tablePathStr + "' " + "does not have column '" + col + "'";
            return {CreateReject(opId, NKikimrScheme::EStatus::StatusPreconditionFailed, std::move(error))};
        }
    }

    TVector<ISubOperation::TPtr> result;

    {
        auto outTx = TransactionTemplate(tablePath.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpCreateSetColumnConstraintsLock);
        *outTx.MutableLockGuard() = tx.GetLockGuard();

        outTx.MutableSetColumnConstraintsLock()->SetTableName(op.GetTableName());
        outTx.MutableSetColumnConstraintsLock()->MutableConstraintSettings()->CopyFrom(op.GetConstraintSettings());

        outTx.SetInternal(true);

        result.push_back(CreateSetColumnConstraintsLock(NextPartId(opId, result), outTx));
    }

    {
        auto outTx = TransactionTemplate(tablePath.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpCreateSetColumnConstraintsCheck);
        *outTx.MutableLockGuard() = tx.GetLockGuard();

        outTx.MutableSetColumnConstraintsCheck()->SetTableName(op.GetTableName());
        outTx.MutableSetColumnConstraintsCheck()->MutableConstraintSettings()->CopyFrom(op.GetConstraintSettings());

        outTx.SetInternal(true);

        result.push_back(CreateSetColumnConstraintsCheck(NextPartId(opId, result), outTx));
    }

    {
        auto outTx = TransactionTemplate(tablePath.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpCreateSetColumnConstraintsFinalize);

        outTx.MutableSetColumnConstraintsFinalize()->SetTableName(op.GetTableName());
        outTx.MutableSetColumnConstraintsFinalize()->MutableConstraintSettings()->CopyFrom(op.GetConstraintSettings());

        outTx.SetInternal(true);

        result.push_back(CreateSetColumnConstraintsFinalize(NextPartId(opId, result), outTx));
    }

    return result;
}

} // namespace NKikimr::NSchemeShard
