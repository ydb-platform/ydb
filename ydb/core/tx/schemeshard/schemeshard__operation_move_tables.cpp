#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_path_element.h"

#include "schemeshard_impl.h"

#include <ydb/core/base/path.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

namespace NKikimr {
namespace NSchemeShard {

TVector<ISubOperation::TPtr> CreateConsistentMoveTable(TOperationId nextId, const TTxTransaction& tx, TOperationContext& context) {
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpMoveTable);

    TVector<ISubOperation::TPtr> result;

    const auto& moving = tx.GetMoveTable();
    const auto& srcStr = moving.GetSrcPath();
    const auto& dstStr = moving.GetDstPath();

    {
        TString errStr;
        if (!context.SS->CheckApplyIf(tx, errStr)) {
            return {CreateReject(nextId, NKikimrScheme::EStatus::StatusPreconditionFailed, errStr)};
        }
    }

    TPath srcPath = TPath::Resolve(srcStr, context.SS);
    {
        TPath::TChecker checks = srcPath.Check();
        checks.IsResolved()
              .NotDeleted()
              .IsTable()
              .NotAsyncReplicaTable()
              .IsCommonSensePath();

        if (!checks) {
            return {CreateReject(nextId, checks.GetStatus(), checks.GetError())};
        }
    }

    {
        TStringBuilder explain = TStringBuilder() << "fail checks";

        if (!context.SS->CheckLocks(srcPath.Base()->PathId, tx, explain)) {
            return {CreateReject(nextId, NKikimrScheme::StatusMultipleModifications, explain)};
        }
    }

    TPath dstPath = TPath::Resolve(dstStr, context.SS);

    result.push_back(CreateMoveTable(NextPartId(nextId, result), MoveTableTask(srcPath, dstPath)));

    for (auto& child: srcPath.Base()->GetChildren()) {
        auto name = child.first;

        TPath srcChildPath = srcPath.Child(name);
        if (srcChildPath.IsDeleted()) {
            continue;
        }

        if (srcChildPath.IsCdcStream()) {
            return {CreateReject(nextId, NKikimrScheme::StatusPreconditionFailed, "Cannot move table with cdc streams")};
        }

        if (srcChildPath.IsSequence()) {
            return {CreateReject(nextId, NKikimrScheme::StatusPreconditionFailed, "Cannot move table with sequences")};
        }

        TPath dstIndexPath = dstPath.Child(name);

        Y_ABORT_UNLESS(srcChildPath.Base()->PathId == child.second);
        Y_VERIFY_S(srcChildPath.Base()->GetChildren().size() == 1,
                   srcChildPath.PathString() << " has children " << srcChildPath.Base()->GetChildren().size());

        result.push_back(CreateMoveTableIndex(NextPartId(nextId, result), MoveTableIndexTask(srcChildPath, dstIndexPath)));

        TString srcImplTableName = srcChildPath.Base()->GetChildren().begin()->first;
        TPath srcImplTable = srcChildPath.Child(srcImplTableName);
        if (srcImplTable.IsDeleted()) {
            continue;
        }
        Y_ABORT_UNLESS(srcImplTable.Base()->PathId == srcChildPath.Base()->GetChildren().begin()->second);

        TPath dstImplTable = dstIndexPath.Child(srcImplTableName);

        result.push_back(CreateMoveTable(NextPartId(nextId, result), MoveTableTask(srcImplTable, dstImplTable)));
    }

    return result;
}

}
}
