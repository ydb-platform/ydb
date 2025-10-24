#include "schemeshard__operation_common.h"
#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"
#include "schemeshard_path_element.h"
#include "schemeshard_utils.h"  // for TransactionTemplate

#include <ydb/core/base/path.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>

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
        if (!srcPath->IsTable() && !srcPath->IsColumnTable()) {
            return {CreateReject(nextId, NKikimrScheme::StatusPreconditionFailed, "Cannot move non-tables")};
        }
        if (srcPath->IsColumnTable() && !AppData()->FeatureFlags.GetEnableMoveColumnTable()) {
            return {CreateReject(nextId, NKikimrScheme::StatusPreconditionFailed, "RENAME is prohibited for column tables")};
        }
        TPath::TChecker checks = srcPath.Check();
        checks.IsResolved()
              .NotDeleted()
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

    THashSet<TString> sequences = GetLocalSequences(context, srcPath);

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
            continue;
        }

        TPath dstIndexPath = dstPath.Child(name);

        Y_ABORT_UNLESS(srcChildPath.Base()->PathId == child.second);

        result.push_back(CreateMoveTableIndex(NextPartId(nextId, result), MoveTableIndexTask(srcChildPath, dstIndexPath)));

        for (const auto& [implTableName, implTablePathId]: srcChildPath.Base()->GetChildren()) {
            TPath srcImplTable = srcChildPath.Child(implTableName);
            if (srcImplTable.IsDeleted()) {
                continue;
            }
            Y_ABORT_UNLESS(srcImplTable.Base()->PathId == implTablePathId);

            TPath dstImplTable = dstIndexPath.Child(implTableName);

            result.push_back(CreateMoveTable(NextPartId(nextId, result), MoveTableTask(srcImplTable, dstImplTable)));
            AddMoveSequences(nextId, result, srcImplTable, dstImplTable.PathString(), GetLocalSequences(context, srcImplTable));
        }
    }

    AddMoveSequences(nextId, result, srcPath, dstPath.PathString(), sequences);

    return result;
}

void AddMoveSequences(TOperationId nextId, TVector<ISubOperation::TPtr>& result, const TPath& srcTable,
    const TString& dstPath, const THashSet<TString>& sequences)
{
    for (const auto& sequence : sequences) {
        auto scheme = TransactionTemplate(dstPath, NKikimrSchemeOp::EOperationType::ESchemeOpMoveSequence);
        scheme.SetFailOnExist(true);

        auto* moveSequence = scheme.MutableMoveSequence();
        moveSequence->SetSrcPath(srcTable.PathString() + "/" + sequence);
        moveSequence->SetDstPath(dstPath + "/" + sequence);

        result.push_back(CreateMoveSequence(NextPartId(nextId, result), scheme));
    }
}

}
}
