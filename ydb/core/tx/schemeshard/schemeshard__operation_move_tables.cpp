#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_path_element.h"

#include "schemeshard_impl.h"

#include "schemeshard_utils.h"  // for TransactionTemplate

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

    THashSet<TString> sequences;
    for (const auto& child: srcPath.Base()->GetChildren()) {
        auto name = child.first;
        auto pathId = child.second;

        TPath childPath = srcPath.Child(name);
        if (!childPath.IsSequence() || childPath.IsDeleted()) {
            continue;
        }

        Y_ABORT_UNLESS(childPath.Base()->PathId == pathId);

        TSequenceInfo::TPtr sequenceInfo = context.SS->Sequences.at(pathId);
        const auto& sequenceDesc = sequenceInfo->Description;
        const auto& sequenceName = sequenceDesc.GetName();

        sequences.emplace(sequenceName);
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
        }
    }

    for (const auto& sequence : sequences) {
        auto scheme = TransactionTemplate(
            dstPath.PathString(),
            NKikimrSchemeOp::EOperationType::ESchemeOpMoveSequence);
        scheme.SetFailOnExist(true);

        auto* moveSequence = scheme.MutableMoveSequence();
        moveSequence->SetSrcPath(srcPath.PathString() + "/" + sequence);
        moveSequence->SetDstPath(dstPath.PathString() + "/" + sequence);

        result.push_back(CreateMoveSequence(NextPartId(nextId, result), scheme));
    }

    return result;
}

}
}
