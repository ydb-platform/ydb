#include "schemeshard__operation_change_path_state.h"
#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

namespace NKikimr::NSchemeShard {

namespace {

bool BuildLockSubOps(TOperationId opId, const TTxTransaction& tx, TOperationContext& context,
                     bool lock, TVector<ISubOperation::TPtr>& result)
{
    if (!tx.HasIncrementalRestoreLockTargets()) {
        result = {CreateReject(opId, NKikimrScheme::StatusInvalidParameter,
            "Missing IncrementalRestoreLockTargets in ModifyScheme")};
        return false;
    }

    const auto& targets = tx.GetIncrementalRestoreLockTargets();
    if (targets.DstPathsSize() + targets.SrcPathsSize() == 0) {
        result = {CreateReject(opId, NKikimrScheme::StatusInvalidParameter,
            "IncrementalRestoreLockTargets must specify at least one DstPath or SrcPath")};
        return false;
    }

    const TString workingDir = tx.GetWorkingDir();
    const auto dstState = lock
        ? NKikimrSchemeOp::EPathStateIncomingIncrementalRestore
        : NKikimrSchemeOp::EPathStateNoChanges;
    const auto srcState = lock
        ? NKikimrSchemeOp::EPathStateOutgoingIncrementalRestore
        : NKikimrSchemeOp::EPathStateNoChanges;

    YDB_LOG_CTX_INFO(context.Ctx, "BuildLockSubOps for incremental restore",
        {"TabletID", context.SS->TabletID()},
        {"opId", opId},
        {"lock", lock},
        {"workingDir", workingDir},
        {"dstPaths", targets.DstPathsSize()},
        {"srcPaths", targets.SrcPathsSize()},
        {"restoreOpId", targets.GetRestoreOpId()});

    // Absolute paths ("/...") are passed with workingDir="" to avoid double-joining.
    auto fanOut = [&](const ::google::protobuf::RepeatedPtrField<TString>& paths,
                      NKikimrSchemeOp::EPathState newState) -> bool {
        for (const auto& path : paths) {
            const bool isAbsolute = !path.empty() && path[0] == '/';
            TTxTransaction subTx;
            subTx.SetOperationType(NKikimrSchemeOp::ESchemeOpChangePathState);
            subTx.SetInternal(true);
            subTx.SetWorkingDir(isAbsolute ? TString() : workingDir);

            auto& change = *subTx.MutableChangePathState();
            change.SetPath(path);
            change.SetTargetState(newState);

            if (!CreateChangePathState(opId, subTx, context, result)) {
                return false;
            }
        }
        return true;
    };

    if (!fanOut(targets.GetDstPaths(), dstState)) {
        return false;
    }
    if (!fanOut(targets.GetSrcPaths(), srcState)) {
        return false;
    }
    return true;
}

} // namespace

ISubOperation::TPtr CreateIncrementalRestoreLockTargets(TOperationId opId, const TTxTransaction& tx) {
    Y_UNUSED(opId);
    Y_UNUSED(tx);
    Y_ABORT("use the (opId, tx, context) overload");
}

ISubOperation::TPtr CreateIncrementalRestoreLockTargets(TOperationId opId, TTxState::ETxState state) {
    Y_UNUSED(opId);
    Y_UNUSED(state);
    Y_ABORT("lock op is propose-only");
}

TVector<ISubOperation::TPtr> CreateIncrementalRestoreLockTargets(TOperationId opId, const TTxTransaction& tx, TOperationContext& context) {
    TVector<ISubOperation::TPtr> result;
    BuildLockSubOps(opId, tx, context, /*lock=*/true, result);
    return result;
}

ISubOperation::TPtr CreateIncrementalRestoreUnlockTargets(TOperationId opId, const TTxTransaction& tx) {
    Y_UNUSED(opId);
    Y_UNUSED(tx);
    Y_ABORT("use the (opId, tx, context) overload");
}

ISubOperation::TPtr CreateIncrementalRestoreUnlockTargets(TOperationId opId, TTxState::ETxState state) {
    Y_UNUSED(opId);
    Y_UNUSED(state);
    Y_ABORT("unlock op is propose-only");
}

TVector<ISubOperation::TPtr> CreateIncrementalRestoreUnlockTargets(TOperationId opId, const TTxTransaction& tx, TOperationContext& context) {
    TVector<ISubOperation::TPtr> result;
    BuildLockSubOps(opId, tx, context, /*lock=*/false, result);
    return result;
}

} // namespace NKikimr::NSchemeShard
