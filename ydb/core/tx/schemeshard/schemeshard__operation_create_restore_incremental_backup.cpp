#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateRestoreIncrementalBackupAtTable(TOperationId id, const TTxTransaction&) {
    return CreateReject(id, NKikimrScheme::StatusInvalidParameter,
        "ESchemeOpRestoreIncrementalBackupAtTable is no longer supported; "
        "use the RPC-driven incremental restore channel");
}

ISubOperation::TPtr CreateRestoreIncrementalBackupAtTable(TOperationId id, TTxState::ETxState, TOperationContext&) {
    return CreateReject(id, NKikimrScheme::StatusInvalidParameter,
        "ESchemeOpRestoreIncrementalBackupAtTable is no longer supported; "
        "use the RPC-driven incremental restore channel");
}

bool CreateRestoreMultipleIncrementalBackups(
    TOperationId opId,
    const TTxTransaction& tx,
    TOperationContext&,
    bool /*dstCreatedInSameOp*/,
    TVector<ISubOperation::TPtr>& result)
{
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpRestoreMultipleIncrementalBackups);

    result = {CreateReject(opId, NKikimrScheme::StatusInvalidParameter,
        "ESchemeOpRestoreMultipleIncrementalBackups schema-op dispatch has been retired; "
        "the incremental restore orchestrator now uses the RPC channel "
        "(TEvIncrementalRestoreSrcCreateRequest) instead")};
    return false;
}

TVector<ISubOperation::TPtr> CreateRestoreMultipleIncrementalBackups(TOperationId opId, const TTxTransaction& tx, TOperationContext& context) {
    TVector<ISubOperation::TPtr> result;
    CreateRestoreMultipleIncrementalBackups(opId, tx, context, false, result);
    return result;
}

} // namespace NKikimr::NSchemeShard
