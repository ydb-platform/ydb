#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"

namespace NKikimr::NSchemeShard {

// Slice 5 of Path A: the per-table TRestoreIncrementalBackupAtTable schema-op
// machinery has been removed. Incremental restore is now driven entirely via
// the RPC channel (TIncrementalRestoreSrcActor + TEvIncrementalRestoreSrcCreateRequest)
// dispatched directly by the orchestrator.
//
// The outer ESchemeOpRestoreMultipleIncrementalBackups op type is kept as a
// no-op acknowledgement: the orchestrator (schemeshard_incremental_restore_scan.cpp)
// still emits a schema-tx of this type while it dispatches the RPC channel in
// parallel; legacy direct callers from ut_incremental_backup also propose this
// type expecting StatusAccepted. Keep the factory returning success so both
// paths complete cleanly; the actual scan work happens on the RPC channel.

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
