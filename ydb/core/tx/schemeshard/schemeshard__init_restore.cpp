#include "schemeshard__init_tx.h"

namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

void TSchemeShard::TTxInit::RestoreIncrementalRestoreOpPathStates(const NKikimrSchemeOp::TLongIncrementalRestoreOp& op) {
        TPathId backupCollectionPathId = TPathId(
            op.GetBackupCollectionPathId().GetOwnerId(),
            op.GetBackupCollectionPathId().GetLocalId()
        );

        for (const auto& tablePath : op.GetTablePathList()) {
            TPath resolvedPath = TPath::Resolve(tablePath, Self);
            if (resolvedPath.IsResolved() && Self->PathsById.contains(resolvedPath.Base()->PathId)) {
                auto targetPathElement = Self->PathsById.at(resolvedPath.Base()->PathId);
                Y_VERIFY_S(!targetPathElement->Dropped(),
                    "RestoreIncrementalRestoreOpPathStates: path is dropped but data restore is in-flight"
                    << ", pathId: " << resolvedPath.Base()->PathId
                    << ", StepDropped: " << targetPathElement->StepDropped);
                targetPathElement->PathState = TPathElement::EPathState::EPathStateIncomingIncrementalRestore;
            }
        }

        if (Self->PathsById.contains(backupCollectionPathId)) {
            auto sourcePath = Self->PathsById.at(backupCollectionPathId);
            Y_VERIFY_S(!sourcePath->Dropped(),
                "RestoreIncrementalRestoreOpPathStates: path is dropped but data restore is in-flight"
                << ", pathId: " << backupCollectionPathId
                << ", StepDropped: " << sourcePath->StepDropped);
            sourcePath->PathState = TPathElement::EPathState::EPathStateOutgoingIncrementalRestore;

            TPath backupCollectionPath = TPath::Init(backupCollectionPathId, Self);
            if (backupCollectionPath.IsResolved()) {
                TString backupCollectionPathStr = backupCollectionPath.PathString();

                if (op.HasFullBackupTrimmedName()) {
                    TString fullBackupName = NBackup::FullBackupDirName(op.GetFullBackupTrimmedName());
                    TString fullBackupPath = backupCollectionPathStr + "/" + fullBackupName;

                    for (const auto& tablePath : op.GetTablePathList()) {
                        TPath originalTablePath = TPath::Resolve(tablePath, Self);
                        if (originalTablePath.IsResolved()) {
                            TString tableName = originalTablePath.LeafName();
                            TString fullBackupTablePath = fullBackupPath + "/" + tableName;

                            TPath fullBackupTableResolvedPath = TPath::Resolve(fullBackupTablePath, Self);
                            if (fullBackupTableResolvedPath.IsResolved() && Self->PathsById.contains(fullBackupTableResolvedPath.Base()->PathId)) {
                                auto backupTablePathElement = Self->PathsById.at(fullBackupTableResolvedPath.Base()->PathId);
                                Y_VERIFY_S(!backupTablePathElement->Dropped(),
                                    "RestoreIncrementalRestoreOpPathStates: path is dropped but data restore is in-flight"
                                    << ", pathId: " << fullBackupTableResolvedPath.Base()->PathId
                                    << ", StepDropped: " << backupTablePathElement->StepDropped);
                                backupTablePathElement->PathState = TPathElement::EPathState::EPathStateOutgoingIncrementalRestore;
                            }
                        }
                    }
                }

                for (const auto& trimmedIncrName : op.GetIncrementalBackupTrimmedNames()) {
                    TString incrBackupName = NBackup::IncrementalBackupDirName(trimmedIncrName);
                    TString incrBackupPath = backupCollectionPathStr + "/" + incrBackupName;

                    for (const auto& tablePath : op.GetTablePathList()) {
                        TPath originalTablePath = TPath::Resolve(tablePath, Self);
                        if (originalTablePath.IsResolved()) {
                            TString tableName = originalTablePath.LeafName();
                            TString incrBackupTablePath = incrBackupPath + "/" + tableName;

                            TPath incrBackupTableResolvedPath = TPath::Resolve(incrBackupTablePath, Self);
                            if (incrBackupTableResolvedPath.IsResolved() && Self->PathsById.contains(incrBackupTableResolvedPath.Base()->PathId)) {
                                auto backupTablePathElement = Self->PathsById.at(incrBackupTableResolvedPath.Base()->PathId);
                                Y_VERIFY_S(!backupTablePathElement->Dropped(),
                                    "RestoreIncrementalRestoreOpPathStates: path is dropped but data restore is in-flight"
                                    << ", pathId: " << incrBackupTableResolvedPath.Base()->PathId
                                    << ", StepDropped: " << backupTablePathElement->StepDropped);
                                backupTablePathElement->PathState = TPathElement::EPathState::EPathStateAwaitingOutgoingIncrementalRestore;
                            }
                        }
                    }
                }
            }
        }
    }

void TSchemeShard::TTxInit::ScheduleOrphanedIncrementalRestoreOp(const TOperationId& opId, const NKikimrSchemeOp::TLongIncrementalRestoreOp& op, TSideEffects& onComplete, const TActorContext& ctx) {
        TTxId txId = opId.GetTxId();

        bool controlOperationExists = false;
        for (const auto& [txOpId, txState] : Self->TxInFlight) {
            if (txOpId.GetTxId() == txId) {
                controlOperationExists = true;
                break;
            }
        }

        if (!controlOperationExists) {
            TPathId backupCollectionPathId;
            backupCollectionPathId.OwnerId = op.GetBackupCollectionPathId().GetOwnerId();
            backupCollectionPathId.LocalPathId = op.GetBackupCollectionPathId().GetLocalId();

            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TTxInit detected orphaned incremental restore operation during recovery"
                    << ", operationId: " << opId
                    << ", txId: " << txId
                    << ", backupCollectionPathId: " << backupCollectionPathId
                    << ", scheduling TTxProgress to continue operation"
                    << ", at schemeshard: " << Self->TabletID());

            TVector<TString> backupNames;
            for (const auto& name : op.GetIncrementalBackupTrimmedNames()) {
                backupNames.push_back(name);
            }
            RestoreIncrementalRestoreOpPathStates(op);
            onComplete.Send(Self->SelfId(),
                new TEvPrivate::TEvRunIncrementalRestore(backupCollectionPathId, opId, backupNames));
        }
    }


} // namespace NSchemeShard
} // namespace NKikimr
