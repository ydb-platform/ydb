#include <ydb/core/protos/schemeshard/operations.pb.h>

#include "schemeshard__op_traits.h"


namespace NKikimr::NSchemeShard {

enum EOperationClass {
    Create = 0,
    Alter,
    Drop,
    Other,
    Unknown
};

EOperationClass GetOperationClass(NKikimrSchemeOp::EOperationType op) {
    switch (op) {
        // Simple operations that create paths
        case NKikimrSchemeOp::EOperationType::ESchemeOpMkDir:
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateTable:
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreatePersQueueGroup:
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateSubDomain:
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateRtmrVolume:
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateBlockStoreVolume:
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateKesus:
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateSolomonVolume:
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateIndexedTable:
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateTableIndex:
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateExtSubDomain:
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateFileStore:
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateColumnStore:
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateColumnTable:
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateCdcStream:
        case NKikimrSchemeOp::EOperationType::ESchemeOpMoveTable:
        case NKikimrSchemeOp::EOperationType::ESchemeOpMoveTableIndex:
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateSequence:
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateReplication:
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateBlobDepot:
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateExternalTable:
        case NKikimrSchemeOp::EOperationType::ESchemeOpMoveIndex:
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateExternalDataSource:
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateView:
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateResourcePool:
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateContinuousBackup:
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateBackupCollection:
        case NKikimrSchemeOp::EOperationType::ESchemeOpMoveSequence:
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateTransfer:
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateSysView:
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateStreamingQuery:
            return EOperationClass::Create;

        // Simple operations that drop paths
        case NKikimrSchemeOp::EOperationType::ESchemeOpDropTable:
        case NKikimrSchemeOp::EOperationType::ESchemeOpDropPersQueueGroup:
        case NKikimrSchemeOp::EOperationType::ESchemeOpRmDir:
        case NKikimrSchemeOp::EOperationType::ESchemeOpDropSubDomain:
        case NKikimrSchemeOp::EOperationType::ESchemeOpDropBlockStoreVolume:
        case NKikimrSchemeOp::EOperationType::ESchemeOpDropKesus:
        case NKikimrSchemeOp::EOperationType::ESchemeOpForceDropSubDomain:
        case NKikimrSchemeOp::EOperationType::ESchemeOpDropSolomonVolume:
        case NKikimrSchemeOp::EOperationType::ESchemeOpForceDropUnsafe:
        case NKikimrSchemeOp::EOperationType::ESchemeOpDropTableIndex:
        case NKikimrSchemeOp::EOperationType::ESchemeOpForceDropExtSubDomain:
        case NKikimrSchemeOp::EOperationType::ESchemeOpDropLock:
        case NKikimrSchemeOp::EOperationType::ESchemeOpDropIndex:
        case NKikimrSchemeOp::EOperationType::ESchemeOpDropTableIndexAtMainTable:
        case NKikimrSchemeOp::EOperationType::ESchemeOpDropFileStore:
        case NKikimrSchemeOp::EOperationType::ESchemeOpDropColumnStore:
        case NKikimrSchemeOp::EOperationType::ESchemeOpDropColumnTable:
        case NKikimrSchemeOp::EOperationType::ESchemeOpDropCdcStream:
        case NKikimrSchemeOp::EOperationType::ESchemeOpDropSequence:
        case NKikimrSchemeOp::EOperationType::ESchemeOpDropReplication:
        case NKikimrSchemeOp::EOperationType::ESchemeOpDropBlobDepot:
        case NKikimrSchemeOp::EOperationType::ESchemeOpDropView:
        case NKikimrSchemeOp::EOperationType::ESchemeOpDropResourcePool:
        case NKikimrSchemeOp::EOperationType::ESchemeOpDropExternalTable:
        case NKikimrSchemeOp::EOperationType::ESchemeOpDropExternalDataSource:
        case NKikimrSchemeOp::EOperationType::ESchemeOpDropContinuousBackup:
        case NKikimrSchemeOp::EOperationType::ESchemeOpDropBackupCollection:
        case NKikimrSchemeOp::EOperationType::ESchemeOpDropTransfer:
        case NKikimrSchemeOp::EOperationType::ESchemeOpDropTransferCascade:
        case NKikimrSchemeOp::EOperationType::ESchemeOpDropSysView:
        case NKikimrSchemeOp::EOperationType::ESchemeOpDropStreamingQuery:
            return EOperationClass::Drop;

        // Simple operations that alter paths
        case NKikimrSchemeOp::EOperationType::ESchemeOpAlterTable:
        case NKikimrSchemeOp::EOperationType::ESchemeOpAlterPersQueueGroup:
        case NKikimrSchemeOp::EOperationType::ESchemeOpModifyACL:
        case NKikimrSchemeOp::EOperationType::ESchemeOpAlterBlockStoreVolume:
        case NKikimrSchemeOp::EOperationType::ESchemeOpAlterKesus:
        case NKikimrSchemeOp::EOperationType::ESchemeOpAlterSubDomain:
        case NKikimrSchemeOp::EOperationType::ESchemeOpAlterUserAttributes:
        case NKikimrSchemeOp::EOperationType::ESchemeOpAlterExtSubDomain:
        case NKikimrSchemeOp::EOperationType::ESchemeOpAlterTableIndex:
        case NKikimrSchemeOp::EOperationType::ESchemeOpAlterSolomonVolume:
        case NKikimrSchemeOp::EOperationType::ESchemeOpAlterFileStore:
        case NKikimrSchemeOp::EOperationType::ESchemeOpAlterColumnStore:
        case NKikimrSchemeOp::EOperationType::ESchemeOpAlterColumnTable:
        case NKikimrSchemeOp::EOperationType::ESchemeOpAlterCdcStream:
        case NKikimrSchemeOp::EOperationType::ESchemeOpAlterSequence:
        case NKikimrSchemeOp::EOperationType::ESchemeOpAlterReplication:
        case NKikimrSchemeOp::EOperationType::ESchemeOpAlterBlobDepot:
        case NKikimrSchemeOp::EOperationType::ESchemeOpAlterExternalTable:
        case NKikimrSchemeOp::EOperationType::ESchemeOpAlterExternalDataSource:
        case NKikimrSchemeOp::EOperationType::ESchemeOpAlterView:
        case NKikimrSchemeOp::EOperationType::ESchemeOpAlterResourcePool:
        case NKikimrSchemeOp::EOperationType::ESchemeOpAlterContinuousBackup:
        case NKikimrSchemeOp::EOperationType::ESchemeOpAlterBackupCollection:
        case NKikimrSchemeOp::EOperationType::ESchemeOpAlterTransfer:
        case NKikimrSchemeOp::EOperationType::ESchemeOpAlterStreamingQuery:
            return EOperationClass::Alter;

        // Compound or special operations
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateConsistentCopyTables:
        case NKikimrSchemeOp::EOperationType::ESchemeOpSplitMergeTablePartitions:
        case NKikimrSchemeOp::EOperationType::ESchemeOpBackup:
        case NKikimrSchemeOp::EOperationType::ESchemeOpAssignBlockStoreVolume:
        case NKikimrSchemeOp::EOperationType::ESchemeOp_DEPRECATED_35:
        case NKikimrSchemeOp::EOperationType::ESchemeOpUpgradeSubDomain:
        case NKikimrSchemeOp::EOperationType::ESchemeOpUpgradeSubDomainDecision:
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateIndexBuild:
        case NKikimrSchemeOp::EOperationType::ESchemeOpInitiateBuildIndexMainTable:
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateLock:
        case NKikimrSchemeOp::EOperationType::ESchemeOpApplyIndexBuild:
        case NKikimrSchemeOp::EOperationType::ESchemeOpFinalizeBuildIndexMainTable:
        case NKikimrSchemeOp::EOperationType::ESchemeOpFinalizeBuildIndexImplTable:
        case NKikimrSchemeOp::EOperationType::ESchemeOpInitiateBuildIndexImplTable:
        case NKikimrSchemeOp::EOperationType::ESchemeOpCancelIndexBuild:
        case NKikimrSchemeOp::EOperationType::ESchemeOpRestore:
        case NKikimrSchemeOp::EOperationType::ESchemeOpAlterLogin:
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateCdcStreamImpl:
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateCdcStreamAtTable:
        case NKikimrSchemeOp::EOperationType::ESchemeOpAlterCdcStreamImpl:
        case NKikimrSchemeOp::EOperationType::ESchemeOpAlterCdcStreamAtTable:
        case NKikimrSchemeOp::EOperationType::ESchemeOpDropCdcStreamImpl:
        case NKikimrSchemeOp::EOperationType::ESchemeOpDropCdcStreamAtTable:
        case NKikimrSchemeOp::EOperationType::ESchemeOpRotateCdcStream:
        case NKikimrSchemeOp::EOperationType::ESchemeOpRotateCdcStreamImpl:
        case NKikimrSchemeOp::EOperationType::ESchemeOpRotateCdcStreamAtTable:
        case NKikimrSchemeOp::EOperationType::ESchemeOpDropReplicationCascade:
        case NKikimrSchemeOp::EOperationType::ESchemeOpAlterExtSubDomainCreateHive:
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateColumnBuild:
        case NKikimrSchemeOp::EOperationType::ESchemeOpRestoreMultipleIncrementalBackups:
        case NKikimrSchemeOp::EOperationType::ESchemeOpRestoreIncrementalBackupAtTable:
        case NKikimrSchemeOp::EOperationType::ESchemeOpBackupBackupCollection:
        case NKikimrSchemeOp::EOperationType::ESchemeOpBackupIncrementalBackupCollection:
        case NKikimrSchemeOp::EOperationType::ESchemeOpRestoreBackupCollection:
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateLongIncrementalRestoreOp:
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateLongIncrementalBackupOp:
        case NKikimrSchemeOp::EOperationType::ESchemeOpChangePathState:
        case NKikimrSchemeOp::EOperationType::ESchemeOpIncrementalRestoreFinalize:
            return EOperationClass::Other;

        // intentionally no default -- to trigger [-Werror,-Wswitch] compile error on any new entry not handled here
    }

    return EOperationClass::Unknown;
}

bool IsCreatePathOperation(NKikimrSchemeOp::EOperationType op) {
    return (GetOperationClass(op) == EOperationClass::Create);
}

}  // namespace NKikimr::NSchemeShard
