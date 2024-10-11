#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/protos/tx_datashard.pb.h>

#include "schemeshard_tx_infly.h"

namespace NKikimr::NSchemeShard {

bool TTxState::IsItActuallyMerge() {
    return TxType == TTxState::TxSplitTablePartition
        && SplitDescription
        && SplitDescription->SourceRangesSize() > 1;
}

TTxState::ETxType TTxState::ConvertToTxType(NKikimrSchemeOp::EOperationType opType) {
    switch (opType) {
        case NKikimrSchemeOp::ESchemeOpMkDir: return TxMkDir;
        case NKikimrSchemeOp::ESchemeOpCreateTable: return TxCreateTable;
        case NKikimrSchemeOp::ESchemeOpCreatePersQueueGroup: return TxCreatePQGroup;
        case NKikimrSchemeOp::ESchemeOpDropTable: return TxDropTable;
        case NKikimrSchemeOp::ESchemeOpDropPersQueueGroup: return TxDropPQGroup;
        case NKikimrSchemeOp::ESchemeOpAlterTable: return TxAlterTable;
        case NKikimrSchemeOp::ESchemeOpAlterPersQueueGroup: return TxAlterPQGroup;
        case NKikimrSchemeOp::ESchemeOpModifyACL: return TxModifyACL;
        case NKikimrSchemeOp::ESchemeOpRmDir: return TxRmDir;
        case NKikimrSchemeOp::ESchemeOpSplitMergeTablePartitions: return TxSplitTablePartition;
        case NKikimrSchemeOp::ESchemeOpBackup: return TxBackup;
        case NKikimrSchemeOp::ESchemeOpCreateSubDomain: return TxCreateSubDomain;
        case NKikimrSchemeOp::ESchemeOpDropSubDomain: return TxDropSubDomain;
        case NKikimrSchemeOp::ESchemeOpCreateRtmrVolume: return TxCreateRtmrVolume;
        case NKikimrSchemeOp::ESchemeOpCreateBlockStoreVolume: return TxCreateBlockStoreVolume;
        case NKikimrSchemeOp::ESchemeOpAlterBlockStoreVolume: return TxAlterBlockStoreVolume;
        case NKikimrSchemeOp::ESchemeOpAssignBlockStoreVolume: return TxAssignBlockStoreVolume;
        case NKikimrSchemeOp::ESchemeOpDropBlockStoreVolume: return TxDropBlockStoreVolume;
        case NKikimrSchemeOp::ESchemeOpCreateKesus: return TxCreateKesus;
        case NKikimrSchemeOp::ESchemeOpDropKesus: return TxDropKesus;
        case NKikimrSchemeOp::ESchemeOpForceDropSubDomain: return TxForceDropSubDomain;
        case NKikimrSchemeOp::ESchemeOpCreateSolomonVolume: return TxCreateSolomonVolume;
        case NKikimrSchemeOp::ESchemeOpDropSolomonVolume: return TxDropSolomonVolume;
        case NKikimrSchemeOp::ESchemeOpAlterKesus: return TxAlterKesus;
        case NKikimrSchemeOp::ESchemeOpAlterSubDomain: return TxAlterSubDomain;
        case NKikimrSchemeOp::ESchemeOpAlterUserAttributes: return TxAlterUserAttributes;
        case NKikimrSchemeOp::ESchemeOpForceDropUnsafe: return TxForceDropSubDomain;
        case NKikimrSchemeOp::ESchemeOpCreateIndexedTable: return TxInvalid;
        case NKikimrSchemeOp::ESchemeOpCreateTableIndex: return TxCreateTableIndex;
        case NKikimrSchemeOp::ESchemeOpCreateConsistentCopyTables: return TxCopyTable;
        case NKikimrSchemeOp::ESchemeOpDropTableIndex: return TxDropTableIndex;
        case NKikimrSchemeOp::ESchemeOpCreateExtSubDomain: return TxCreateExtSubDomain;
        case NKikimrSchemeOp::ESchemeOpAlterExtSubDomain: return TxAlterExtSubDomain;
        case NKikimrSchemeOp::ESchemeOpForceDropExtSubDomain: return TxForceDropExtSubDomain;
        case NKikimrSchemeOp::ESchemeOp_DEPRECATED_35: return TxInvalid;
        case NKikimrSchemeOp::ESchemeOpUpgradeSubDomain: return TxUpgradeSubDomain;
        case NKikimrSchemeOp::ESchemeOpUpgradeSubDomainDecision: return TxUpgradeSubDomainDecision;
        case NKikimrSchemeOp::ESchemeOpCreateIndexBuild: return TxInvalid;
        case NKikimrSchemeOp::ESchemeOpInitiateBuildIndexMainTable: return TxInitializeBuildIndex;
        case NKikimrSchemeOp::ESchemeOpCreateLock: return TxCreateLock;
        case NKikimrSchemeOp::ESchemeOpApplyIndexBuild: return TxInvalid;
        case NKikimrSchemeOp::ESchemeOpFinalizeBuildIndexMainTable: return TxFinalizeBuildIndex;
        case NKikimrSchemeOp::ESchemeOpAlterTableIndex: return TxAlterTableIndex;
        case NKikimrSchemeOp::ESchemeOpAlterSolomonVolume: return TxAlterSolomonVolume;
        case NKikimrSchemeOp::ESchemeOpDropLock: return TxDropLock;
        case NKikimrSchemeOp::ESchemeOpFinalizeBuildIndexImplTable: return TxAlterTable;
        case NKikimrSchemeOp::ESchemeOpInitiateBuildIndexImplTable: return TxCreateTable;
        case NKikimrSchemeOp::ESchemeOpDropIndex: return TxInvalid;
        case NKikimrSchemeOp::ESchemeOpDropTableIndexAtMainTable: return TxDropTableIndexAtMainTable;
        case NKikimrSchemeOp::ESchemeOpCancelIndexBuild: return TxInvalid;
        case NKikimrSchemeOp::ESchemeOpCreateFileStore: return TxCreateFileStore;
        case NKikimrSchemeOp::ESchemeOpAlterFileStore: return TxAlterFileStore;
        case NKikimrSchemeOp::ESchemeOpDropFileStore: return TxDropFileStore;
        case NKikimrSchemeOp::ESchemeOpRestore: return TxRestore;
        case NKikimrSchemeOp::ESchemeOpCreateColumnStore: return TxCreateOlapStore;
        case NKikimrSchemeOp::ESchemeOpAlterColumnStore: return TxAlterOlapStore;
        case NKikimrSchemeOp::ESchemeOpDropColumnStore: return TxDropOlapStore;
        case NKikimrSchemeOp::ESchemeOpCreateColumnTable: return TxCreateColumnTable;
        case NKikimrSchemeOp::ESchemeOpAlterColumnTable: return TxAlterColumnTable;
        case NKikimrSchemeOp::ESchemeOpDropColumnTable: return TxDropColumnTable;
        case NKikimrSchemeOp::ESchemeOpAlterLogin: return TxInvalid;
        case NKikimrSchemeOp::ESchemeOpCreateCdcStream: return TxInvalid;
        case NKikimrSchemeOp::ESchemeOpCreateCdcStreamImpl: return TxCreateCdcStream;
        case NKikimrSchemeOp::ESchemeOpCreateCdcStreamAtTable: return TxCreateCdcStreamAtTable;
        case NKikimrSchemeOp::ESchemeOpAlterCdcStream: return TxInvalid;
        case NKikimrSchemeOp::ESchemeOpAlterCdcStreamImpl: return TxAlterCdcStream;
        case NKikimrSchemeOp::ESchemeOpAlterCdcStreamAtTable: return TxAlterCdcStreamAtTable;
        case NKikimrSchemeOp::ESchemeOpDropCdcStream: return TxInvalid;
        case NKikimrSchemeOp::ESchemeOpDropCdcStreamImpl: return TxDropCdcStream;
        case NKikimrSchemeOp::ESchemeOpDropCdcStreamAtTable: return TxDropCdcStreamAtTable;
        case NKikimrSchemeOp::ESchemeOpMoveTable: return TxMoveTable;
        case NKikimrSchemeOp::ESchemeOpMoveTableIndex: return TxMoveTableIndex;
        case NKikimrSchemeOp::ESchemeOpCreateSequence: return TxCreateSequence;
        case NKikimrSchemeOp::ESchemeOpAlterSequence: return TxAlterSequence;
        case NKikimrSchemeOp::ESchemeOpDropSequence: return TxDropSequence;
        case NKikimrSchemeOp::ESchemeOpCreateReplication: return TxCreateReplication;
        case NKikimrSchemeOp::ESchemeOpAlterReplication: return TxAlterReplication;
        case NKikimrSchemeOp::ESchemeOpDropReplication: return TxDropReplication;
        case NKikimrSchemeOp::ESchemeOpDropReplicationCascade: return TxDropReplicationCascade;
        case NKikimrSchemeOp::ESchemeOpCreateBlobDepot: return TxCreateBlobDepot;
        case NKikimrSchemeOp::ESchemeOpAlterBlobDepot: return TxAlterBlobDepot;
        case NKikimrSchemeOp::ESchemeOpDropBlobDepot: return TxDropBlobDepot;
        case NKikimrSchemeOp::ESchemeOpMoveIndex: return TxInvalid;
        case NKikimrSchemeOp::ESchemeOpCreateExternalTable: return TxCreateExternalTable;
        case NKikimrSchemeOp::ESchemeOpAlterExternalTable: return TxAlterExternalTable;
        case NKikimrSchemeOp::ESchemeOpCreateExternalDataSource: return TxCreateExternalDataSource;
        case NKikimrSchemeOp::ESchemeOpAlterExternalDataSource: return TxAlterExternalDataSource;
        case NKikimrSchemeOp::ESchemeOpCreateView: return TxCreateView;
        case NKikimrSchemeOp::ESchemeOpAlterView: return TxAlterView;
        case NKikimrSchemeOp::ESchemeOpDropView: return TxDropView;
        case NKikimrSchemeOp::ESchemeOpCreateResourcePool: return TxCreateResourcePool;
        case NKikimrSchemeOp::ESchemeOpAlterResourcePool: return TxAlterResourcePool;
        case NKikimrSchemeOp::ESchemeOpDropResourcePool: return TxDropResourcePool;
        case NKikimrSchemeOp::ESchemeOpAlterExtSubDomainCreateHive: return TxInvalid;
        case NKikimrSchemeOp::ESchemeOpDropExternalTable: return TxInvalid;
        case NKikimrSchemeOp::ESchemeOpDropExternalDataSource: return TxInvalid;
        case NKikimrSchemeOp::ESchemeOpCreateColumnBuild: return TxInvalid;
        case NKikimrSchemeOp::ESchemeOpCreateContinuousBackup: return TxInvalid;
        case NKikimrSchemeOp::ESchemeOpAlterContinuousBackup: return TxInvalid;
        case NKikimrSchemeOp::ESchemeOpDropContinuousBackup: return TxInvalid;
        case NKikimrSchemeOp::ESchemeOpRestoreIncrementalBackup: return TxInvalid;
        case NKikimrSchemeOp::ESchemeOpCreateBackupCollection: return TxCreateBackupCollection;
        case NKikimrSchemeOp::ESchemeOpAlterBackupCollection: return TxAlterBackupCollection;
        case NKikimrSchemeOp::ESchemeOpDropBackupCollection: return TxDropBackupCollection;
        default: return TxInvalid;
    }
}

}  // namespace NKikimr::NSchemeShard

