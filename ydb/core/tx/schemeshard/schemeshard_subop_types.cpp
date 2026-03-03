#include <util/generic/string.h>
#include <util/string/printf.h>

#include <ydb/core/protos/counters_schemeshard.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>

#include "schemeshard_subop_types.h"


namespace NKikimr::NSchemeShard {

#define TX_STATE_ENUM_NAME(n, ...) case n: return #n;
TString TxTypeName(ETxType t) {
    switch (t) {
        TX_STATE_TYPE_ENUM(TX_STATE_ENUM_NAME)
    default:
        return Sprintf("Unknown tx type %d", t);
    }
}
#undef TX_STATE_ENUM_NAME

#define TX_STATE_IN_FLIGHT_COUNTER(n, ...) case n: return COUNTER_IN_FLIGHT_OPS_##n;
ui32 TxTypeInFlightCounter(ETxType t) {
    switch (t) {
        TX_STATE_TYPE_ENUM(TX_STATE_IN_FLIGHT_COUNTER)
    default:
        return COUNTER_IN_FLIGHT_OPS_UNKNOWN;
    }
}
#undef TX_STATE_IN_FLIGHT_COUNTER

#define TX_STATE_FINISHED_COUNTER(n, ...) case n: return COUNTER_FINISHED_OPS_##n;
ui32 TxTypeFinishedCounter(ETxType t) {
    switch (t) {
        TX_STATE_TYPE_ENUM(TX_STATE_FINISHED_COUNTER)
    default:
        return COUNTER_FINISHED_OPS_UNKNOWN;
    }
}
#undef TX_STATE_FINISHED_COUNTER

#define TX_STATE_FROM_COUNTER(n, ...) case ESimpleCounters::COUNTER_IN_FLIGHT_OPS_##n: return ETxType::n;
ETxType ConvertToTxType(ESimpleCounters t) {
    switch (t) {
        TX_STATE_TYPE_ENUM(TX_STATE_FROM_COUNTER)
    default:
        return ETxType::TxInvalid;
    }
}
#undef TX_STATE_FROM_COUNTER


bool IsCreate(ETxType t) {
    switch (t) {
        case TxMkDir:
        case TxCreateTable:
        case TxCopyTable:
        case TxReadOnlyCopyColumnTable:
        case TxCreateOlapStore:
        case TxCreateColumnTable:
        case TxCreatePQGroup:
        case TxCreateSubDomain:
        case TxCreateExtSubDomain:
        case TxCreateBlockStoreVolume:
        case TxCreateFileStore:
        case TxCreateKesus:
        case TxCreateSolomonVolume:
        case TxCreateRtmrVolume:
        case TxCreateTableIndex:
        case TxFillIndex:
        case TxCreateCdcStream:
        case TxCreateSequence:
        case TxCreateReplication:
        case TxCreateTransfer:
        case TxCreateBlobDepot:
        case TxCreateExternalTable:
        case TxCreateExternalDataSource:
        case TxCreateView:
        case TxCopySequence:
        case TxCreateContinuousBackup:
        case TxCreateResourcePool:
        case TxCreateBackupCollection:
        case TxCreateSysView:
        case TxCreateLongIncrementalRestoreOp:
        case TxCreateLongIncrementalBackupOp:
        case TxCreateSecret:
        case TxCreateStreamingQuery:
            return true; // IsCreate
        case TxIncrementalRestoreFinalize:
            return false; // IsCreate
        case TxInitializeBuildIndex: //this is more like alter
        case TxCreateCdcStreamAtTable:
        case TxCreateCdcStreamAtTableWithInitialScan:
            return false; // IsCreate
        case TxCreateLock: //this is more like alter
        case TxDropLock: //this is more like alter
            return false; // IsCreate
        case TxDropTable:
        case TxDropOlapStore:
        case TxDropColumnTable:
        case TxDropPQGroup:
        case TxDropSubDomain:
        case TxDropBlockStoreVolume:
        case TxDropFileStore:
        case TxDropKesus:
        case TxForceDropSubDomain:
        case TxForceDropExtSubDomain:
        case TxDropTableIndex:
        case TxDropSolomonVolume:
        case TxRmDir:
        case TxFinalizeBuildIndex:
        case TxDropTableIndexAtMainTable:
        case TxDropCdcStream:
        case TxDropCdcStreamAtTable:
        case TxDropCdcStreamAtTableDropSnapshot:
        case TxDropSequence:
        case TxDropReplication:
        case TxDropReplicationCascade:
        case TxDropTransfer:
        case TxDropTransferCascade:
        case TxDropBlobDepot:
        case TxUpdateMainTableOnIndexMove:
        case TxDropExternalTable:
        case TxDropExternalDataSource:
        case TxDropView:
        case TxDropContinuousBackup:
        case TxDropResourcePool:
        case TxDropBackupCollection:
        case TxDropSysView:
        case TxDropStreamingQuery:
        case TxDropSecret:
            return false; // IsCreate
        case TxAlterPQGroup:
        case TxAlterTable:
        case TxAlterOlapStore:
        case TxAlterColumnTable:
        case TxModifyACL:
        case TxSplitTablePartition:
        case TxMergeTablePartition:
        case TxBackup:
        case TxRestore:
        case TxAlterBlockStoreVolume:
        case TxAssignBlockStoreVolume:
        case TxAlterFileStore:
        case TxAlterKesus:
        case TxAlterSubDomain:
        case TxUpgradeSubDomain:
        case TxUpgradeSubDomainDecision:
        case TxAlterExtSubDomain:
        case TxAlterExtSubDomainCreateHive:
        case TxAlterUserAttributes:
        case TxAlterTableIndex:
        case TxAlterSolomonVolume:
        case TxAlterCdcStream:
        case TxAlterCdcStreamAtTable:
        case TxAlterCdcStreamAtTableDropSnapshot:
        case TxAlterSequence:
        case TxAlterReplication:
        case TxAlterTransfer:
        case TxAlterBlobDepot:
        case TxAlterExternalTable:
        case TxAlterExternalDataSource:
        case TxAlterView:
        case TxAlterContinuousBackup:
        case TxAlterResourcePool:
        case TxRestoreIncrementalBackupAtTable:
        case TxAlterBackupCollection:
        case TxChangePathState:
        case TxAlterSecret:
        case TxAlterStreamingQuery:
            return false; // IsCreate
        case TxMoveTable:
        case TxMoveTableIndex:
        case TxMoveSequence:
            return true; // IsCreate
        case TxRotateCdcStream:
            return true; // IsCreate
        case TxRotateCdcStreamAtTable:
            return false; // IsCreate
        case TxTruncateTable:
            return false; // IsCreate
        case TxInvalid:
        case TxAllocatePQ:
            Y_DEBUG_ABORT("UNREACHABLE");
            Y_UNREACHABLE();

        //NOTE: intentionally no default: case
    }
}

bool IsDrop(ETxType t) {
    switch (t) {
        case TxDropTable:
        case TxDropOlapStore:
        case TxDropColumnTable:
        case TxDropPQGroup:
        case TxDropSubDomain:
        case TxDropBlockStoreVolume:
        case TxDropFileStore:
        case TxDropKesus:
        case TxForceDropSubDomain:
        case TxForceDropExtSubDomain:
        case TxDropTableIndex:
        case TxDropSolomonVolume:
        case TxRmDir:
        case TxDropCdcStream:
        case TxDropSequence:
        case TxDropReplication:
        case TxDropReplicationCascade:
        case TxDropTransfer:
        case TxDropTransferCascade:
        case TxDropBlobDepot:
        case TxDropExternalTable:
        case TxDropExternalDataSource:
        case TxDropView:
        case TxDropContinuousBackup:
        case TxDropResourcePool:
        case TxDropBackupCollection:
        case TxDropSysView:
        case TxDropSecret:
        case TxDropStreamingQuery:
            return true; // IsDrop
        case TxIncrementalRestoreFinalize:
            return false; // IsDrop
        case TxMkDir:
        case TxCreateTable:
        case TxCopyTable:
        case TxReadOnlyCopyColumnTable:
        case TxCreateOlapStore:
        case TxCreateColumnTable:
        case TxCreatePQGroup:
        case TxCreateSubDomain:
        case TxCreateExtSubDomain:
        case TxCreateBlockStoreVolume:
        case TxCreateFileStore:
        case TxCreateKesus:
        case TxCreateSolomonVolume:
        case TxCreateRtmrVolume:
        case TxCreateTableIndex:
        case TxFillIndex:
        case TxCreateCdcStream:
        case TxCreateCdcStreamAtTable:
        case TxCreateCdcStreamAtTableWithInitialScan:
        case TxCreateSequence:
        case TxCreateReplication:
        case TxCreateTransfer:
        case TxCreateBlobDepot:
        case TxInitializeBuildIndex:
        case TxCreateLock:
        case TxDropLock:
        case TxFinalizeBuildIndex:
        case TxDropTableIndexAtMainTable: // just increments schemaversion at main table
        case TxDropCdcStreamAtTable:
        case TxDropCdcStreamAtTableDropSnapshot:
        case TxUpdateMainTableOnIndexMove:
        case TxCreateExternalTable:
        case TxCreateExternalDataSource:
        case TxCreateView:
        case TxCopySequence:
        case TxCreateContinuousBackup:
        case TxCreateResourcePool:
        case TxRestoreIncrementalBackupAtTable:
        case TxCreateBackupCollection:
        case TxCreateSysView:
        case TxCreateLongIncrementalRestoreOp:
        case TxCreateLongIncrementalBackupOp:
        case TxCreateSecret:
        case TxCreateStreamingQuery:
            return false; // IsDrop
        case TxAlterPQGroup:
        case TxAlterTable:
        case TxAlterOlapStore:
        case TxAlterColumnTable:
        case TxModifyACL:
        case TxSplitTablePartition:
        case TxMergeTablePartition:
        case TxBackup:
        case TxRestore:
        case TxAlterBlockStoreVolume:
        case TxAssignBlockStoreVolume:
        case TxAlterFileStore:
        case TxAlterKesus:
        case TxAlterSubDomain:
        case TxUpgradeSubDomain:
        case TxUpgradeSubDomainDecision:
        case TxAlterExtSubDomain:
        case TxAlterExtSubDomainCreateHive:
        case TxAlterUserAttributes:
        case TxAlterTableIndex:
        case TxAlterSolomonVolume:
        case TxAlterCdcStream:
        case TxAlterCdcStreamAtTable:
        case TxAlterCdcStreamAtTableDropSnapshot:
        case TxAlterSequence:
        case TxAlterReplication:
        case TxAlterTransfer:
        case TxAlterBlobDepot:
        case TxAlterExternalTable:
        case TxAlterExternalDataSource:
        case TxAlterView:
        case TxAlterContinuousBackup:
        case TxAlterResourcePool:
        case TxAlterBackupCollection:
        case TxChangePathState:
        case TxAlterSecret:
        case TxAlterStreamingQuery:
            return false; // IsDrop
        case TxRotateCdcStream:
        case TxRotateCdcStreamAtTable:
            return false; // IsDrop
        case TxMoveTable:
        case TxMoveTableIndex:
        case TxMoveSequence:
            return false; // IsDrop
        case TxTruncateTable:
            return false; // IsDrop
        case TxInvalid:
        case TxAllocatePQ:
            Y_DEBUG_ABORT("UNREACHABLE");
            Y_UNREACHABLE();

        //NOTE: intentionally no default: case
    }
}

bool IsAlter(ETxType t) {
    switch (t) {
        case TxAlterTable:
        case TxAlterPQGroup:
        case TxAlterSubDomain:
        case TxAlterExtSubDomain:
        case TxAlterExtSubDomainCreateHive:
        case TxAlterBlockStoreVolume:
        case TxAssignBlockStoreVolume:
        case TxAlterFileStore:
        case TxAlterKesus:
        case TxAlterSolomonVolume:
        case TxAlterUserAttributes:
        case TxAlterTableIndex:
        case TxAlterCdcStream:
        case TxAlterCdcStreamAtTable:
        case TxAlterCdcStreamAtTableDropSnapshot:
        case TxAlterSequence:
        case TxAlterReplication:
        case TxAlterTransfer:
        case TxAlterBlobDepot:
        case TxAlterExternalTable:
        case TxAlterExternalDataSource:
        case TxAlterView:
        case TxAlterOlapStore:
        case TxAlterColumnTable:
        case TxAlterContinuousBackup:
        case TxAlterResourcePool:
        case TxAlterBackupCollection:
        case TxModifyACL:
        case TxChangePathState:
        case TxAlterSecret:
        case TxAlterStreamingQuery:
            return true;
        default:
            return false;
    }
}

bool CanDeleteParts(ETxType t) {
    switch (t) {
        case TxDropTable:
        case TxDropOlapStore:
        case TxDropColumnTable:
        case TxDropPQGroup:
        case TxDropSubDomain:
        case TxDropBlockStoreVolume:
        case TxDropFileStore:
        case TxDropKesus:
        case TxForceDropSubDomain:
        case TxForceDropExtSubDomain:
        case TxDropSolomonVolume:
        case TxSplitTablePartition:
        case TxMergeTablePartition:
        case TxDropCdcStream:
        case TxDropSequence:
        case TxDropReplication:
        case TxDropReplicationCascade:
        case TxDropTransfer:
        case TxDropTransferCascade:
        case TxDropBlobDepot:
        case TxDropContinuousBackup:
        case TxDropBackupCollection:
            return true; // CanDeleteParts
        case TxDropTableIndex:
        case TxRmDir:
        case TxFinalizeBuildIndex:
        case TxDropExternalTable:
        case TxDropExternalDataSource:
        case TxDropView:
        case TxDropResourcePool:
        case TxDropSysView:
        case TxCreateLongIncrementalRestoreOp:
        case TxCreateLongIncrementalBackupOp:
        case TxDropStreamingQuery:
            return false; // CanDeleteParts
        case TxMkDir:
        case TxCreateTable:
        case TxCreateOlapStore:
        case TxCreateColumnTable:
        case TxCopyTable:
        case TxReadOnlyCopyColumnTable:
        case TxCreatePQGroup:
        case TxCreateSubDomain:
        case TxCreateExtSubDomain:
        case TxCreateBlockStoreVolume:
        case TxCreateFileStore:
        case TxCreateKesus:
        case TxCreateSolomonVolume:
        case TxCreateRtmrVolume:
        case TxCreateTableIndex:
        case TxCreateCdcStream:
        case TxCreateCdcStreamAtTable:
        case TxCreateCdcStreamAtTableWithInitialScan:
        case TxCreateSequence:
        case TxCopySequence:
        case TxCreateReplication:
        case TxCreateTransfer:
        case TxCreateBlobDepot:
        case TxInitializeBuildIndex:
        case TxCreateLock:
        case TxDropLock:
        case TxDropTableIndexAtMainTable:
        case TxDropCdcStreamAtTable:
        case TxDropCdcStreamAtTableDropSnapshot:
        case TxUpdateMainTableOnIndexMove:
        case TxCreateExternalTable:
        case TxCreateExternalDataSource:
        case TxCreateView:
        case TxCreateContinuousBackup:
        case TxCreateResourcePool:
        case TxRestoreIncrementalBackupAtTable:
        case TxCreateBackupCollection:
        case TxCreateSysView:
        case TxCreateSecret:
        case TxDropSecret:
        case TxCreateStreamingQuery:
            return false; // CanDeleteParts
        case TxAlterPQGroup:
        case TxAlterTable:
        case TxAlterOlapStore:
        case TxAlterColumnTable:
        case TxModifyACL:
        case TxBackup:
        case TxRestore:
        case TxAlterBlockStoreVolume:
        case TxAssignBlockStoreVolume:
        case TxAlterFileStore:
        case TxAlterKesus:
        case TxAlterSubDomain:
        case TxAlterExtSubDomain:
        case TxAlterExtSubDomainCreateHive:
        case TxUpgradeSubDomain:
        case TxUpgradeSubDomainDecision:
        case TxAlterUserAttributes:
        case TxFillIndex:
        case TxAlterTableIndex:
        case TxAlterSolomonVolume:
        case TxAlterCdcStream:
        case TxAlterCdcStreamAtTable:
        case TxAlterCdcStreamAtTableDropSnapshot:
        case TxMoveTable:
        case TxMoveTableIndex:
        case TxMoveSequence:
        case TxAlterSequence:
        case TxAlterReplication:
        case TxAlterTransfer:
        case TxAlterBlobDepot:
        case TxAlterExternalTable:
        case TxAlterExternalDataSource:
        case TxAlterView:
        case TxAlterContinuousBackup:
        case TxAlterResourcePool:
        case TxAlterBackupCollection:
        case TxChangePathState:
        case TxRotateCdcStream:
        case TxRotateCdcStreamAtTable:
        case TxAlterSecret:
        case TxAlterStreamingQuery:
        case TxIncrementalRestoreFinalize:
        case TxTruncateTable:
            return false; // CanDeleteParts
        case TxInvalid:
        case TxAllocatePQ:
            Y_DEBUG_ABORT("UNREACHABLE");
            Y_UNREACHABLE();

        //NOTE: intentionally no default: case
    }
}

ETxType ConvertToTxType(NKikimrSchemeOp::EOperationType opType) {
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
        case NKikimrSchemeOp::ESchemeOpRotateCdcStream: return TxInvalid;
        case NKikimrSchemeOp::ESchemeOpRotateCdcStreamImpl: return TxRotateCdcStream;
        case NKikimrSchemeOp::ESchemeOpRotateCdcStreamAtTable: return TxRotateCdcStreamAtTable;
        case NKikimrSchemeOp::ESchemeOpMoveTable: return TxMoveTable;
        case NKikimrSchemeOp::ESchemeOpMoveTableIndex: return TxMoveTableIndex;
        case NKikimrSchemeOp::ESchemeOpCreateSequence: return TxCreateSequence;
        case NKikimrSchemeOp::ESchemeOpAlterSequence: return TxAlterSequence;
        case NKikimrSchemeOp::ESchemeOpDropSequence: return TxDropSequence;
        case NKikimrSchemeOp::ESchemeOpCreateReplication: return TxCreateReplication;
        case NKikimrSchemeOp::ESchemeOpAlterReplication: return TxAlterReplication;
        case NKikimrSchemeOp::ESchemeOpDropReplication: return TxDropReplication;
        case NKikimrSchemeOp::ESchemeOpDropReplicationCascade: return TxDropReplicationCascade;
        case NKikimrSchemeOp::ESchemeOpCreateTransfer: return TxCreateTransfer;
        case NKikimrSchemeOp::ESchemeOpAlterTransfer: return TxAlterTransfer;
        case NKikimrSchemeOp::ESchemeOpDropTransfer: return TxDropTransfer;
        case NKikimrSchemeOp::ESchemeOpDropTransferCascade: return TxDropTransferCascade;
        case NKikimrSchemeOp::ESchemeOpCreateBlobDepot: return TxCreateBlobDepot;
        case NKikimrSchemeOp::ESchemeOpAlterBlobDepot: return TxAlterBlobDepot;
        case NKikimrSchemeOp::ESchemeOpDropBlobDepot: return TxDropBlobDepot;
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
        case NKikimrSchemeOp::ESchemeOpCreateBackupCollection: return TxCreateBackupCollection;
        case NKikimrSchemeOp::ESchemeOpAlterBackupCollection: return TxAlterBackupCollection;
        case NKikimrSchemeOp::ESchemeOpDropBackupCollection: return TxDropBackupCollection;
        case NKikimrSchemeOp::ESchemeOpCreateSysView: return TxCreateSysView;
        case NKikimrSchemeOp::ESchemeOpDropSysView: return TxDropSysView;
        case NKikimrSchemeOp::ESchemeOpCreateLongIncrementalRestoreOp: return TxCreateLongIncrementalRestoreOp;
        case NKikimrSchemeOp::ESchemeOpChangePathState: return TxChangePathState;
        case NKikimrSchemeOp::ESchemeOpIncrementalRestoreFinalize: return TxIncrementalRestoreFinalize;
        case NKikimrSchemeOp::ESchemeOpCreateLongIncrementalBackupOp: return TxCreateLongIncrementalBackupOp;
        case NKikimrSchemeOp::ESchemeOpAlterExtSubDomainCreateHive: return TxAlterExtSubDomainCreateHive;
        case NKikimrSchemeOp::ESchemeOpDropExternalTable: return TxDropExternalTable;
        case NKikimrSchemeOp::ESchemeOpDropExternalDataSource: return TxDropExternalDataSource;
        case NKikimrSchemeOp::ESchemeOpCreateContinuousBackup: return TxCreateContinuousBackup;
        case NKikimrSchemeOp::ESchemeOpAlterContinuousBackup: return TxAlterContinuousBackup;
        case NKikimrSchemeOp::ESchemeOpDropContinuousBackup: return TxDropContinuousBackup;
        case NKikimrSchemeOp::ESchemeOpMoveIndex: return TxMoveTableIndex;
        case NKikimrSchemeOp::ESchemeOpMoveSequence: return TxMoveSequence;
        case NKikimrSchemeOp::ESchemeOpRestoreIncrementalBackupAtTable: return TxRestoreIncrementalBackupAtTable;
        case NKikimrSchemeOp::ESchemeOpCreateSecret: return TxCreateSecret;
        case NKikimrSchemeOp::ESchemeOpAlterSecret: return TxAlterSecret;
        case NKikimrSchemeOp::ESchemeOpDropSecret: return TxDropSecret;
        case NKikimrSchemeOp::ESchemeOpCreateStreamingQuery: return TxCreateStreamingQuery;
        case NKikimrSchemeOp::ESchemeOpAlterStreamingQuery: return TxAlterStreamingQuery;
        case NKikimrSchemeOp::ESchemeOpDropStreamingQuery: return TxDropStreamingQuery;
        case NKikimrSchemeOp::ESchemeOpTruncateTable: return TxTruncateTable;

        // no matching tx-type
        case NKikimrSchemeOp::ESchemeOpBackupBackupCollection:
        case NKikimrSchemeOp::ESchemeOpRestoreBackupCollection:
        case NKikimrSchemeOp::ESchemeOpBackupIncrementalBackupCollection:
        case NKikimrSchemeOp::ESchemeOpRestoreMultipleIncrementalBackups:
        case NKikimrSchemeOp::ESchemeOpCreateColumnBuild:
        case NKikimrSchemeOp::ESchemeOpDropColumnBuild:
        case NKikimrSchemeOp::ESchemeOpCreateSetConstraintInitiate: // TODO flown4qqqq
            return TxInvalid;

        //NOTE: intentionally no default: case
    }
}

}  // namespace NKikimr::NSchemeShard
