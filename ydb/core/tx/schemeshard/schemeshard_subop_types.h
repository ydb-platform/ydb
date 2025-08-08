#pragma once

#include <util/generic/string.h>


namespace NKikimrSchemeOp {
    enum EOperationType : int;
}

namespace NKikimr::NSchemeShard {

enum ESimpleCounters : int;

// WARNING: DO NOT REORDER this constants
// reordering breaks update
#define TX_STATE_TYPE_ENUM(item) \
    item(TxInvalid, 0) \
    item(TxMkDir, 1) \
    item(TxCreateTable, 2) \
    item(TxCreatePQGroup, 3) \
    item(TxAlterPQGroup, 4) \
    item(TxAlterTable, 5) \
    item(TxDropTable, 6) \
    item(TxDropPQGroup, 7) \
    item(TxModifyACL, 8) \
    item(TxRmDir, 9) \
    item(TxCopyTable, 10) \
    item(TxSplitTablePartition, 11) \
    item(TxBackup, 12) \
    item(TxCreateSubDomain, 13) \
    item(TxDropSubDomain, 14) \
    item(TxCreateRtmrVolume, 15) \
    item(TxCreateBlockStoreVolume, 16) \
    item(TxAlterBlockStoreVolume, 17) \
    item(TxAssignBlockStoreVolume, 18) \
    item(TxDropBlockStoreVolume, 19) \
    item(TxCreateKesus, 20) \
    item(TxDropKesus, 21) \
    item(TxForceDropSubDomain, 22) \
    item(TxCreateSolomonVolume, 23) \
    item(TxDropSolomonVolume, 24) \
    item(TxAlterKesus, 25) \
    item(TxAlterSubDomain, 26) \
    item(TxAlterUserAttributes, 27) \
    item(TxCreateTableIndex, 28) \
    item(TxDropTableIndex, 29) \
    item(TxCreateExtSubDomain, 30) \
    item(TxMergeTablePartition, 31) \
    item(TxAlterExtSubDomain, 32) \
    item(TxForceDropExtSubDomain, 33) \
    item(TxFillIndex, 34) \
    item(TxUpgradeSubDomain, 35) \
    item(TxUpgradeSubDomainDecision, 36) \
    item(TxInitializeBuildIndex, 37) \
    item(TxCreateLock, 38) \
    item(TxAlterTableIndex, 39) \
    item(TxFinalizeBuildIndex, 40) \
    item(TxAlterSolomonVolume, 41) \
    item(TxDropLock, 42) \
    item(TxDropTableIndexAtMainTable, 43) \
    item(TxCreateFileStore, 44) \
    item(TxAlterFileStore, 45) \
    item(TxDropFileStore, 46) \
    item(TxRestore, 47) \
    item(TxCreateOlapStore, 48) \
    item(TxAlterOlapStore, 49) \
    item(TxDropOlapStore, 50) \
    item(TxCreateColumnTable, 51) \
    item(TxAlterColumnTable, 52) \
    item(TxDropColumnTable, 53) \
    item(TxCreateCdcStream, 54) \
    item(TxCreateCdcStreamAtTable, 55) \
    item(TxAlterCdcStream, 56) \
    item(TxAlterCdcStreamAtTable, 57) \
    item(TxDropCdcStream, 58) \
    item(TxDropCdcStreamAtTable, 59) \
    item(TxMoveTable, 60) \
    item(TxMoveTableIndex, 61) \
    item(TxCreateSequence, 62) \
    item(TxAlterSequence, 63) \
    item(TxDropSequence, 64) \
    item(TxCreateReplication, 65) \
    item(TxAlterReplication, 66) \
    item(TxDropReplicationCascade, 67) \
    item(TxCreateBlobDepot, 68) \
    item(TxAlterBlobDepot, 69) \
    item(TxDropBlobDepot, 70) \
    item(TxUpdateMainTableOnIndexMove, 71) \
    item(TxAllocatePQ, 72) \
    item(TxCreateCdcStreamAtTableWithInitialScan, 73) \
    item(TxAlterExtSubDomainCreateHive, 74) \
    item(TxAlterCdcStreamAtTableDropSnapshot, 75) \
    item(TxDropCdcStreamAtTableDropSnapshot, 76) \
    item(TxCreateExternalTable, 77) \
    item(TxDropExternalTable, 78) \
    item(TxAlterExternalTable, 79) \
    item(TxCreateExternalDataSource, 80) \
    item(TxDropExternalDataSource, 81) \
    item(TxAlterExternalDataSource, 82) \
    item(TxCreateView, 83) \
    item(TxAlterView, 84) \
    item(TxDropView, 85) \
    item(TxCopySequence, 86) \
    item(TxDropReplication, 87) \
    item(TxCreateContinuousBackup, 88) \
    item(TxAlterContinuousBackup, 89) \
    item(TxDropContinuousBackup, 90) \
    item(TxCreateResourcePool, 91) \
    item(TxDropResourcePool, 92) \
    item(TxAlterResourcePool, 93) \
    item(TxRestoreIncrementalBackupAtTable, 94) \
    item(TxCreateBackupCollection, 95) \
    item(TxDropBackupCollection, 96) \
    item(TxAlterBackupCollection, 97) \
    item(TxMoveSequence, 98) \
    item(TxCreateTransfer, 99) \
    item(TxAlterTransfer, 100) \
    item(TxDropTransfer, 101) \
    item(TxDropTransferCascade, 102) \
    item(TxCreateSysView, 103) \
    item(TxDropSysView, 104) \
    item(TxCreateLongIncrementalRestoreOp, 105) \
    item(TxChangePathState, 106) \
    item(TxRotateCdcStream, 107) \
    item(TxRotateCdcStreamAtTable, 108) \
    item(TxIncrementalRestoreFinalize, 109) \
    item(TxCreateLongIncrementalBackupOp, 110) \

// TX_STATE_TYPE_ENUM

#define TX_STATE_DECLARE_ENUM(n, v, ...) n = v,
enum ETxType {
    TX_STATE_TYPE_ENUM(TX_STATE_DECLARE_ENUM)
};
#undef TX_STATE_DECLARE_ENUM

TString TxTypeName(ETxType t);
ui32 TxTypeInFlightCounter(ETxType t);
ui32 TxTypeFinishedCounter(ETxType t);
ETxType ConvertToTxType(ESimpleCounters t);
ETxType ConvertToTxType(NKikimrSchemeOp::EOperationType opType);

bool IsCreate(ETxType t);
bool IsDrop(ETxType t);
bool CanDeleteParts(ETxType t);

}  // namespace NKikimr::NSchemeShard
