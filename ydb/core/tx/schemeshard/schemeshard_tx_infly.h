#pragma once

#include "schemeshard_types.h"

#include <ydb/core/protos/counters_schemeshard.pb.h>
#include <ydb/core/protos/tx_datashard.pb.h>

#include <ydb/core/tx/datashard/datashard.h>

#include <ydb/library/actors/core/actorid.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

namespace NKikimr {
namespace NSchemeShard {

struct TNotifications {
    TTxId TxId = InvalidTxId;
    THashSet<TActorId> Actors;

    void Add(const TActorId& actor, TTxId txId) {
        Y_ABORT_UNLESS(!TxId || TxId == txId);
        TxId = txId;
        Actors.insert(actor);
    }

    void Swap(TNotifications& notifications) {
        std::swap(TxId, notifications.TxId);
        Actors.swap(notifications.Actors);
    }

    bool Empty() const { return Actors.empty(); }
};

// Describes in-progress operation
struct TTxState {
    #define TX_STATE_DECLARE_ENUM(n, v, ...) n = v,
    #define TX_STATE_ENUM_NAME(n, ...) case n: return #n;
    #define TX_STATE_IN_FLIGHT_COUNTER(n, ...) case n: return COUNTER_IN_FLIGHT_OPS_##n ;
    #define TX_STATE_FINISHED_COUNTER(n, ...) case n: return COUNTER_FINISHED_OPS_##n ;
    #define TX_STATE_FROM_COUNTER(n, ...) case ESimpleCounters::COUNTER_IN_FLIGHT_OPS_##n: return ETxType::n;

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

    // TX_STATE_TYPE_ENUM

    enum ETxType {
        TX_STATE_TYPE_ENUM(TX_STATE_DECLARE_ENUM)
    };

    static TString TypeName(ETxType t) {
        switch (t) {
            TX_STATE_TYPE_ENUM(TX_STATE_ENUM_NAME)
        default:
            return Sprintf("Unknown tx type %d", t);
        }
    }
    static ui32 TxTypeInFlightCounter(ETxType t) {
        switch (t) {
            TX_STATE_TYPE_ENUM(TX_STATE_IN_FLIGHT_COUNTER)
        default:
            return COUNTER_IN_FLIGHT_OPS_UNKNOWN;
        }
    }
    static ui32 TxTypeFinishedCounter(ETxType t) {
        switch (t) {
            TX_STATE_TYPE_ENUM(TX_STATE_FINISHED_COUNTER)
        default:
            return COUNTER_FINISHED_OPS_UNKNOWN;
        }
    }
    static ETxType ConvertToTxType(ESimpleCounters t) {
        switch (t) {
            TX_STATE_TYPE_ENUM(TX_STATE_FROM_COUNTER)
        default:
            return TTxState::ETxType::TxInvalid;
        }
    }
    #undef TX_STATE_TYPE_ENUM


    // WARNING: DO NOT REORDER this constants
    // reordering breaks update
    #define TX_STATE_STATE_ENUM(item) \
        item(Invalid, 0, "") \
        item(Waiting, 1, "") \
        item(CreateParts, 2,        "get shards from Hive") \
        item(ConfigureParts, 3,     "send msg to shards to configure them") \
        item(DropParts, 4,          "send msg to shards that they are dropped") \
        item(DeleteParts, 5,        "send shards back to Hive (to drop their data)") \
        item(PublishTenantReadOnly, 6,      "send msg to tenant schemeshard to get it online in RO mode") \
        item(PublishGlobal, 7,      "start provide subdomains description as external domain") \
        item(RewriteOwners, 8,      "") \
        item(PublishTenant, 9,      "") \
        item(DoneMigrateTree, 10,      "") \
        item(DeleteTenantSS, 11,      "") \
        item(Propose, 128,          "propose operation to Coordinator (get global timestamp of operation)") \
        item(ProposedWaitParts, 129, "") \
        item(ProposedDeleteParts, 130, "") \
        item(TransferData, 131, "") \
        item(NotifyPartitioningChanged, 132, "") \
        item(Aborting, 133, "") \
        item(DeleteExternalShards, 134, "") \
        item(DeletePrivateShards, 135, "") \
        item(WaitShadowPathPublication, 136, "") \
        item(DeletePathBarrier, 137, "") \
        item(SyncHive, 138, "") \
        item(CopyTableBarrier, 139, "") \
        item(ProposedCopySequence, 140, "") \
        item(Done, 240, "") \
        item(Aborted, 250, "")

    enum ETxState {
        TX_STATE_STATE_ENUM(TX_STATE_DECLARE_ENUM)
    };

    static TString StateName(ETxState s) {
        switch (s) {
            TX_STATE_STATE_ENUM(TX_STATE_ENUM_NAME)
        default:
            return Sprintf("Unknown state %d", s);
        }
    }
    #undef TX_STATE_STATE_ENUM

    #undef TX_STATE_FROM_COUNTER
    #undef TX_STATE_FINISHED_COUNTER
    #undef TX_STATE_IN_FLIGHT_COUNTER
    #undef TX_STATE_ENUM_NAME
    #undef TX_STATE_DECLARE_ENUM

    struct TShardOperation {
        TShardIdx Idx;                   // shard's internal index
        TTabletTypes::EType TabletType;
        ETxState Operation;         // (first) TxState, that affects on shard

        TString RangeEnd;            // For datashard split

        TShardOperation(TShardIdx idx, TTabletTypes::EType type, ETxState op)
            : Idx(idx), TabletType(type), Operation(op)
        {}
    };

    struct TShardStatus {
        bool Success = false;
        TString Error;
        ui64 BytesProcessed = 0;
        ui64 RowsProcessed = 0;

        TShardStatus() = default;

        explicit TShardStatus(bool success, const TString& error, ui64 bytes, ui64 rows)
            : Success(success)
            , Error(error)
            , BytesProcessed(bytes)
            , RowsProcessed(rows)
        {
        }

        explicit TShardStatus(const TString& error)
            : TShardStatus(false, error, 0, 0)
        {
        }
    };

    // persist - TxInFlight:
    ETxType TxType = TxInvalid;
    TPathId TargetPathId = InvalidPathId;           // path (dir or table) being modified
    TPathId SourcePathId = InvalidPathId;           // path (dir or table) being modified
    ETxState State = Invalid;
    TStepId MinStep = InvalidStepId;
    TStepId PlanStep = InvalidStepId;

    // persist - TxShards:
    TVector<TShardOperation> Shards; // shards + operations on them
    bool NeedUpdateObject = false;
    bool NeedSyncHive = false;
    // not persist:
    THashSet<TShardIdx> ShardsInProgress; // indexes of datashards or pqs that operation waits for
    THashMap<TShardIdx, std::pair<TActorId, ui32>> SchemeChangeNotificationReceived;
    bool ReadyForNotifications = false;
    std::shared_ptr<NKikimrTxDataShard::TSplitMergeDescription> SplitDescription;
    bool TxShardsListFinalized = false;
    TTxId BuildIndexId;
    std::shared_ptr<NKikimrSchemeOp::TBuildIndexOutcome> BuildIndexOutcome;
    // fields below used for backup/restore
    bool Cancel = false;
    THashMap<TShardIdx, TShardStatus> ShardStatuses;
    ui64 DataTotalSize = 0;


    TMessageSeqNo SchemeOpSeqNo;       // For SS -> DS propose events

//    TNotifications Notify; // volatile set of actors that requested completion notification
    TInstant StartTime = TInstant::Zero();

    TTxState()
        : StartTime(::Now())
    {}

    TTxState(ETxType txType, TPathId targetPath, TPathId sourcePath = InvalidPathId)
        : TxType(txType)
        , TargetPathId(targetPath)
        , SourcePathId(sourcePath)
        , State(Waiting)
        , StartTime(::Now())
    {}

    void AcceptPendingSchemeNotification() {
        ReadyForNotifications = true;
        for (const auto& shard : SchemeChangeNotificationReceived) {
            ShardsInProgress.erase(shard.first);
        }
    }

    void ClearShardsInProgress() {
        ShardsInProgress.clear();
    }

    void UpdateShardsInProgress(ETxState operation = Invalid) {
        for (auto shard : Shards) {
            if (!operation || operation == shard.Operation) {
                ShardsInProgress.insert(shard.Idx);
            }
        }
    }

    bool IsItActuallyMerge() {
        return TxType == TTxState::TxSplitTablePartition
            && SplitDescription
            && SplitDescription->SourceRangesSize() > 1;
    }

    bool IsCreate() const {
        switch (TxType) {
        case TxMkDir:
        case TxCreateTable:
        case TxCopyTable:
        case TxCreateOlapStore:
        case TxCreateColumnTable:
        case TxCreatePQGroup:
        case TxAllocatePQ:
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
        case TxCreateBlobDepot:
        case TxCreateExternalTable:
        case TxCreateExternalDataSource:
        case TxCreateView:
        case TxCopySequence:
        case TxCreateContinuousBackup:
            return true;
        case TxInitializeBuildIndex: //this is more like alter
        case TxCreateCdcStreamAtTable:
        case TxCreateCdcStreamAtTableWithInitialScan:
            return false;
        case TxCreateLock: //this is more like alter
        case TxDropLock: //this is more like alter
            return false;
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
        case TxDropBlobDepot:
        case TxUpdateMainTableOnIndexMove:
        case TxDropExternalTable:
        case TxDropExternalDataSource:
        case TxDropView:
        case TxDropContinuousBackup:
            return false;
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
        case TxAlterBlobDepot:
        case TxAlterExternalTable:
        case TxAlterExternalDataSource:
        case TxAlterView:
        case TxAlterContinuousBackup:
            return false;
        case TxMoveTable:
        case TxMoveTableIndex:
            return true;
        case TxInvalid:
            Y_DEBUG_ABORT_UNLESS("UNREACHABLE");
            Y_UNREACHABLE();
        }
    }

    bool IsDrop() const {
        switch (TxType) {
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
        case TxDropBlobDepot:
        case TxDropExternalTable:
        case TxDropExternalDataSource:
        case TxDropView:
        case TxDropContinuousBackup:
            return true;
        case TxMkDir:
        case TxCreateTable:
        case TxCopyTable:
        case TxCreateOlapStore:
        case TxCreateColumnTable:
        case TxCreatePQGroup:
        case TxAllocatePQ:
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
            return false;
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
        case TxAlterBlobDepot:
        case TxAlterExternalTable:
        case TxAlterExternalDataSource:
        case TxAlterView:
        case TxAlterContinuousBackup:
            return false;
        case TxMoveTable:
        case TxMoveTableIndex:
            return false;
        case TxInvalid:
            Y_DEBUG_ABORT_UNLESS("UNREACHABLE");
            Y_UNREACHABLE();
        }
    }

    bool CanDeleteParts() const {
        switch (TxType) {
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
        case TxDropBlobDepot:
        case TxDropContinuousBackup:
            return true;
        case TxDropTableIndex:
        case TxRmDir:
        case TxFinalizeBuildIndex:
        case TxDropExternalTable:
        case TxDropExternalDataSource:
        case TxDropView:
            return false;
        case TxMkDir:
        case TxCreateTable:
        case TxCreateOlapStore:
        case TxCreateColumnTable:
        case TxCopyTable:
        case TxCreatePQGroup:
        case TxAllocatePQ:
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
            return false;
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
        case TxAlterSequence:
        case TxAlterReplication:
        case TxAlterBlobDepot:
        case TxAlterExternalTable:
        case TxAlterExternalDataSource:
        case TxAlterView:
        case TxAlterContinuousBackup:
            return false;
        case TxInvalid:
            Y_DEBUG_ABORT_UNLESS("UNREACHABLE");
            Y_UNREACHABLE();
        }
    }

    static ETxType ConvertToTxType(NKikimrSchemeOp::EOperationType opType) {
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
            case NKikimrSchemeOp::ESchemeOpAllocatePersQueueGroup: return TxAllocatePQ;
            case NKikimrSchemeOp::ESchemeOpDeallocatePersQueueGroup: return TxInvalid;
            case NKikimrSchemeOp::ESchemeOpCreateExternalTable: return TxCreateExternalTable;
            case NKikimrSchemeOp::ESchemeOpAlterExternalTable: return TxAlterExternalTable;
            case NKikimrSchemeOp::ESchemeOpCreateExternalDataSource: return TxCreateExternalDataSource;
            case NKikimrSchemeOp::ESchemeOpAlterExternalDataSource: return TxAlterExternalDataSource;
            case NKikimrSchemeOp::ESchemeOpCreateView: return TxCreateView;
            case NKikimrSchemeOp::ESchemeOpAlterView: return TxAlterView;
            case NKikimrSchemeOp::ESchemeOpDropView: return TxDropView;
            default: return TxInvalid;
        }
    }
};

}
}
