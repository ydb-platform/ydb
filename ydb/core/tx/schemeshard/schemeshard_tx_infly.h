#pragma once

#include "schemeshard_types.h"

#include <ydb/core/protos/counters_schemeshard.pb.h>
#include <ydb/core/protos/tx_datashard.pb.h>

#include <ydb/core/tx/datashard/datashard.h>

#include <library/cpp/actors/core/actorid.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

namespace NKikimr {
namespace NSchemeShard {

struct TNotifyes {
    TTxId TxId = InvalidTxId;
    THashSet<TActorId> Actors;

    void Add(const TActorId& actor, TTxId txId) {
        Y_VERIFY(!TxId || TxId == txId);
        TxId = txId;
        Actors.insert(actor);
    }

    void Swap(TNotifyes& notifyes) {
        std::swap(TxId, notifyes.TxId);
        Actors.swap(notifyes.Actors);
    }

    bool Empty() const { return Actors.empty(); }
};

// Describes in-progress operation
struct TTxState {
    #define TX_STATE_DECLARE_ENUM(n, v, ...) n = v,
    #define TX_STATE_ENUM_NAME(n, ...) case n: return #n;
    #define TX_STATE_IN_FLIGHT_COUNTER(n, ...) case n: return COUNTER_IN_FLIGHT_OPS_##n ;
    #define TX_STATE_FINISHED_COUNTER(n, ...) case n: return COUNTER_FINISHED_OPS_##n ;

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
        item(TxCreateLockForIndexBuild, 38) \
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
        item(TxCreateOlapTable, 51) \
        item(TxAlterOlapTable, 52) \
        item(TxDropOlapTable, 53) \
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
        item(TxDropReplication, 67) \
        item(TxCreateBlobDepot, 68) \
        item(TxAlterBlobDepot, 69) \
        item(TxDropBlobDepot, 70) \
        item(TxUpdateMainTableOnIndexMove, 71)

    // TX_STATE_TYPE_ENUM

    //TxMergeTablePartition only for sensors yet

    enum ETxType {
        TX_STATE_TYPE_ENUM(TX_STATE_DECLARE_ENUM)
    };

    static TString TypeName(ETxType t) {
        switch(t) {
            TX_STATE_TYPE_ENUM(TX_STATE_ENUM_NAME)
        default:
            return Sprintf("Unknown tx type %d", t);
        }
    }
    static ui32 TxTypeInFlightCounter(ETxType t) {
        switch(t) {
            TX_STATE_TYPE_ENUM(TX_STATE_IN_FLIGHT_COUNTER)
        default:
            return COUNTER_IN_FLIGHT_OPS_UNKNOWN;
        }
    }
    static ui32 TxTypeFinishedCounter(ETxType t) {
        switch(t) {
            TX_STATE_TYPE_ENUM(TX_STATE_FINISHED_COUNTER)
        default:
            return COUNTER_FINISHED_OPS_UNKNOWN;
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
        item(Done, 240, "") \
        item(Aborted, 250, "")

    enum ETxState {
        TX_STATE_STATE_ENUM(TX_STATE_DECLARE_ENUM)
    };

    static TString StateName(ETxState s) {
        switch(s) {
            TX_STATE_STATE_ENUM(TX_STATE_ENUM_NAME)
        default:
            return Sprintf("Unknown state %d", s);
        }
    }
    #undef TX_STATE_STATE_ENUM

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

//    TNotifyes Notify; // volatile set of actors that requested completion notification
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
        case TxCreateOlapTable:
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
        case TxCreateBlobDepot:
            return true;
        case TxInitializeBuildIndex: //this is more like alter
        case TxCreateCdcStreamAtTable:
            return false;
        case TxCreateLockForIndexBuild: //this is more like alter
        case TxDropLock: //this is more like alter
            return false;
        case TxDropTable:
        case TxDropOlapStore:
        case TxDropOlapTable:
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
        case TxDropSequence:
        case TxDropReplication:
        case TxDropBlobDepot:
        case TxUpdateMainTableOnIndexMove:
            return false;
        case TxAlterPQGroup:
        case TxAlterTable:
        case TxAlterOlapStore:
        case TxAlterOlapTable:
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
        case TxAlterUserAttributes:
        case TxAlterTableIndex:
        case TxAlterSolomonVolume:
        case TxAlterCdcStream:
        case TxAlterCdcStreamAtTable:
        case TxAlterSequence:
        case TxAlterReplication:
        case TxAlterBlobDepot:
            return false;
        case TxMoveTable:
        case TxMoveTableIndex:
            return true;
        case TxInvalid:
            Y_VERIFY_DEBUG("UNREACHEBLE");
            Y_UNREACHABLE();
        }
    }

    bool IsDrop() const {
        switch (TxType) {
        case TxDropTable:
        case TxDropOlapStore:
        case TxDropOlapTable:
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
        case TxDropBlobDepot:
            return true;
        case TxMkDir:
        case TxCreateTable:
        case TxCopyTable:
        case TxCreateOlapStore:
        case TxCreateOlapTable:
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
        case TxCreateSequence:
        case TxCreateReplication:
        case TxCreateBlobDepot:
        case TxInitializeBuildIndex:
        case TxCreateLockForIndexBuild:
        case TxDropLock:
        case TxFinalizeBuildIndex:
        case TxDropTableIndexAtMainTable: // just increments schemaversion at main table
        case TxDropCdcStreamAtTable:
        case TxUpdateMainTableOnIndexMove:
            return false;
        case TxAlterPQGroup:
        case TxAlterTable:
        case TxAlterOlapStore:
        case TxAlterOlapTable:
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
        case TxAlterUserAttributes:
        case TxAlterTableIndex:
        case TxAlterSolomonVolume:
        case TxAlterCdcStream:
        case TxAlterCdcStreamAtTable:
        case TxAlterSequence:
        case TxAlterReplication:
        case TxAlterBlobDepot:
            return false;
        case TxMoveTable:
        case TxMoveTableIndex:
            return false;
        case TxInvalid:
            Y_VERIFY_DEBUG("UNREACHEBLE");
            Y_UNREACHABLE();
        }
    }

    bool CanDeleteParts() const {
        switch (TxType) {
        case TxDropTable:
        case TxDropOlapStore:
        case TxDropOlapTable:
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
        case TxDropBlobDepot:
            return true;
        case TxDropTableIndex:
        case TxRmDir:
        case TxFinalizeBuildIndex:
            return false;
        case TxMkDir:
        case TxCreateTable:
        case TxCreateOlapStore:
        case TxCreateOlapTable:
        case TxCopyTable:
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
        case TxCreateSequence:
        case TxCreateReplication:
        case TxCreateBlobDepot:
        case TxInitializeBuildIndex:
        case TxCreateLockForIndexBuild:
        case TxDropLock:
        case TxDropTableIndexAtMainTable:
        case TxDropCdcStreamAtTable:
        case TxUpdateMainTableOnIndexMove:
            return false;
        case TxAlterPQGroup:
        case TxAlterTable:
        case TxAlterOlapStore:
        case TxAlterOlapTable:
        case TxModifyACL:
        case TxBackup:
        case TxRestore:
        case TxAlterBlockStoreVolume:
        case TxAssignBlockStoreVolume:
        case TxAlterFileStore:
        case TxAlterKesus:
        case TxAlterSubDomain:
        case TxAlterExtSubDomain:
        case TxUpgradeSubDomain:
        case TxUpgradeSubDomainDecision:
        case TxAlterUserAttributes:
        case TxFillIndex:
        case TxAlterTableIndex:
        case TxAlterSolomonVolume:
        case TxAlterCdcStream:
        case TxAlterCdcStreamAtTable:
        case TxMoveTable:
        case TxMoveTableIndex:
        case TxAlterSequence:
        case TxAlterReplication:
        case TxAlterBlobDepot:
            return false;
        case TxInvalid:
            Y_VERIFY_DEBUG("UNREACHEBLE");
            Y_UNREACHABLE();
        }
    }
};

}
}
