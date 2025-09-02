#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

#include <ydb/library/actors/core/actorid.h>
#include <ydb/core/base/tablet_types.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/tx/message_seqno.h>

#include "schemeshard_identificators.h"  // for TStepId, TTxId, TShardIdx
#include "schemeshard_subop_types.h"  // for ETxType
#include "schemeshard_subop_state_types.h"  // for ETxState


namespace NKikimrSchemeOp {
    enum EPathState : int;
    class TBuildIndexOutcome;
}

namespace NKikimrTxDataShard {
    class TSplitMergeDescription;
}

namespace NKikimr::NSchemeShard {

// Describes in-progress operation
struct TTxState {
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

    using ETxType = NKikimr::NSchemeShard::ETxType;
    using enum ETxType;
    using ETxState = NKikimr::NSchemeShard::ETxState;
    using enum ETxState;

    static TString TypeName(ETxType t) {
        return NKikimr::NSchemeShard::TxTypeName(t);
    }
    static TString StateName(ETxState t) {
        return NKikimr::NSchemeShard::TxStateName(t);
    }

    // persist - TxInFlight:
    ETxType TxType = TxInvalid;
    TPathId TargetPathId = InvalidPathId;           // path (dir or table) being modified
    TPathId SourcePathId = InvalidPathId;           // path (dir or table) being modified
    ETxState State = Invalid;
    TStepId MinStep = InvalidStepId;
    TStepId PlanStep = InvalidStepId;

    // TxCopy or TxRotateCdcStreamAtTable: Stores path for cdc stream to create in case of ContinuousBackup;
    // uses ExtraData through proto
    TPathId CdcPathId = InvalidPathId;
    TMaybe<NKikimrSchemeOp::EPathState> TargetPathTargetState;

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

    bool IsItActuallyMerge() const;

    bool IsCreate() const {
        return NKikimr::NSchemeShard::IsCreate(TxType);
    }

    bool IsDrop() const {
        return NKikimr::NSchemeShard::IsDrop(TxType);
    }

    bool CanDeleteParts() const {
        return NKikimr::NSchemeShard::CanDeleteParts(TxType);
    }
};

}  // namespace NKikimr::NSchemeShard
