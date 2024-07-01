#pragma once

#include "schemeshard__operation_part.h"
#include "schemeshard_tx_infly.h"

#include <util/generic/set.h>

namespace NKikimr::NSchemeShard {

struct TOperation: TSimpleRefCount<TOperation> {
    using TPtr = TIntrusivePtr<TOperation>;

    const TTxId TxId;
    TVector<ISubOperation::TPtr> Parts;

    THashSet<TActorId> Subscribers;
    THashSet<TTxId> DependentOperations;
    THashSet<TTxId> WaitOperations;

    struct TPreSerializedMessage {
        ui32 Type;
        TIntrusivePtr<TEventSerializedData> Data;
        TOperationId OpId;

        TPreSerializedMessage() = default;

        TPreSerializedMessage(ui32 type, TIntrusivePtr<TEventSerializedData> data, TOperationId opId)
            : Type(type)
            , Data(std::move(data))
            , OpId(opId)
        { }
    };

    THashMap<TTabletId, TMap<TPipeMessageId, TPreSerializedMessage>> PipeBindedMessages; // std::pair<ui64, ui64> it's a cookie

    THashMap<TTabletId, TSubTxId> RelationsByTabletId;
    THashMap<TShardIdx, TSubTxId> RelationsByShardIdx;

    using TProposeRec = std::tuple<TSubTxId, TPathId, TStepId>;
    TDeque<TProposeRec> Proposes;

    using TProposeShards = std::tuple<TSubTxId, TTabletId>;
    TDeque<TProposeShards> ShardsProposes;

    using TPublishPath = std::pair<TPathId, ui64>;
    TSet<TPublishPath> Publications;

    TSet<TSubTxId> ReadyToProposeParts;
    THashSet<TSubTxId> ReadyToNotifyParts;
    THashSet<TSubTxId> DoneParts;
    THashMap<TPathId, NKikimrSchemeOp::EPathState> ReleasePathAtDone;

    THashMap<TShardIdx, THashSet<TSubTxId>> WaitingShardCreatedByShard;
    THashMap<TSubTxId, THashSet<TShardIdx>> WaitingShardCreatedByPart;

    TMap<TSubTxId, TSet<TPublishPath>> WaitingPublicationsByPart;
    TMap<TPublishPath, TSet<TSubTxId>> WaitingPublicationsByPath;

    TMap<TString, TSet<TSubTxId>> Barriers;

    struct TConsumeQuotaResult {
        NKikimrScheme::EStatus Status = NKikimrScheme::StatusSuccess;
        TString Reason;
    };

    struct TSplitTransactionsResult {
        NKikimrScheme::EStatus Status = NKikimrScheme::StatusSuccess;
        TString Reason;
        TVector<TTxTransaction> Transactions;
    };

    TOperation(TTxId txId)
        : TxId(txId)
    {}
    ~TOperation() = default;

    TTxId GetTxId() const { return TxId; }

    static TConsumeQuotaResult ConsumeQuota(const TTxTransaction& tx, TOperationContext& context);
    static TSplitTransactionsResult SplitIntoTransactions(const TTxTransaction& tx, const TOperationContext& context);

    ISubOperation::TPtr RestorePart(TTxState::ETxType opType, TTxState::ETxState opState) const;
    TVector<ISubOperation::TPtr> ConstructParts(const TTxTransaction& tx, TOperationContext& context) const;
    void AddPart(ISubOperation::TPtr part);

    bool AddPublishingPath(TPathId pathId, ui64 version);
    bool IsPublished() const;

    void ReadyToNotifyPart(TSubTxId partId);
    bool IsReadyToNotify(const TActorContext& ctx) const;
    bool IsReadyToNotify() const;
    void AddNotifySubscriber(const TActorId& actorId);
    void DoNotify(TSchemeShard* ss, TSideEffects& sideEffects, const TActorContext& ctx);

    bool IsReadyToDone(const TActorContext& ctx) const;

    // propose operation to coordinator
    bool IsReadyToPropose(const TActorContext& ctx) const;
    bool IsReadyToPropose() const;
    void ProposePart(TSubTxId partId, TPathId pathId, TStepId minStep);
    void ProposePart(TSubTxId partId, TTabletId tableId);
    void DoPropose(TSchemeShard* ss, TSideEffects& sideEffects, const TActorContext& ctx) const;

    // route incoming messages to suboperations (parts)
    void RegisterRelationByTabletId(TSubTxId partId, TTabletId tablet, const TActorContext& ctx);
    void RegisterRelationByShardIdx(TSubTxId partId, TShardIdx shardIdx, const TActorContext& ctx);
    TSubTxId FindRelatedPartByTabletId(TTabletId tablet, const TActorContext& ctx) const;
    TSubTxId FindRelatedPartByShardIdx(TShardIdx shardIdx, const TActorContext& ctx) const;

    void WaitShardCreated(TShardIdx shardIdx, TSubTxId partId);
    TVector<TSubTxId> ActivateShardCreated(TShardIdx shardIdx);

    void RegisterWaitPublication(TSubTxId partId, TPathId pathId, ui64 pathVersion);
    TSet<TOperationId> ActivatePartsWaitPublication(TPathId pathId, ui64 pathVersion);
    ui64 CountWaitPublication(TOperationId opId) const;

    void RegisterBarrier(TSubTxId partId, const TString& name) {
        Barriers[name].insert(partId);
        Y_ABORT_UNLESS(Barriers.size() == 1);
    }

    bool HasBarrier() const {
        Y_ABORT_UNLESS(Barriers.size() <= 1);
        return Barriers.size() == 1;
    }

    bool IsDoneBarrier() const {
        Y_ABORT_UNLESS(Barriers.size() <= 1);

        for (const auto& [_, subTxIds] : Barriers) {
            for (const auto blocked : subTxIds) {
                Y_VERIFY_S(!DoneParts.contains(blocked), "part is blocked and done: " << blocked);
            }
            return subTxIds.size() + DoneParts.size() == Parts.size();
        }

        return false;
    }

    void DropBarrier(const TString& name) {
        Y_ABORT_UNLESS(IsDoneBarrier());
        Y_ABORT_UNLESS(Barriers.begin()->first == name);
        Barriers.erase(name);
    }

    TOperationId NextPartId() const {
        return TOperationId(TxId, TSubTxId(Parts.size()));
    }
};

inline TOperationId NextPartId(const TOperationId& opId, const TVector<ISubOperation::TPtr>& parts) {
    return TOperationId(opId.GetTxId(), opId.GetSubTxId() + parts.size());
}

}
