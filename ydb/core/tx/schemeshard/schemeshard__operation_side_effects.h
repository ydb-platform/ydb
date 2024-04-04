#pragma once

#include "schemeshard_identificators.h"
#include "schemeshard_types.h"

#include <ydb/core/tablet/pipe_tracker.h>
#include <ydb/core/tablet_flat/tablet_flat_executor.h>

#include <util/generic/ptr.h>
#include <util/generic/map.h>

namespace NKikimr {
namespace NSchemeShard {

class TSchemeShard;

class TSideEffects: public TSimpleRefCount<TSideEffects> {
public:
    using TPublications = THashMap<TTxId, TDeque<TPathId>>;

private:
    using TCoordinatorAck = std::tuple<TActorId, TStepId, TTxId>;
    using TMediatorAck = std::tuple<TActorId, TStepId>;
    using TSendRec = std::tuple<TActorId, TAutoPtr<::NActors::IEventBase>, ui64, ui32>;
    using TBindMsgRec = std::tuple<TOperationId, TTabletId, TPipeMessageId, TAutoPtr<::NActors::IEventBase>>;
    using TBindMsgAck = std::tuple<TOperationId, TTabletId, TPipeMessageId>;
    using TNotifyRec = std::tuple<TActorId, TTxId>;
    using TProposeRec = std::tuple<TOperationId, TPathId, TStepId>;
    using TProposeShards = std::tuple<TOperationId, TTabletId>;
    using TRelationByTabletId = std::tuple<TOperationId, TTabletId>;
    using TRelationByShardIdx = std::tuple<TOperationId, TShardIdx>;
    using TDependence = std::tuple<TTxId, TTxId>;
    using TPathStateRec = std::tuple<TOperationId, TPathId, NKikimrSchemeOp::EPathState>;
    using TWaitShardCreated = std::tuple<TShardIdx, TOperationId>;
    using TActivateShardCreated = std::tuple<TShardIdx, TTxId>;
    using TWaitPublication = std::tuple<TOperationId, TPathId>;
    using TBarrierRec = std::tuple<TOperationId, TString>;

    THashSet<TTxId> ActivationOps;
    THashSet<TOperationId> ActivationParts;

    TDeque<TCoordinatorAck> CoordinatorAcks;
    TDeque<TMediatorAck> MediatorAcks;
    TDeque<TSendRec> Messages;
    TDeque<TBindMsgRec> BindedMessages;
    TDeque<TBindMsgAck> BindedMessageAcks;
    TPublications PublishPaths;
    TPublications RePublishPaths; // only for UpgradeSubDomain
    TDeque<TProposeRec> CoordinatorProposes;
    TDeque<TProposeShards> CoordinatorProposesShards;
    TPendingPipeTrackerCommands PendingPipeTrackerCommands;
    TDeque<TOperationId> RelationsByTabletsFromOperation;
    TDeque<TRelationByTabletId> RelationsByTabletId;
    TDeque<TRelationByShardIdx> RelationsByShardIdx;
    THashSet<TOperationId> ReadyToNotifyOperations;
    THashSet<TOperationId> DoneOperations;
    THashSet<TTxId> DoneTransactions;
    THashSet<TShardIdx> ToDeleteShards;
    TDeque<TDependence> Dependencies;
    TDeque<TPathStateRec> ReleasePathStateRecs;
    THashSet<TPathId> TenantsToUpdate;
    TDeque<TIndexBuildId> IndexToProgress;
    TVector<TWaitShardCreated> PendingWaitShardCreated;
    TVector<TActivateShardCreated> PendingActivateShardCreated;
    TDeque<TWaitPublication> WaitPublications;
    TDeque<TBarrierRec> Barriers;
    THashMap<TActorId, TVector<TPathId>> TempDirsToMakeState;
    THashMap<TActorId, TVector<TPathId>> TempDirsToRemoveState;

public:
    using TPtr = TIntrusivePtr<TSideEffects>;
    ~TSideEffects() = default;

    void ProposeToCoordinator(TOperationId opId, TPathId pathId, TStepId minStep);
    template <class TContainer>
    void ProposeToCoordinator(TOperationId opId, TPathId pathId, TStepId minStep, TContainer txShards) {
        ProposeToCoordinator(opId, pathId, minStep);

        for(auto& shard: txShards) {
            CoordinatorProposesShards.push_back(TProposeShards(opId, shard));
        }
    }
    void CoordinatorAck(TActorId coordinator, TStepId stepId, TTxId txId);
    void MediatorAck(TActorId mediator, TStepId stepId);

    void UpdateTenant(TPathId pathIds);
    void UpdateTenants(THashSet<TPathId>&& pathIds);

    void PublishToSchemeBoard(TOperationId opId, TPathId pathId);
    void RePublishToSchemeBoard(TOperationId opId, TPathId pathId);

    void Send(TActorId dst, ::NActors::IEventBase* message, ui64 cookie = 0, ui32 flags = 0);
    template <typename TEvent>
    void Send(TActorId dst, THolder<TEvent> message, ui64 cookie = 0, ui32 flags = 0) {
        Send(dst, static_cast<::NActors::IEventBase*>(message.Release()), cookie, flags);
    }
    void BindMsgToPipe(TOperationId opId, TTabletId dst, TPathId pathId, TAutoPtr<::NActors::IEventBase> message);
    void BindMsgToPipe(TOperationId opId, TTabletId dst, TShardIdx shardIdx, TAutoPtr<::NActors::IEventBase> message);
    void BindMsgToPipe(TOperationId opId, TTabletId dst, TPipeMessageId cookie, TAutoPtr<::NActors::IEventBase> message);

    void UnbindMsgFromPipe(TOperationId opId, TTabletId dst, TPathId pathId);
    void UnbindMsgFromPipe(TOperationId opId, TTabletId dst, TShardIdx shardIdx);
    void UnbindMsgFromPipe(TOperationId opId, TTabletId dst, TPipeMessageId cookie);

    void UpdateTempDirsToMakeState(const TActorId& ownerActorId, const TPathId& pathId);
    void UpdateTempDirsToRemoveState(const TActorId& ownerActorId, const TPathId& pathId);

    void RouteByTabletsFromOperation(TOperationId opId);
    void RouteByTablet(TOperationId opId, TTabletId dst);
    void RouteByShardIdx(TOperationId opId, TShardIdx shardIdx);

    void ReleasePathState(TOperationId opId, TPathId pathId, NKikimrSchemeOp::EPathState state);

    void DoneOperation(TOperationId opId);
    void ReadyToNotify(TOperationId opId);

    void Dependence(TTxId parent, TTxId child);
    void ActivateTx(TOperationId opId);
    void ActivateOperation(TTxId txId);

    void WaitShardCreated(TShardIdx idx, TOperationId opId);
    void ActivateShardCreated(TShardIdx idx, TTxId txId);

    void PublishAndWaitPublication(TOperationId opId, TPathId pathId);

    void DeleteShard(TShardIdx idx);

    void ToProgress(TIndexBuildId id);

    void ApplyOnExecute(TSchemeShard* ss, NTabletFlatExecutor::TTransactionContext &txc, const TActorContext &ctx);
    void ApplyOnComplete(TSchemeShard* ss, const TActorContext &ctx);
    TPublications ExtractPublicationsToSchemeBoard();

    void Barrier(TOperationId opId, TString barrierName);

private:
    bool CheckDecouplingProposes(TString& errExpl) const;
    void ExpandCoordinatorProposes(TSchemeShard* ss, const TActorContext& ctx);
    void DoCoordinatorAck(TSchemeShard* ss, const TActorContext& ctx);
    void DoMediatorsAck(TSchemeShard* ss, const TActorContext& ctx);

    void DoUpdateTenant(TSchemeShard* ss, NTabletFlatExecutor::TTransactionContext &txc, const TActorContext& ctx);

    void DoPersistPublishPaths(TSchemeShard* ss, NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx);
    void DoPublishToSchemeBoard(TSchemeShard* ss, const TActorContext& ctx);

    void DoSend(TSchemeShard* ss, const TActorContext& ctx);
    void DoBindMsg(TSchemeShard* ss, const TActorContext& ctx);
    void DoBindMsgAcks(TSchemeShard* ss, const TActorContext& ctx);

    void AttachOperationToPipe(TOperationId opId, TTabletId dst);
    void DetachOperationFromPipe(TOperationId opId, TTabletId dst);

    void DoRegisterRelations(TSchemeShard* ss, const TActorContext& ctx);
    void DoTriggerDeleteShards(TSchemeShard* ss, const TActorContext &ctx);

    void DoReleasePathState(TSchemeShard* ss, const TActorContext &ctx);
    void DoDoneParts(TSchemeShard* ss, const TActorContext& ctx);
    void DoDoneTransactions(TSchemeShard* ss, NTabletFlatExecutor::TTransactionContext &txc, const TActorContext& ctx);
    void DoReadyToNotify(TSchemeShard* ss, const TActorContext& ctx);

    void DoPersistDependencies(TSchemeShard* ss, NTabletFlatExecutor::TTransactionContext &txc, const TActorContext &ctx);
    void DoActivateOps(TSchemeShard* ss, const TActorContext& ctx);

    void DoPersistDeleteShards(TSchemeShard* ss, NTabletFlatExecutor::TTransactionContext &txc, const TActorContext &ctx);

    void DoUpdateTempDirsToMakeState(TSchemeShard* ss, const TActorContext &ctx);
    void DoUpdateTempDirsToRemoveState(TSchemeShard* ss, const TActorContext &ctx);

    void ResumeLongOps(TSchemeShard* ss, const TActorContext& ctx);
    void SetupRoutingLongOps(TSchemeShard* ss, const TActorContext& ctx);

    void DoWaitShardCreated(TSchemeShard* ss, const TActorContext& ctx);
    void DoActivateShardCreated(TSchemeShard* ss, const TActorContext& ctx);

    void DoWaitPublication(TSchemeShard* ss, const TActorContext& ctx);

    void DoSetBarriers(TSchemeShard* ss, const TActorContext& ctx);
    void DoCheckBarriers(TSchemeShard *ss, NTabletFlatExecutor::TTransactionContext &txc, const TActorContext &ctx);
};

}
}
