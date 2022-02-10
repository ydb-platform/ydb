#include "coordinator.h"
#include "coordinator_impl.h"

#include <ydb/core/tx/tx_processing.h>
#include <ydb/core/base/tablet_pipe.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>

namespace NKikimr {
namespace NFlatTxCoordinator {

class TTxCoordinatorMediatorQueue : public TActorBootstrapped<TTxCoordinatorMediatorQueue> {
    const TActorId Owner;

    const ui64 Coordinator;
    const ui64 Mediator;
    const ui64 CoordinatorGeneration;

    TActorId PipeClient;
    ui64 GenCookie;

    ui64 PrevStep;

    TAutoPtr<TMediatorConfirmations> Confirmations;

    void Die(const TActorContext &ctx) override {
        if (PipeClient) {
            NTabletPipe::CloseClient(ctx, PipeClient);
            PipeClient = TActorId();
        }
        return IActor::Die(ctx);
    }

    void Sync(const TActorContext &ctx) {
        LOG_DEBUG(ctx, NKikimrServices::TX_COORDINATOR_PRIVATE, "[%" PRIu64 "] to [%" PRIu64 "] sync", Coordinator, Mediator);

        if (PipeClient) {
            NTabletPipe::CloseClient(ctx, PipeClient);
            PipeClient = TActorId();
        }

        PipeClient = ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, Mediator));

        LOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR_MEDIATOR_QUEUE, "Actor# " << ctx.SelfID.ToString()
            << " tablet# " << Coordinator << " SEND EvCoordinatorSync to# " << Mediator << " Mediator");
        NTabletPipe::SendData(ctx, PipeClient, new TEvTxCoordinator::TEvCoordinatorSync(++GenCookie, Mediator, Coordinator));
        Become(&TThis::StateSync);
    }

    void Handle(TEvTxCoordinator::TEvCoordinatorSyncResult::TPtr &ev, const TActorContext &ctx) {
        TEvTxCoordinator::TEvCoordinatorSyncResult *msg = ev->Get();
        LOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR_MEDIATOR_QUEUE, "Actor# " << ctx.SelfID.ToString()
            << " tablet# " << Coordinator << " HANDLE EvCoordinatorSyncResult Status# "
            << NKikimrProto::EReplyStatus_Name(msg->Record.GetStatus()));

        if (msg->Record.GetCookie() != GenCookie)
            return;

        if (msg->Record.GetStatus() != NKikimrProto::OK)
            return Sync(ctx);

        PrevStep = 0;
        LOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR_MEDIATOR_QUEUE, "Actor# " << ctx.SelfID.ToString()
            << " tablet# " << Coordinator << " SEND EvMediatorQueueRestart to# " << Owner.ToString() << " Owner");
        ctx.Send(Owner, new TEvTxCoordinator::TEvMediatorQueueRestart(Mediator, 0, ++GenCookie));

        Become(&TThis::StateWork);
    }

    void Handle(TEvTxCoordinator::TEvMediatorQueueStep::TPtr &ev, const TActorContext &ctx) {
        TEvTxCoordinator::TEvMediatorQueueStep *msg = ev->Get();
        LOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR_MEDIATOR_QUEUE, "Actor# " << ctx.SelfID.ToString()
            << " HANDLE EvMediatorQueueStep step# " << msg->Step->Step);

        if (msg->GenCookie == GenCookie && PipeClient) {
            const NFlatTxCoordinator::TMediatorStep &step = *msg->Step;

            LOG_DEBUG(ctx, NKikimrServices::TX_COORDINATOR_PRIVATE, "[%" PRIu64 "] to [%" PRIu64 "], step [%" PRIu64 "]", Coordinator, Mediator, step.Step);

            LOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR_MEDIATOR_QUEUE, "Actor# " << ctx.SelfID.ToString()
                << " tablet# " << Coordinator << " SEND to# " << Mediator << " Mediator TEvCoordinatorStep");
            NTabletPipe::SendData(ctx, PipeClient, new TEvTxCoordinator::TEvCoordinatorStep(
                step, PrevStep, Mediator, Coordinator, CoordinatorGeneration));
            PrevStep = step.Step;
        }

        if (Confirmations) {
            LOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR_MEDIATOR_QUEUE, "Actor# " << ctx.SelfID.ToString()
                << " tablet# " << Coordinator << " SEND EvMediatorQueueConfirmations to# " << Owner.ToString()
                << " Owner");
            ctx.Send(Owner, new TEvTxCoordinator::TEvMediatorQueueConfirmations(Confirmations));
        }
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx) {
        TEvTabletPipe::TEvClientConnected *msg = ev->Get();
        LOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR_MEDIATOR_QUEUE, "Actor# " << ctx.SelfID.ToString()
            << " HANDLE EvClientConnected Status# " << NKikimrProto::EReplyStatus_Name(msg->Status));

        if (msg->ClientId != PipeClient)
            return;

        if (msg->Status != NKikimrProto::OK)
            return Sync(ctx);

        // actual work is in sync-reply handling
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr &ev, const TActorContext &ctx) {
        TEvTabletPipe::TEvClientDestroyed *msg = ev->Get();
        LOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR_MEDIATOR_QUEUE, "Actor# " << ctx.SelfID.ToString()
            << " HANDLE EvClientDestroyed");
        if (msg->ClientId != PipeClient)
            return;
        LOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR_MEDIATOR_QUEUE, "Actor# " << ctx.SelfID.ToString()
            << " tablet# " <<  Coordinator << " SEND EvMediatorQueueStop to# " << Owner.ToString()
            << " Owner Mediator# " << Mediator);
        ctx.Send(Owner, new TEvTxCoordinator::TEvMediatorQueueStop(Mediator));
        Sync(ctx);
    }

    void Handle(TEvTxProcessing::TEvPlanStepAck::TPtr &ev, const TActorContext &ctx) {
        const NKikimrTx::TEvPlanStepAck &record = ev->Get()->Record;
        LOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR_MEDIATOR_QUEUE, "Actor# " << ctx.SelfID.ToString()
            << " HANDLE EvPlanStepAck");

        if (!Confirmations)
            Confirmations.Reset(new TMediatorConfirmations(Mediator));

        const TTabletId tabletId = record.GetTabletId();
        for (const auto txid : record.GetTxId()) {
            Confirmations->Acks[txid].insert(tabletId);
        }
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TX_COORDINATOR_MEDIATORQ_ACTOR;
    }

    TTxCoordinatorMediatorQueue(const TActorId &owner, ui64 coordinator, ui64 mediator, ui64 coordinatorGeneration)
        : Owner(owner)
        , Coordinator(coordinator)
        , Mediator(mediator)
        , CoordinatorGeneration(coordinatorGeneration)
        , PipeClient()
        , GenCookie(0)
        , PrevStep(0)
    {}

    void Bootstrap(const TActorContext &ctx) {
        Sync(ctx);
    }

    STFUNC(StateSync) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxProcessing::TEvPlanStepAck, Handle);
            HFunc(TEvTxCoordinator::TEvCoordinatorSyncResult, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            CFunc(TEvents::TSystem::PoisonPill, Die);
        }
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxProcessing::TEvPlanStepAck, Handle);
            HFunc(TEvTxCoordinator::TEvMediatorQueueStep, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            CFunc(TEvents::TSystem::PoisonPill, Die)
        }
    }
};

IActor* CreateTxCoordinatorMediatorQueue(const TActorId &owner, ui64 coordinator, ui64 mediator, ui64 coordinatorGeneration) {
    return new NFlatTxCoordinator::TTxCoordinatorMediatorQueue(owner, coordinator, mediator, coordinatorGeneration);
}

}
}
