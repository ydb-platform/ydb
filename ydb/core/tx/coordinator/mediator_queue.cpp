#include "coordinator.h"
#include "coordinator_impl.h"

#include <ydb/core/tx/tx_processing.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <library/cpp/containers/absl_flat_hash/flat_hash_map.h>

namespace NKikimr {
namespace NFlatTxCoordinator {

// Normally we flush on every confirmed step, since usually they are confirmed
// in the same order we planned them. However the first step in the queue may
// be blocked by some shard, so make sure we flush when at least 2 other steps
// have been confirmed.
static constexpr size_t ConfirmedStepsToFlush = 2;

// Coordinator may need to persist confirmed participants, and we need to limit
// the number of rows as large transactions are problematic to commit.
static constexpr size_t ConfirmedParticipantsToFlush = 10'000;

void TMediatorStep::SerializeTo(TEvTxCoordinator::TEvCoordinatorStep *msg) const {
    for (const TTx &tx : Transactions) {
        NKikimrTx::TCoordinatorTransaction *x = msg->Record.AddTransactions();
        if (tx.TxId)
            x->SetTxId(tx.TxId);
        for (ui64 affected : tx.PushToAffected)
            x->AddAffectedSet(affected);
    }
}

class TTxCoordinatorMediatorQueue : public TActorBootstrapped<TTxCoordinatorMediatorQueue> {
    const TActorId Owner;

    const ui64 Coordinator;
    const ui64 Mediator;
    const ui64 CoordinatorGeneration;

    TActorId PipeClient;

    ui64 GenCookie = 0;
    ui64 PrevStep = 0;
    bool Active = false;

    TMediatorStepList Queue;
    THashMap<std::pair<ui64, ui64>, TMediatorStepList::iterator> WaitingAcks;

    std::unique_ptr<TMediatorConfirmations> Confirmations;
    size_t ConfirmedParticipants = 0;
    size_t ConfirmedSteps = 0;

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
        Active = false;
    }

    void SendStep(const TMediatorStep &step, const TActorContext &ctx) {
        LOG_DEBUG(ctx, NKikimrServices::TX_COORDINATOR_PRIVATE, "[%" PRIu64 "] to [%" PRIu64 "], step [%" PRIu64 "]", Coordinator, Mediator, step.Step);

        LOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR_MEDIATOR_QUEUE, "Actor# " << ctx.SelfID.ToString()
            << " tablet# " << Coordinator << " SEND to# " << Mediator << " Mediator TEvCoordinatorStep");
        auto msg = std::make_unique<TEvTxCoordinator::TEvCoordinatorStep>(
            step.Step, PrevStep, Mediator, Coordinator, CoordinatorGeneration);
        step.SerializeTo(msg.get());
        NTabletPipe::SendData(ctx, PipeClient, msg.release());
        PrevStep = step.Step;
    }

    void SendConfirmations(const TActorContext &ctx) {
        if (Confirmations) {
            LOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR_MEDIATOR_QUEUE, "Actor# " << ctx.SelfID.ToString()
                << " tablet# " << Coordinator << " SEND EvMediatorQueueConfirmations to# " << Owner.ToString()
                << " Owner");
            ctx.Send(Owner, new TEvMediatorQueueConfirmations(std::move(Confirmations)));
            ConfirmedParticipants = 0;
            ConfirmedSteps = 0;
        }
    }

    /**
     * Filters step by removing txs/shards that have already been acknowledged
     */
    void FilterAcked(TMediatorStep &step) {
        auto end = std::remove_if(
            step.Transactions.begin(),
            step.Transactions.end(),
            [this](TMediatorStep::TTx& tx) -> bool {
                ui64 txId = tx.TxId;
                auto end = std::remove_if(
                    tx.PushToAffected.begin(),
                    tx.PushToAffected.end(),
                    [this, txId](ui64 shardId) -> bool {
                        // Remove shards that have been acknowledged (not in a waiting set)
                        return WaitingAcks.find(std::make_pair(txId, shardId)) == WaitingAcks.end();
                    });
                if (end != tx.PushToAffected.end()) {
                    tx.PushToAffected.erase(end, tx.PushToAffected.end());
                }
                // Remove transactions without any shards left
                return tx.PushToAffected.empty();
            });
        if (end != step.Transactions.end()) {
            step.Transactions.erase(end, step.Transactions.end());
            // Note: fully acked steps are removed immediately
            if (Y_UNLIKELY(step.Transactions.empty())) {
                step.Transactions.shrink_to_fit();
            }
        }
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
        ctx.Send(Owner, new TEvMediatorQueueRestart(Mediator, 0, ++GenCookie));

        Become(&TThis::StateWork);

        Active = true;
        for (auto& step : Queue) {
            FilterAcked(step);
            // TODO: limit the number of steps inflight
            SendStep(step, ctx);
        }
    }

    void Handle(TEvMediatorQueueStep::TPtr &ev, const TActorContext &ctx) {
        TEvMediatorQueueStep *msg = ev->Get();

        while (!msg->Steps.empty()) {
            auto it = msg->Steps.begin();
            LOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR_MEDIATOR_QUEUE, "Actor# " << ctx.SelfID.ToString()
                << " HANDLE EvMediatorQueueStep step# " << it->Step);

            // Remove the last empty step (empty steps except the last one are not needed)
            if (!Queue.empty() && Queue.back().Transactions.empty()) {
                Queue.pop_back();
            }

            Queue.splice(Queue.end(), msg->Steps, it);

            // Index by TxId/ShardId pairs while waiting for acks
            Y_ABORT_UNLESS(it->References == 0);
            for (auto& tx : it->Transactions) {
                for (ui64 shardId : tx.PushToAffected) {
                    auto res = WaitingAcks.insert(
                        std::make_pair(
                            std::make_pair(tx.TxId, shardId),
                            it));
                    Y_DEBUG_ABORT_UNLESS(res.second);
                    if (res.second) {
                        it->References++;
                    }
                }
            }

            if (Active) {
                // TODO: limit the number of steps inflight
                SendStep(*it, ctx);
            }
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
        Active = false;
        LOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR_MEDIATOR_QUEUE, "Actor# " << ctx.SelfID.ToString()
            << " tablet# " <<  Coordinator << " SEND EvMediatorQueueStop to# " << Owner.ToString()
            << " Owner Mediator# " << Mediator);
        ctx.Send(Owner, new TEvMediatorQueueStop(Mediator));
        Sync(ctx);
    }

    void Handle(TEvTxProcessing::TEvPlanStepAck::TPtr &ev, const TActorContext &ctx) {
        const NKikimrTx::TEvPlanStepAck &record = ev->Get()->Record;
        LOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR_MEDIATOR_QUEUE, "Actor# " << ctx.SelfID.ToString()
            << " HANDLE EvPlanStepAck");

        bool firstConfirmed = false;

        const TTabletId tabletId = record.GetTabletId();
        for (const auto txid : record.GetTxId()) {
            auto it = WaitingAcks.find(std::make_pair(txid, tabletId));
            if (it != WaitingAcks.end()) {
                Y_ABORT_UNLESS(it->second->References > 0);

                if (!Confirmations)
                    Confirmations.reset(new TMediatorConfirmations(Mediator));
                if (Confirmations->Acks[txid].insert(tabletId).second)
                    ++ConfirmedParticipants;

                if (0 == --it->second->References) {
                    ++ConfirmedSteps;
                    Y_ABORT_UNLESS(!Queue.empty());
                    if (it->second == Queue.begin()) {
                        firstConfirmed = true;
                    }
                    auto last = --Queue.end();
                    if (it->second != last) {
                        Queue.erase(it->second);
                    } else {
                        // On reconnect we will resend it as empty
                        it->second->Transactions.clear();
                        it->second->Transactions.shrink_to_fit();
                    }
                }
                WaitingAcks.erase(it);
            }
        }

        if (firstConfirmed ||
            ConfirmedSteps >= ConfirmedStepsToFlush ||
            ConfirmedParticipants >= ConfirmedParticipantsToFlush)
        {
            SendConfirmations(ctx);
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
    {}

    void Bootstrap(const TActorContext &ctx) {
        Sync(ctx);
    }

    STFUNC(StateSync) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvMediatorQueueStep, Handle);
            HFunc(TEvTxProcessing::TEvPlanStepAck, Handle);
            HFunc(TEvTxCoordinator::TEvCoordinatorSyncResult, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            CFunc(TEvents::TSystem::PoisonPill, Die);
        }
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvMediatorQueueStep, Handle);
            HFunc(TEvTxProcessing::TEvPlanStepAck, Handle);
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
