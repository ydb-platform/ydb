#pragma once
#include <ydb/core/tablet/tablet_pipe_client_cache.h>
#include <ydb/core/tx/tx_processing.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>

namespace NKikimr {

    class TFakeCoordinator : public TActor<TFakeCoordinator>, public NTabletFlatExecutor::TTabletExecutedFlat {
    public:
        struct TState : TThrRefBase {
            ui64 CurrentStep = 5000000;
            ui64 FrontStep = 0;
            TMap<std::pair<ui64, ui64>, TVector<TAutoPtr<TEvTxProcessing::TEvPlanStep>>> QueuedPlans; // shard+step->event
            TMap<ui64, TSet<ui64>> TxIds; // txid -> list of shards

            TState() = default;

            typedef TIntrusivePtr<TState> TPtr;
        };

        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::TX_COORDINATOR_ACTOR;
        }

        TFakeCoordinator(const TActorId &tablet, TTabletStorageInfo *info, TState::TPtr state)
            : TActor<TFakeCoordinator>(&TFakeCoordinator::StateInit)
            , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
            , State(state)
            , Pipes(NTabletPipe::CreateUnboundedClientCache(GetPipeClientConfig()))
        {
        }

        static NTabletPipe::TClientConfig GetPipeClientConfig() {
            NTabletPipe::TClientConfig config;
            config.CheckAliveness = true;
            config.RetryPolicy = {
                .RetryLimitCount = 3,
                .MinRetryTime = TDuration::MilliSeconds(10),
                .MaxRetryTime = TDuration::MilliSeconds(500),
                .BackoffMultiplier = 2
            };
            return config;
        }

        void DefaultSignalTabletActive(const TActorContext &) override {
            // must be empty
        }

        void OnActivateExecutor(const TActorContext &ctx) final {
            Become(&TFakeCoordinator::StateWork);
            SignalTabletActive(ctx);
            SendQueued(ctx);
        }

        void OnDetach(const TActorContext &ctx) override {
            Pipes->Detach(ctx);
            Die(ctx);
        }

        void OnTabletDead(TEvTablet::TEvTabletDead::TPtr &ev, const TActorContext &ctx) override {
            Y_UNUSED(ev);
            Pipes->Detach(ctx);
            Die(ctx);
        }

        void StateInit(STFUNC_SIG) {
            StateInitImpl(ev, SelfId());
        }

        void StateWork(STFUNC_SIG) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvTablet::TEvTabletDead, HandleTabletDead);
                HFunc(TEvTxProxy::TEvProposeTransaction, Handle);
                HFunc(TEvTabletPipe::TEvClientConnected, Handle);
                HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
                HFunc(TEvTxProcessing::TEvPlanStepAccepted, Handle);
                HFunc(TEvents::TEvPoisonPill, Handle);
            }
        }

        void BrokenState(STFUNC_SIG) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvTablet::TEvTabletDead, HandleTabletDead);
            }
        }

        void Handle(TEvTxProxy::TEvProposeTransaction::TPtr& ev, const TActorContext& ctx) {
            auto tx = ev->Get()->Record.GetTransaction();
            if (State->TxIds.find(tx.GetTxId()) != State->TxIds.end()) {
                SendQueued(ctx);
                return;
            }

            State->CurrentStep = Max(1 + State->CurrentStep, tx.GetMinStep());
            TSet<ui64> shards;
            for (auto shard : tx.GetAffectedSet()) {
                shards.insert(shard.GetTabletId());
            }

            State->TxIds.insert(std::make_pair(tx.GetTxId(), shards));
            Cerr << "FAKE_COORDINATOR: Add transaction: " << tx.GetTxId() << " at step: " << State->CurrentStep << "\n";
            for (ui64 shard : shards) {
                auto evPlan = new TEvTxProcessing::TEvPlanStep(State->CurrentStep, 0, shard);
                auto planTx = evPlan->Record.AddTransactions();
                planTx->SetCoordinator(TabletID());
                planTx->SetTxId(tx.GetTxId());
                ActorIdToProto(ctx.SelfID, planTx->MutableAckTo());
                State->QueuedPlans[std::make_pair(shard, State->CurrentStep)].push_back(evPlan);
            }

            AdvancePlan(ctx);
        }

        void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx) {
            if (!Pipes->OnConnect(ev)) {
                if (ev->Get()->Dead) {
                    AckPlanStepsForDeadTablet(ev->Get()->TabletId);
                    AdvancePlan(ctx);
                } else {
                    SendQueued(ctx, ev->Get()->TabletId);
                }

            }
        }

        void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx) {
            Pipes->OnDisconnect(ev);
            SendQueued(ctx, ev->Get()->TabletId);
        }

        void Handle(TEvTxProcessing::TEvPlanStepAccepted::TPtr& ev, const TActorContext& ctx) {
            auto stepIt = State->QueuedPlans.find(std::make_pair(ev->Get()->Record.GetTabletId(), ev->Get()->Record.GetStep()));
            if (stepIt == State->QueuedPlans.end()) {
                return;
            }

            for (auto& tabletData : stepIt->second) {
                for (auto& tx : tabletData->Record.GetTransactions()) {
                    UnlinkTx(ev->Get()->Record.GetTabletId(), tx.GetTxId());
                }
            }
            State->QueuedPlans.erase(stepIt);

            AdvancePlan(ctx);
        }

        void UnlinkTx(ui64 tabletId, ui64 txId) {
            auto txIt = State->TxIds.find(txId);
            if (txIt != State->TxIds.end()) {
                txIt->second.erase(tabletId);
                if (txIt->second.empty()) {
                    Cerr << "FAKE_COORDINATOR: Erasing txId " << txId << Endl;
                    State->TxIds.erase(txIt);
                }
            }
        }

        void AckPlanStepsForDeadTablet(ui64 tabletId) {
            auto pit = State->QueuedPlans.lower_bound(std::make_pair(tabletId, 0));
            while (pit != State->QueuedPlans.end() && pit->first.first == tabletId) {
                Cerr << "FAKE_COORDINATOR: forgetting step " << pit->first.second << " for dead tablet " << pit->first.first << Endl;
                auto toErase = pit;
                ++pit;
                for (const auto& evPlan : toErase->second) {
                    for (const auto& mediatorTx : evPlan->Record.GetTransactions()) {
                        ui64 txId = mediatorTx.GetTxId();
                        UnlinkTx(tabletId, txId);
                    }
                }
                State->QueuedPlans.erase(toErase);
            }
        }

        ui64 GetMinStep() const {
            ui64 minStep = Max<ui64>();
            for (auto& kv : State->QueuedPlans) {
                const ui64 step = kv.first.second;
                minStep = Min(minStep, step);
            }
            return minStep;
        }

        void AdvancePlan(const TActorContext& ctx, ui64 onlyForTabletId = 0) {
            ui64 minStep = GetMinStep();

            if (minStep == Max<ui64>()) {
                return;
            }

            Cerr << "FAKE_COORDINATOR: advance: minStep" << minStep << " State->FrontStep: " << State->FrontStep << "\n";

            if (State->FrontStep >= minStep) {
                return;
            }

            State->FrontStep = minStep;

            SendQueued(ctx, onlyForTabletId);
        }

        void SendQueued(const TActorContext& ctx, ui64 onlyForTabletId = 0) {

            for (auto& kv : State->QueuedPlans) {
                ui64 tabletId = kv.first.first;
                if (onlyForTabletId && onlyForTabletId != tabletId) {
                    continue;
                }
                const ui64 step = kv.first.second;
                if (step != State->FrontStep) {
                    continue;
                }

                for (auto& ev : kv.second) {
                    TAllocChunkSerializer serializer;
                    ev->SerializeToArcadiaStream(&serializer);
                    Cerr << "FAKE_COORDINATOR:  Send Plan to tablet " << tabletId << " for txId: " << ev->Record.GetTransactions(0).GetTxId() << " at step: " << step << "\n";

                    Pipes->Send(ctx, tabletId, ev->EventType, serializer.Release(ev->CreateSerializationInfo()));
                }
            }
        }

        void Handle(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            Become(&TThis::BrokenState);
            ctx.Send(Tablet(), new TEvents::TEvPoisonPill);
        }

    private:
        TState::TPtr State;
        TAutoPtr<NTabletPipe::IClientCache> Pipes;
    };

    void BootFakeCoordinator(TTestActorRuntime& runtime, ui64 tabletId, TFakeCoordinator::TState::TPtr state);
}
