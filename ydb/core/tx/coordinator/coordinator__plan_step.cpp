#include "coordinator_impl.h"

#include <util/generic/hash_set.h>

namespace NKikimr {
namespace NFlatTxCoordinator {

struct TInFlyAccountant {
    ::NMonitoring::TDynamicCounters::TCounterPtr Counter;
    TInFlyAccountant(::NMonitoring::TDynamicCounters::TCounterPtr counter)
        : Counter(counter)
    {
        Counter->Inc();
    }
    ~TInFlyAccountant() {
        Counter->Dec();
    }
};

struct TTxCoordinator::TTxPlanStep : public TTransactionBase<TTxCoordinator> {
    const ui64 PlanOnStep;
    TVector<TQueueType::TSlot> Slots;

    TMap<ui64, std::pair<ui64, bool *>> StepsToConfirm;
    TAutoPtr<TCoordinatorStepConfirmations> ProxyPlanConfirmations;

    TInstant ExecStartMoment;
    ui64 PlannedCounter;
    ui64 DeclinedCounter;
    TInFlyAccountant InFlyAccountant;

    TTxPlanStep(ui64 toPlan, TVector<TQueueType::TSlot> &slots, TSelf *coordinator)
        : TBase(coordinator)
        , PlanOnStep(toPlan)
        , PlannedCounter(0)
        , DeclinedCounter(0)
        , InFlyAccountant(Self->MonCounters.StepsInFly)
    {
        Slots.swap(slots);
    }

    void Plan(TTransactionContext &txc, const TActorContext &ctx) {
        Y_UNUSED(txc);
        NIceDb::TNiceDb db(txc.DB);
        ExecStartMoment = ctx.Now();
        const bool lowDiskSpace = Self->Executor()->GetStats().IsAnyChannelYellowStop;

        THashSet<TTxId> newTransactions;
        TVector<TAutoPtr<TMediatorStep>> mediatorSteps;
        THashMap<TTabletId, TVector<TTabletId>> byMediatorAffected;

        // first fill every mediator with something (every mediator must receive step)
        const ui32 mediatorsSize = Self->Config.Mediators->List().size();
        mediatorSteps.reserve(mediatorsSize);
        for (TTabletId mediatorId : Self->Config.Mediators->List()) {
            mediatorSteps.push_back(new TMediatorStep(mediatorId, PlanOnStep));
        }

        // create mediator steps
        ProxyPlanConfirmations.Reset(new TCoordinatorStepConfirmations(PlanOnStep));
        for (const auto &slot : Slots) {
            TQueueType::TQ &queue = *slot.Queue;
            TQueueType::TQ::TReadIterator iterator = queue.Iterator();
            while (TTransactionProposal *proposal = iterator.Next()) {
                for (auto &x : byMediatorAffected) {
                    x.second.clear();
                }

                const TTxId txId = proposal->TxId;
                Y_VERIFY(txId);

                Self->MonCounters.StepConsideredTx->Inc();
                auto durationMs = (ExecStartMoment - proposal->AcceptMoment).MilliSeconds();
                Self->MonCounters.TxFromReceiveToPlan->Collect(durationMs);

                if (proposal->MaxStep < PlanOnStep) {
                    Self->MonCounters.StepOutdatedTx->Inc();
                    ProxyPlanConfirmations->Queue->Push(new TCoordinatorStepConfirmations::TEntry {
                        txId,
                        proposal->Proxy,
                        TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusOutdated,
                        0 });
                    ++DeclinedCounter;
                    continue;
                }

                // check is transactions already processed?
                if (newTransactions.insert(txId).second == false) {
                    Self->MonCounters.StepPlannedDeclinedTx->Inc();
                    ProxyPlanConfirmations->Queue->Push(new TCoordinatorStepConfirmations::TEntry {
                        txId,
                        proposal->Proxy,
                        TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusPlanned,
                        PlanOnStep });
                    ++DeclinedCounter;
                    continue;
                }

                {
                    auto it = Self->Transactions.find(txId);
                    if (it != Self->Transactions.end()) {
                        Self->MonCounters.StepPlannedDeclinedTx->Inc();
                        ProxyPlanConfirmations->Queue->Push(new TCoordinatorStepConfirmations::TEntry {
                            txId,
                            proposal->Proxy,
                            TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusPlanned,
                            it->second.PlanOnStep });
                        ++DeclinedCounter;
                        continue;
                    }
                }

                if (lowDiskSpace && !proposal->IgnoreLowDiskSpace) {
                    Self->MonCounters.StepDeclinedNoSpaceTx->Inc();
                    ProxyPlanConfirmations->Queue->Push(new TCoordinatorStepConfirmations::TEntry{
                        txId,
                        proposal->Proxy,
                        TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusDeclinedNoSpace,
                        0
                    });
                    ++DeclinedCounter;
                    continue;
                }

                // write transaction in body
                // todo: subtree insertion, moderator/body store
                {
                    TTransaction& transaction = Self->Transactions[txId];

                    transaction.PlanOnStep = PlanOnStep;
                    Y_VERIFY(!proposal->AffectedSet.empty());
                    for (const auto &txprop : proposal->AffectedSet) {
                        const TTabletId affectedTablet = txprop.TabletId;
                        const TTabletId mediatorId = Self->Config.Mediators->Select(affectedTablet);

                        transaction.AffectedSet.insert(affectedTablet);
                        transaction.UnconfirmedAffectedSet[mediatorId].insert(affectedTablet);

                        byMediatorAffected[mediatorId].push_back(affectedTablet);
                    }

                    TVector<TTabletId> affectedSet(transaction.AffectedSet.begin(), transaction.AffectedSet.end());

                    db.Table<Schema::Transaction>().Key(txId).Update(
                                NIceDb::TUpdate<Schema::Transaction::Plan>(PlanOnStep),
                                NIceDb::TUpdate<Schema::Transaction::AffectedSet>(affectedSet));
                    FLOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR, "Transaction " << txId << " has been planned");
                    // todo: moderator, proxy
                }

                for (ui32 idx = 0; idx < mediatorsSize; ++idx) {
                    TTabletId mediatorId = mediatorSteps[idx]->MediatorId;
                    TVector<TTabletId> &affected = byMediatorAffected[mediatorId];
                    if (!affected.empty()) {
                        mediatorSteps[idx]->Transactions.push_back(TMediatorStep::TTx(txId, &affected.front(), affected.size(), 0));
                    }
                }

                newTransactions.insert(txId);
                ++PlannedCounter;

                Self->MonCounters.StepPlannedTx->Inc();
                ProxyPlanConfirmations->Queue->Push(new TCoordinatorStepConfirmations::TEntry {
                    txId,
                    proposal->Proxy,
                    TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusPlanned,
                    PlanOnStep } );
            }
        }

        for (const TAutoPtr<TMediatorStep> &mp : mediatorSteps) {
            const ui64 mediatorId = mp->MediatorId;

            // write mediator entry
            for (const auto &tx : mp->Transactions) {
                for (TTabletId tablet : tx.PushToAffected) {
                    db.Table<Schema::AffectedSet>().Key(mediatorId, tx.TxId, tablet).Update();
                    FLOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR, "Planned transaction " << tx.TxId << " for mediator " << mediatorId << " tablet " << tablet);
                }
            }

            TMediator& mediator = Self->Mediator(mediatorId, ctx);
            if (mediator.PushUpdates) {
                StepsToConfirm[mediatorId] = std::pair<ui64, bool *>(mediator.GenCookie, &mp->Confirmed);
                mediator.Queue->Push(mp.Release());
            } else if (!StepsToConfirm.empty()) {
                FLOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR, "PushUpdates false for mediator " << mediatorId << " step " << PlanOnStep);
            }
        }
        db.Table<Schema::State>().Key(Schema::State::KeyLastPlanned).Update(NIceDb::TUpdate<Schema::State::StateValue>(PlanOnStep));
    }

    TTxType GetTxType() const override { return TXTYPE_STEP; }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override {
        PlannedCounter = 0;
        DeclinedCounter = 0;

        Plan(txc, ctx);

        *Self->MonCounters.TxPlanned += PlannedCounter;
        *Self->MonCounters.TxInFly += PlannedCounter;
        Self->MonCounters.CurrentTxInFly += PlannedCounter;
        *Self->MonCounters.TxDeclined += DeclinedCounter;

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        auto durationMs = (ctx.Now() - ExecStartMoment).MilliSeconds();
        Self->MonCounters.TxPlanLatency->Collect(durationMs);

        for (auto &cx : StepsToConfirm) {
            const ui64 mediatorId = cx.first;
            TMediator &mediator = Self->Mediator(mediatorId, ctx);
            if (mediator.GenCookie == cx.second.first) {
                *cx.second.second = true;
                Self->SendMediatorStep(mediator, ctx);
            }
        }

        ctx.Send(ctx.SelfID, new TEvTxCoordinator::TEvCoordinatorConfirmPlan(ProxyPlanConfirmations));

        // uncomment this to enable consistency self-check
        //Self->Execute(Self->CreateTxConsistencyCheck(), ctx);
    }
};

ITransaction* TTxCoordinator::CreateTxPlanStep(ui64 toStep, TVector<TQueueType::TSlot> &slots) {
    return new TTxPlanStep(toStep, slots, this);
}

}
}
