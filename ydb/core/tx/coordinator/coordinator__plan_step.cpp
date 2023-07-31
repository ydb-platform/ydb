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
    std::deque<TQueueType::TSlot> Slots;

    TMap<ui64, TMediatorStepList::iterator> StepsToConfirm;
    TCoordinatorStepConfirmations ProxyPlanConfirmations;

    TInstant ExecStartMoment;
    ui64 PlannedCounter;
    ui64 DeclinedCounter;
    TInFlyAccountant InFlyAccountant;
    bool StartedStateActor = false;
    ui64 LastBlockedUpdate = 0;

    TTxPlanStep(ui64 toPlan, std::deque<TQueueType::TSlot> &&slots, TSelf *coordinator)
        : TBase(coordinator)
        , PlanOnStep(toPlan)
        , Slots(std::move(slots))
        , PlannedCounter(0)
        , DeclinedCounter(0)
        , InFlyAccountant(Self->MonCounters.StepsInFly)
    {
    }

    void Plan(TTransactionContext &txc, const TActorContext &ctx) {
        Y_UNUSED(txc);
        NIceDb::TNiceDb db(txc.DB);
        ExecStartMoment = ctx.Now();
        const bool lowDiskSpace = Self->Executor()->GetStats().IsAnyChannelYellowStop;

        THashSet<TTxId> newTransactions;
        TVector<TMediatorStep> mediatorSteps;
        THashMap<TTabletId, TVector<TTabletId>> byMediatorAffected;

        // first fill every mediator with something (every mediator must receive step)
        const ui32 mediatorsSize = Self->Config.Mediators->List().size();
        mediatorSteps.reserve(mediatorsSize);
        for (TTabletId mediatorId : Self->Config.Mediators->List()) {
            mediatorSteps.emplace_back(mediatorId, PlanOnStep);
        }

        // true when step is volatile, i.e. will be acknowledged before we commit
        bool volatileStep = Self->CoordinatorStateActor && PlanOnStep <= Self->VolatileState.LastBlockedCommitted;

        // create mediator steps
        for (auto &slot : Slots) {
            for (auto &proposal : slot) {
                for (auto &x : byMediatorAffected) {
                    x.second.clear();
                }

                const TTxId txId = proposal.TxId;
                Y_VERIFY(txId);

                // currently all transactions are non-volatile
                volatileStep = false;

                Self->MonCounters.StepConsideredTx->Inc();
                auto durationMs = (ExecStartMoment - proposal.AcceptMoment).MilliSeconds();
                Self->MonCounters.TxFromReceiveToPlan->Collect(durationMs);

                if (proposal.MaxStep < PlanOnStep) {
                    Self->MonCounters.StepOutdatedTx->Inc();
                    ProxyPlanConfirmations.Queue.emplace_back(
                        txId,
                        proposal.Proxy,
                        TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusOutdated,
                        0);
                    ++DeclinedCounter;
                    continue;
                }

                // check is transactions already processed?
                if (newTransactions.insert(txId).second == false) {
                    Self->MonCounters.StepPlannedDeclinedTx->Inc();
                    ProxyPlanConfirmations.Queue.emplace_back(
                        txId,
                        proposal.Proxy,
                        TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusPlanned,
                        PlanOnStep);
                    ++DeclinedCounter;
                    continue;
                }

                {
                    auto it = Self->Transactions.find(txId);
                    if (it != Self->Transactions.end()) {
                        Self->MonCounters.StepPlannedDeclinedTx->Inc();
                        ProxyPlanConfirmations.Queue.emplace_back(
                            txId,
                            proposal.Proxy,
                            TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusPlanned,
                            it->second.PlanOnStep);
                        ++DeclinedCounter;
                        continue;
                    }
                }

                if (lowDiskSpace && !proposal.IgnoreLowDiskSpace) {
                    Self->MonCounters.StepDeclinedNoSpaceTx->Inc();
                    ProxyPlanConfirmations.Queue.emplace_back(
                        txId,
                        proposal.Proxy,
                        TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusDeclinedNoSpace,
                        0);
                    ++DeclinedCounter;
                    continue;
                }

                // write transaction in body
                // todo: subtree insertion, moderator/body store
                {
                    TTransaction& transaction = Self->Transactions[txId];

                    transaction.PlanOnStep = PlanOnStep;
                    Y_VERIFY(!proposal.AffectedSet.empty());
                    for (const auto &txprop : proposal.AffectedSet) {
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
                    TTabletId mediatorId = mediatorSteps[idx].MediatorId;
                    TVector<TTabletId> &affected = byMediatorAffected[mediatorId];
                    if (!affected.empty()) {
                        mediatorSteps[idx].Transactions.emplace_back(txId, affected.data(), affected.size(), 0);
                    }
                }

                newTransactions.insert(txId);
                ++PlannedCounter;

                Self->MonCounters.StepPlannedTx->Inc();
                ProxyPlanConfirmations.Queue.emplace_back(
                    txId,
                    proposal.Proxy,
                    TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusPlanned,
                    PlanOnStep);
            }
        }

        for (TMediatorStep& m : mediatorSteps) {
            const ui64 mediatorId = m.MediatorId;

            // true when mediator step is volatile, i.e. we don't need to wait for persistence
            // this will often be true for mediators that don't have transactions
            bool volatileMediatorStep = Self->CoordinatorStateActor && PlanOnStep <= Self->VolatileState.LastBlockedCommitted;

            // write mediator entry
            for (const auto &tx : m.Transactions) {
                // all transactions are currently non-volatile
                volatileMediatorStep = false;

                for (TTabletId tablet : tx.PushToAffected) {
                    db.Table<Schema::AffectedSet>().Key(mediatorId, tx.TxId, tablet).Update();
                    FLOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR, "Planned transaction " << tx.TxId << " for mediator " << mediatorId << " tablet " << tablet);
                }
            }

            TMediator& mediator = Self->Mediator(mediatorId, ctx);
            if (!mediator.Queue.empty() && mediator.Queue.back().Confirmed && mediator.Queue.back().Transactions.empty()) {
                // Remove the last confirmed empty step
            }
            mediator.Queue.emplace_back(std::move(m));
            auto it = --mediator.Queue.end();

            if (volatileMediatorStep) {
                // Mark as confirmed and send before we persist everything
                // The step is protected by LastBlockedStep persisted previously
                // Note this will wait for previous uncommitted steps which may be non-volatile
                it->Confirmed = true;
                Self->SendMediatorStep(mediator, ctx);
                continue;
            }

            StepsToConfirm[mediatorId] = it;
        }

        if (volatileStep) {
            // Volatile transactions may be confirmed before we commit the transaction
            Self->SendStepConfirmations(ProxyPlanConfirmations, ctx);
        }

        Schema::SaveState(db, Schema::State::KeyLastPlanned, PlanOnStep);

        ui64 volatileLeaseMs = Self->VolatilePlanLeaseMs;
        if (volatileLeaseMs > 0) {
            StartedStateActor = Self->StartStateActor();
            if (StartedStateActor) {
                Schema::SaveState(db, Schema::State::LastBlockedActorX1, Self->CoordinatorStateActorId.RawX1());
                Schema::SaveState(db, Schema::State::LastBlockedActorX2, Self->CoordinatorStateActorId.RawX2());
            }
        }

        // Note: if lease time drops to 0 at runtime we will stop blocking new
        // steps, but we need to keep state actor active to correctly transfer
        // state to newer generation. It is likely blocked step will be outdated
        // by that time.
        if (Self->CoordinatorStateActorId && volatileLeaseMs > 0) {
            LastBlockedUpdate = Max(
                Self->VolatileState.LastBlockedPending,
                Self->VolatileState.LastBlockedCommitted,
                PlanOnStep + volatileLeaseMs);
            Self->VolatileState.LastBlockedPending = LastBlockedUpdate;
            Schema::SaveState(db, Schema::State::LastBlockedStep, LastBlockedUpdate);
        }
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

        if (StartedStateActor) {
            Self->ConfirmStateActorPersistent();
        }

        if (LastBlockedUpdate) {
            Self->VolatileState.LastBlockedCommitted = LastBlockedUpdate;
        }

        for (auto &pr : StepsToConfirm) {
            const ui64 mediatorId = pr.first;
            TMediator &mediator = Self->Mediator(mediatorId, ctx);
            Y_VERIFY(!mediator.Queue.empty());
            pr.second->Confirmed = true;
            for (auto it = pr.second; it != mediator.Queue.begin();) {
                --it;
                if (!it->Confirmed) break;
                if (!it->Transactions.empty()) break;
                // Remove empty confirmed steps before us
                // Needed so the queue does not grow for disconnected mediators
                mediator.Queue.erase(it++);
            }
            Self->SendMediatorStep(mediator, ctx);
        }

        Self->SendStepConfirmations(ProxyPlanConfirmations, ctx);

        // uncomment this to enable consistency self-check
        //Self->Execute(Self->CreateTxConsistencyCheck(), ctx);
    }
};

ITransaction* TTxCoordinator::CreateTxPlanStep(ui64 toStep, std::deque<TQueueType::TSlot> &&slots) {
    return new TTxPlanStep(toStep, std::move(slots), this);
}

}
}
