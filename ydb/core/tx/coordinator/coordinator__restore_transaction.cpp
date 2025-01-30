#include "coordinator_impl.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/tablet/tablet_exception.h>

#include <util/stream/file.h>

namespace NKikimr {
namespace NFlatTxCoordinator {

struct TTxCoordinator::TTxRestoreTransactions : public TTransactionBase<TTxCoordinator> {
    TTxRestoreTransactions(TSelf *coordinator)
        : TBase(coordinator)
    {}

    struct TMediatorState {
        TMediatorStepList Steps;
        THashMap<TStepId, TMediatorStepList::iterator> Index;
    };

    THashMap<TTabletId, TMediatorState> Mediators;

    bool StartedStateActor = false;
    ui64 LastBlockedUpdate = 0;

    TMediatorStep& GetMediatorStep(TTabletId mediatorId, TStepId step) {
        auto& state = Mediators[mediatorId];

        auto it = state.Index.find(step);
        if (it != state.Index.end()) {
            return *it->second;
        }

        auto& entry = state.Steps.emplace_back(mediatorId, step);
        state.Index[step] = --state.Steps.end();
        return entry;
    }

    TMediatorStep::TTx& GetMediatorTx(TTabletId mediatorId, TStepId step, TTxId txId) {
        auto& medStep = GetMediatorStep(mediatorId, step);
        if (medStep.Transactions.empty() || medStep.Transactions.back().TxId != txId) {
            return medStep.Transactions.emplace_back(txId);
        }
        return medStep.Transactions.back();
    }

    void EnsureLastMediatorStep(TTabletId mediatorId, TStepId step) {
        auto& state = Mediators[mediatorId];
        if (!state.Steps.empty()) {
            auto it = --state.Steps.end();
            if (step <= it->Step) {
                return; // nothing to do
            }
        }
        state.Steps.emplace_back(mediatorId, step);
        state.Index[step] = --state.Steps.end();
    }

    bool Restore(TTransactions &transactions, TTransactionContext &txc, const TActorContext &ctx) {
        Y_UNUSED(ctx);
        NIceDb::TNiceDb db(txc.DB);

        {
            auto rowset = db.Table<Schema::Transaction>().Range().Select();
            if (!rowset.IsReady())
                return false;

            while (!rowset.EndOfSet()) {
                TTxId txId = rowset.GetValue<Schema::Transaction::ID>();
                Y_VERIFY_DEBUG_S(!Self->VolatileTransactions.contains(txId),
                    "Unexpected txId# " << txId << " both volatile and persistent");
                Self->VolatileTransactions.erase(txId);
                TTransaction& transaction = transactions[txId];
                transaction.PlanOnStep = rowset.GetValue<Schema::Transaction::Plan>();
                TVector<TTabletId> affectedSet = rowset.GetValue<Schema::Transaction::AffectedSet>();
                transaction.AffectedSet.reserve(affectedSet.size());
                for (TTabletId id : affectedSet)
                    transaction.AffectedSet.insert(id);
                if (!rowset.Next())
                    return false; // data not ready
            }
        }

        Mediators.clear();

        {
            int errors = 0;
            auto rowset = db.Table<Schema::AffectedSet>().Range().Select();
            if (!rowset.IsReady())
                return false;

            while (!rowset.EndOfSet()) {
                const TTxId txId = rowset.GetValue<Schema::AffectedSet::TransactionID>();
                const TTabletId medId = rowset.GetValue<Schema::AffectedSet::MediatorID>();
                const ui64 affectedShardId = rowset.GetValue<Schema::AffectedSet::DataShardID>();

                auto itTransaction = transactions.find(txId);
                if (itTransaction != transactions.end()) {
                    auto& tx = itTransaction->second;
                    tx.UnconfirmedAffectedSet[medId].insert(affectedShardId);
                    auto& medTx = GetMediatorTx(medId, tx.PlanOnStep, txId);
                    medTx.PushToAffected.push_back(affectedShardId);
                } else {
                    LOG_ERROR_S(ctx, NKikimrServices::TX_COORDINATOR, "Transaction not found: MedId = " << medId << " TxId = " << txId << " DataShardId = " << affectedShardId);
                    ++errors;
                }

                if (!rowset.Next())
                    return false;
            }

            if (errors > 0) {
                // DB is corrupt. Make a dump and stop
                const NScheme::TTypeRegistry& tr = *AppData(ctx)->TypeRegistry;
                TString dbDumpFile = Sprintf("/tmp/coordinator_db_dump_%" PRIu64 ".%" PRIi32, Self->TabletID(), getpid());
                TFixedBufferFileOutput out(dbDumpFile);
                txc.DB.DebugDump(out, tr);
                out.Finish();
                Cerr << "Coordinator DB dumped to " << dbDumpFile;
                Sleep(TDuration::Seconds(10));
                Y_ABORT("Transaction(s) not found!");
            }
        }

        return true;
    }

    TStepId RestoreVolatileSteps() {
        TStepId maxStep = 0;
        for (auto &pr : Self->VolatileTransactions) {
            auto txId = pr.first;
            auto &tx = pr.second;
            maxStep = Max(maxStep, tx.PlanOnStep);
            for (auto &prmed : tx.UnconfirmedAffectedSet) {
                auto medId = prmed.first;
                auto &medTx = GetMediatorTx(medId, tx.PlanOnStep, txId);
                for (ui64 tabletId : prmed.second) {
                    medTx.PushToAffected.push_back(tabletId);
                }
            }
        }
        return maxStep;
    }

    TTxType GetTxType() const override { return TXTYPE_INIT; }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override {
        TTransactions transactions;
        bool result = Restore(transactions, txc, ctx);
        if (!result)
            return false;
        TStepId maxVolatileStep = RestoreVolatileSteps();
        i64 txCounter = transactions.size() + Self->VolatileTransactions.size();
        Self->Transactions.swap(transactions);
        *Self->MonCounters.TxInFly += txCounter;
        Self->MonCounters.CurrentTxInFly = txCounter;

        NIceDb::TNiceDb db(txc.DB);

        // Previous coordinator might have had transactions that were after
        // its persistent blocked range, but before LastPlanned was updated.
        // Since we pick them up as planned and send to mediators we also need
        // to make sure LastPlanned reflects that.
        if (Self->VolatileState.LastPlanned < maxVolatileStep) {
            Self->VolatileState.LastPlanned = maxVolatileStep;
            Schema::SaveState(db, Schema::State::KeyLastPlanned, maxVolatileStep);
        }

        if (Self->PrevStateActorId) {
            ui64 volatileLeaseMs = Self->VolatilePlanLeaseMs;
            if (volatileLeaseMs > 0) {
                // Make sure we start and persist new state actor before allowing clients to acquire new read steps
                StartedStateActor = Self->StartStateActor();
                if (StartedStateActor) {
                    Schema::SaveState(db, Schema::State::LastBlockedActorX1, Self->CoordinatorStateActorId.RawX1());
                    Schema::SaveState(db, Schema::State::LastBlockedActorX2, Self->CoordinatorStateActorId.RawX2());
                }

                LastBlockedUpdate = Max(
                    Self->VolatileState.LastBlockedPending,
                    Self->VolatileState.LastBlockedCommitted,
                    Self->VolatileState.LastPlanned);
                Self->VolatileState.LastBlockedPending = LastBlockedUpdate;
                Schema::SaveState(db, Schema::State::LastBlockedStep, LastBlockedUpdate);
            } else {
                // Make sure the next generation will not use outdated state
                Schema::SaveState(db, Schema::State::LastBlockedActorX1, 0);
                Schema::SaveState(db, Schema::State::LastBlockedActorX2, 0);
            }

            Self->PrevStateActorId = {};
        }

        // Prepare mediator queues
        for (ui64 mediatorId : Self->Config.Mediators->List()) {
            auto& state = Mediators[mediatorId];
            // We need to slice steps in a sorted order
            state.Steps.sort([](const TMediatorStep& a, const TMediatorStep& b) -> bool {
                return a.Step < b.Step;
            });
            // Make sure each mediator will receive last planned step
            if (Self->VolatileState.LastPlanned != 0) {
                EnsureLastMediatorStep(mediatorId, Self->VolatileState.LastPlanned);
            }
            // Splice all steps to the queue
            TMediator& mediator = Self->Mediator(mediatorId, ctx);
            Y_ABORT_UNLESS(mediator.Queue.empty());
            mediator.Queue.splice(mediator.Queue.end(), state.Steps);
        }
        return true;
    }

    void Complete(const TActorContext &ctx) override {
        if (StartedStateActor) {
            Self->ConfirmStateActorPersistent();
        }

        if (LastBlockedUpdate) {
            Self->VolatileState.LastBlockedCommitted = LastBlockedUpdate;
        }

        // Send steps to connected queues
        for (ui64 mediatorId: Self->Config.Mediators->List()) {
            TMediator &mediator = Self->Mediator(mediatorId, ctx);
            auto& state = Mediators[mediatorId];
            for (auto& pr : state.Index) {
                Y_ABORT_UNLESS(!pr.second->Confirmed);
                pr.second->Confirmed = true;
            }
            Self->SendMediatorStep(mediator, ctx);
        }

        // start plan process.
        Self->Become(&TSelf::StateWork);
        Self->SignalTabletActive(ctx);
        Self->SchedulePlanTick();

        if (Self->Config.HaveProcessingParams) {
            Self->SubscribeToSiblings();
        } else {
            Self->RestoreProcessingParams(ctx);
        }
    }
};

ITransaction* TTxCoordinator::CreateTxRestoreTransactions() {
    return new TTxRestoreTransactions(this);
}

}
}
