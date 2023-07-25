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
        if (medStep.Transactions.empty() || medStep.Transactions.back().TxId < txId) {
            return medStep.Transactions.emplace_back(txId);
        }
        auto& medTx = medStep.Transactions.back();
        Y_VERIFY_S(medTx.TxId == txId, "Transaction loading must be ordered by TxId:"
            << " Mediator# " << mediatorId
            << " step# " << step
            << " TxId# " << txId
            << " PrevTxId# " << medTx.TxId);
        return medTx;
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
                Y_FAIL("Transaction(s) not found!");
            }
        }

        return true;
    }

    TTxType GetTxType() const override { return TXTYPE_INIT; }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override {
        TTransactions transactions;
        bool result = Restore(transactions, txc, ctx);
        if (!result)
            return false;
        i64 txCounter = transactions.size();
        Self->Transactions.swap(transactions);
        *Self->MonCounters.TxInFly += txCounter;
        Self->MonCounters.CurrentTxInFly = txCounter;

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
            Y_VERIFY(mediator.Queue.empty());
            mediator.Queue.splice(mediator.Queue.end(), state.Steps);
        }
        return true;
    }

    void Complete(const TActorContext &ctx) override {
        // Send steps to connected queues
        for (ui64 mediatorId: Self->Config.Mediators->List()) {
            TMediator &mediator = Self->Mediator(mediatorId, ctx);
            auto& state = Mediators[mediatorId];
            for (auto& pr : state.Index) {
                Y_VERIFY(!pr.second->Confirmed);
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
