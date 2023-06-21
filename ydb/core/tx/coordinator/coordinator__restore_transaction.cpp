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
                    itTransaction->second.UnconfirmedAffectedSet[medId].insert(affectedShardId);
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
        return true;
    }

    void Complete(const TActorContext &ctx) override {
        // start mediator queues
        for (ui64 mediatorId: Self->Config.Mediators->List()) {
            TMediator &mediator = Self->Mediator(mediatorId, ctx);
            Y_UNUSED(mediator);
        }

        // start plan process.
        Self->Become(&TSelf::StateWork);
        Self->SignalTabletActive(ctx);
        Self->SchedulePlanTick(ctx);

        if (!Self->Config.HaveProcessingParams) {
            Self->RestoreProcessingParams(ctx);
        }
    }
};

ITransaction* TTxCoordinator::CreateTxRestoreTransactions() {
    return new TTxRestoreTransactions(this);
}

}
}
