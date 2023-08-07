#include "coordinator_impl.h"

#include <ydb/core/tablet_flat/flat_cxx_database.h>

#include <util/generic/hash_set.h>

namespace NKikimr {
namespace NFlatTxCoordinator {

struct TTxCoordinator::TTxMediatorConfirmations : public TTransactionBase<TTxCoordinator> {
    std::unique_ptr<TMediatorConfirmations> Confirmations;
    i64 CompleteTransactions;

    TTxMediatorConfirmations(std::unique_ptr<TMediatorConfirmations> &&confirmations, TSelf *coordinator)
        : TBase(coordinator)
        , Confirmations(std::move(confirmations))
        , CompleteTransactions(0)
    {}

    TTxType GetTxType() const override { return TXTYPE_MEDIATOR_CONFIRMATIONS; }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override {
        const TTabletId mediatorId = Confirmations->MediatorId;
        Y_UNUSED(ctx);
        CompleteTransactions = 0;
        NIceDb::TNiceDb db(txc.DB);

        ui64 internalTxGen = txc.Generation;
        ui64 internalTxStep = txc.Step;

        for (const auto &txidsx : Confirmations->Acks) {
            const TTxId txid = txidsx.first;
            auto txit = Self->Transactions.find(txid);
            if (txit == Self->Transactions.end()) {
                FLOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR,
                             "at tablet# " << Self->TabletID()
                             << " gen:step " << internalTxGen << ":" << internalTxStep
                             << " Mediator " << mediatorId << " confirmed finish of transaction " << txid << " but transaction wasn't found");
                for (const TTabletId affected : txidsx.second) {
                    db.Table<Schema::AffectedSet>().Key(mediatorId, txid, affected).Delete();
                }
                continue;
            }

            THashSet<TTabletId>& mediatorAffectedSet = txit->second.UnconfirmedAffectedSet[mediatorId];
            for (const TTabletId affected : txidsx.second) {
                THashSet<TTabletId>::size_type result = mediatorAffectedSet.erase(affected);
                db.Table<Schema::AffectedSet>().Key(mediatorId, txid, affected).Delete();
                FLOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR,
                             "at tablet# " << Self->TabletID()
                             << " gen:step " << internalTxGen << ":" << internalTxStep
                             << " Confirmed transaction " << txid << " for mediator " << mediatorId << " tablet " << affected << " result=" << result);
            }

            if (mediatorAffectedSet.empty()) {
                FLOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR,
                             "at tablet# " << Self->TabletID()
                             << " gen:step " << internalTxGen << ":" << internalTxStep
                             << " Mediator " << mediatorId << " confirmed finish of transaction " << txid);
                txit->second.UnconfirmedAffectedSet.erase(mediatorId);
            }

            if (txit->second.UnconfirmedAffectedSet.empty()) { // transaction finished
                FLOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR,
                             "at tablet# " << Self->TabletID()
                             << " gen:step " << internalTxGen << ":" << internalTxStep
                             << " Transaction " << txid << " has been completed");
                db.Table<Schema::Transaction>().Key(txid).Delete();
                Self->Transactions.erase(txit);
                ++CompleteTransactions;
            }
        }

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        *Self->MonCounters.TxInFly -= CompleteTransactions;
        Self->MonCounters.CurrentTxInFly -= CompleteTransactions;

        Y_UNUSED(ctx);
    }
};

ITransaction* TTxCoordinator::CreateTxMediatorConfirmations(std::unique_ptr<TMediatorConfirmations> &&confirmations) {
    return new TTxMediatorConfirmations(std::move(confirmations), this);
}

}
}
