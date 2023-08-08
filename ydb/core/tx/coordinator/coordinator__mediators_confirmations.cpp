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

        for (const auto &pr : Confirmations->Acks) {
            const TTxId txid = pr.first;
            const THashSet<TTabletId> &confirmed = pr.second;

            // Volatile transactions are confirmed purely in memory
            if (auto it = Self->VolatileTransactions.find(txid); it != Self->VolatileTransactions.end()) {
                auto &unconfirmed = it->second.UnconfirmedAffectedSet[mediatorId];
                for (TTabletId tabletId : confirmed) {
                    auto removed = unconfirmed.erase(tabletId);
                    FLOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR,
                        "at tablet# " << Self->TabletID()
                        << " [" << internalTxGen << ":" << internalTxStep << "]"
                        << " volatile tx " << txid << " for mediator " << mediatorId << " tablet " << tabletId << " removed=" << removed);
                }

                if (unconfirmed.empty()) {
                    FLOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR,
                        "at tablet# " << Self->TabletID()
                        << " [" << internalTxGen << ":" << internalTxStep << "]"
                        << " volatile tx " << txid << " for mediator " << mediatorId << " acknowledged");
                    it->second.UnconfirmedAffectedSet.erase(mediatorId);
                }

                if (it->second.UnconfirmedAffectedSet.empty()) {
                    FLOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR,
                        "at tablet# " << Self->TabletID()
                        << " [" << internalTxGen << ":" << internalTxStep << "]"
                        << " volatile tx " << txid << " acknowledged");
                    Self->VolatileTransactions.erase(it);
                    ++CompleteTransactions;
                }

                continue;
            }

            auto it = Self->Transactions.find(txid);
            if (it == Self->Transactions.end()) {
                FLOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR,
                    "at tablet# " << Self->TabletID()
                    << " [" << internalTxGen << ":" << internalTxStep << "]"
                    << " persistent tx " << txid << " for mediator " << mediatorId << " not found");
                continue;
            }

            auto &unconfirmed = it->second.UnconfirmedAffectedSet[mediatorId];
            for (TTabletId tabletId : confirmed) {
                auto removed = unconfirmed.erase(tabletId);
                if (removed) {
                    db.Table<Schema::AffectedSet>().Key(mediatorId, txid, tabletId).Delete();
                }
                FLOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR,
                    "at tablet# " << Self->TabletID()
                    << " [" << internalTxGen << ":" << internalTxStep << "]"
                    << " persistent tx " << txid << " for mediator " << mediatorId << " tablet " << tabletId << " removed=" << removed);
            }

            if (unconfirmed.empty()) {
                FLOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR,
                    "at tablet# " << Self->TabletID()
                    << " [" << internalTxGen << ":" << internalTxStep << "]"
                    << " persistent tx " << txid << " for mediator " << mediatorId << " acknowledged");
                it->second.UnconfirmedAffectedSet.erase(mediatorId);
            }

            if (it->second.UnconfirmedAffectedSet.empty()) {
                FLOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR,
                    "at tablet# " << Self->TabletID()
                    << " [" << internalTxGen << ":" << internalTxStep << "]"
                    << " persistent tx " << txid << " acknowledged");
                db.Table<Schema::Transaction>().Key(txid).Delete();
                Self->Transactions.erase(it);
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
