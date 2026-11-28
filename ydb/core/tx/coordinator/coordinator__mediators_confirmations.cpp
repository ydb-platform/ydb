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
                    if (const auto& trace = it->second.Trace) {
                        trace->Events.Event(trace->Span, 11, removed ? "participant plan acknowledged" : "duplicate participant plan ack", [&] {
                            NWilson::TKeyValueList attributes;
                            attributes["tablet_id"] = static_cast<i64>(tabletId);
                            attributes["mediator_id"] = static_cast<i64>(mediatorId);
                            attributes["remaining_for_mediator"] = static_cast<i64>(unconfirmed.size());
                            return attributes;
                        });
                    }
                    FLOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR, "",
                        {"tabletId", Self->TabletID()},
                        {"internalTxGen", internalTxGen},
                        {"internalTxStep", internalTxStep},
                        {"volatileTx", txid},
                        {"mediatorId", mediatorId},
                        {"confirmedTabletId", tabletId},
                        {"removed", removed});
                }

                if (unconfirmed.empty()) {
                    FLOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR, "Acknowledged for mediator",
                        {"tabletId", Self->TabletID()},
                        {"internalTxGen", internalTxGen},
                        {"internalTxStep", internalTxStep},
                        {"volatileTx", txid},
                        {"mediatorId", mediatorId});
                    it->second.UnconfirmedAffectedSet.erase(mediatorId);
                }

                if (it->second.UnconfirmedAffectedSet.empty()) {
                    FLOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR, "Acknowledged fully",
                        {"tabletId", Self->TabletID()},
                        {"internalTxGen", internalTxGen},
                        {"internalTxStep", internalTxStep},
                        {"volatileTx", txid});
                    if (it->second.Trace) { it->second.Trace->Finish("all_plan_acknowledgments_received"); }
                    Self->VolatileTransactions.erase(it);
                    ++CompleteTransactions;
                }

                continue;
            }

            auto it = Self->Transactions.find(txid);
            if (it == Self->Transactions.end()) {
                FLOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR, "Transaction not found",
                    {"tabletId", Self->TabletID()},
                    {"internalTxGen", internalTxGen},
                    {"internalTxStep", internalTxStep},
                    {"persistentTx", txid},
                    {"mediatorId", mediatorId});
                continue;
            }

            auto &unconfirmed = it->second.UnconfirmedAffectedSet[mediatorId];
            for (TTabletId tabletId : confirmed) {
                auto removed = unconfirmed.erase(tabletId);
                if (const auto& trace = it->second.Trace) {
                    trace->Events.Event(trace->Span, 11, removed ? "participant plan acknowledged" : "duplicate participant plan ack", [&] {
                        NWilson::TKeyValueList attributes;
                        attributes["tablet_id"] = static_cast<i64>(tabletId);
                        attributes["mediator_id"] = static_cast<i64>(mediatorId);
                        attributes["remaining_for_mediator"] = static_cast<i64>(unconfirmed.size());
                        return attributes;
                    });
                }
                if (removed) {
                    db.Table<Schema::AffectedSet>().Key(mediatorId, txid, tabletId).Delete();
                }
                FLOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR, "",
                        {"tabletId", Self->TabletID()},
                        {"internalTxGen", internalTxGen},
                        {"internalTxStep", internalTxStep},
                        {"persistentTx", txid},
                        {"mediatorId", mediatorId},
                        {"confirmedTabletId", tabletId},
                        {"removed", removed});
            }

            if (unconfirmed.empty()) {
                FLOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR, "Acknowledged for mediator",
                    {"tabletId", Self->TabletID()},
                    {"internalTxGen", internalTxGen},
                    {"internalTxStep", internalTxStep},
                    {"persistentTx", txid},
                    {"mediatorId", mediatorId});
                it->second.UnconfirmedAffectedSet.erase(mediatorId);
            }

            if (it->second.UnconfirmedAffectedSet.empty()) {
                FLOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR, "Acknowledged fully",
                    {"tabletId", Self->TabletID()},
                    {"internalTxGen", internalTxGen},
                    {"internalTxStep", internalTxStep},
                    {"persistentTx", txid});
                db.Table<Schema::Transaction>().Key(txid).Delete();
                if (it->second.Trace) { it->second.Trace->Finish("all_plan_acknowledgments_received"); }
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
