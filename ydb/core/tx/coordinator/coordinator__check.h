#pragma once

#include "coordinator_impl.h"

#include <util/generic/hash_set.h>

namespace NKikimr {
namespace NFlatTxCoordinator {

struct TTxCoordinator::TTxConsistencyCheck : public TTransactionBase<TTxCoordinator> {
    TTxConsistencyCheck(TSelf* self)
        : TTransactionBase<TTxCoordinator>(self)
    {}

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);
        TTransactions transactions;
        {
            auto rowset = db.Table<Schema::Transaction>().Range().Select();
            if (!rowset.IsReady())
                return false;

            while (!rowset.EndOfSet()) {
                TTxId txId = rowset.GetKey();
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
            auto rowset = db.Table<Schema::AffectedSet>().Range().Select();
            if (!rowset.IsReady())
                return false;
            while (!rowset.EndOfSet()) {
                TTabletId medId = rowset.GetValue<Schema::AffectedSet::MediatorID>();
                TTxId txId = rowset.GetValue<Schema::AffectedSet::TransactionID>();
                auto itTransaction = transactions.find(txId);
                Y_ENSURE(itTransaction != transactions.end(), "Could not find mediator's transaction");
                THashSet<TTabletId>& unconfirmedAffectedSet = itTransaction->second.UnconfirmedAffectedSet[medId];
                unconfirmedAffectedSet.insert(rowset.GetValue<Schema::AffectedSet::DataShardID>());
                if (!rowset.Next())
                    return false;
            }
        }
        {
            Y_ENSURE(transactions.size() == Self->Transactions.size(), "Size of in memory and stored transactions mismatch");
            for (auto it = transactions.begin(); it != transactions.end(); ++it) {
                auto jt = Self->Transactions.find(it->first);
                Y_ENSURE(jt != Self->Transactions.end(), "Stored transaction hasn't been found in memory");
                Y_ENSURE(it->second.PlanOnStep == jt->second.PlanOnStep, "Plan step mismatch");
                Y_ENSURE(it->second.AffectedSet == jt->second.AffectedSet, "Affected set mismatch");
                Y_ENSURE(it->second.UnconfirmedAffectedSet.size() == jt->second.UnconfirmedAffectedSet.size(), "Unconfirmed affected set size mismatch");
                for (auto kt = it->second.UnconfirmedAffectedSet.begin(); kt != it->second.UnconfirmedAffectedSet.end(); ++kt) {
                    auto lt = jt->second.UnconfirmedAffectedSet.find(kt->first);
                    Y_ENSURE(lt != jt->second.UnconfirmedAffectedSet.end(), "Mediator hasn't' been found in memory");
                    Y_ENSURE(lt->second == kt->second, "Unconfirmed affected set doesn't match");
                }
            }
        }
        return true;
    }

    void Complete(const TActorContext&) override {
    }
};

}
}
