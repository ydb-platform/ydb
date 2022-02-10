#include "coordinator_impl.h"

namespace NKikimr {
namespace NFlatTxCoordinator {

struct TTxCoordinator::TTxRestartMediatorQueue : public TTransactionBase<TTxCoordinator> {
    const TTabletId MediatorId;
    const ui64 GenCookie;

    TVector<bool *> StepsToConfirm;

    TTxRestartMediatorQueue(ui64 mediatorId, ui64 genCookie, TSelf *coordinator)
        : TBase(coordinator)
        , MediatorId(mediatorId)
        , GenCookie(genCookie)
    {}

    TTxType GetTxType() const override { return TXTYPE_RESTART_MEDIATOR; }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override {
        TMediator &mediator = Self->Mediator(MediatorId, ctx);
        if (mediator.GenCookie != GenCookie)
            return true;

        THashMap<TTxId,TVector<TTabletId>> pushToAffectedBuffer;
        TVector<TAutoPtr<TMediatorStep>> mediatorSteps;

        if (!Self->RestoreMediatorInfo(MediatorId, mediatorSteps, txc, pushToAffectedBuffer))
            return false;

        for (const auto& it : pushToAffectedBuffer) {
            TTransaction& transaction = Self->Transactions[it.first];
            THashSet<TTabletId>& unconfirmedAffectedSet = transaction.UnconfirmedAffectedSet[MediatorId];
            Y_VERIFY(unconfirmedAffectedSet.size() == it.second.size(),
                     "Incosistent affected set in mem in DB for txId %" PRIu64, it.first);
            for (const TTabletId affectedTabletId : it.second) {
                Y_VERIFY(unconfirmedAffectedSet.contains(affectedTabletId),
                         "Incosistent affected set in mem in DB for txId %" PRIu64 " missing tabletId %" PRIu64,
                         it.first, affectedTabletId);
            }
        }

        for (const auto &mp : mediatorSteps) {
            StepsToConfirm.push_back(&mp->Confirmed);
            mediator.Queue->Push(mp.Release());
        }

        mediator.PushUpdates = true;
        return true;
    }

    void Complete(const TActorContext &ctx) override {
        TMediator &mediator = Self->Mediator(MediatorId, ctx);
        if (mediator.GenCookie != GenCookie)
            return;

        for (bool *x : StepsToConfirm)
            *x = true;

        Self->SendMediatorStep(mediator, ctx);
    }
};

ITransaction* TTxCoordinator::CreateTxRestartMediatorQueue(TTabletId mediatorId, ui64 genCookie) {
    return new TTxRestartMediatorQueue(mediatorId, genCookie, this);
}

bool TTxCoordinator::RestoreMediatorInfo(TTabletId mediatorId, TVector<TAutoPtr<TMediatorStep>> &planned, TTransactionContext &txc, /*TKeyBuilder &kb, */THashMap<TTxId,TVector<TTabletId>> &pushToAffected) const {
    NIceDb::TNiceDb db(txc.DB);
    pushToAffected.clear();
    planned.clear();

    auto rowset = db.Table<Schema::AffectedSet>().Range(mediatorId).Select();
    if (!rowset.IsReady())
        return false;

    // Later we will need this to be sorted by stepId
    TMap<TStepId, TAutoPtr<TMediatorStep>> mediatorSteps;

    while (!rowset.EndOfSet()) {
        const TTxId txId = rowset.GetValue<Schema::AffectedSet::TransactionID>();
        auto itTransaction = Transactions.find(txId);
        Y_VERIFY(itTransaction != Transactions.end());

        TStepId step = itTransaction->second.PlanOnStep;
        auto itStep = mediatorSteps.find(step);
        if (itStep == mediatorSteps.end()) {
            itStep = mediatorSteps.insert(std::make_pair(step, new TMediatorStep(mediatorId, step))).first;
        }
        TAutoPtr<TMediatorStep>& mediatorStep = itStep->second;

        if (mediatorStep->Transactions.empty() || mediatorStep->Transactions.back().TxId != txId)
            mediatorStep->Transactions.push_back(TMediatorStep::TTx(txId));
        TMediatorStep::TTx &tx = mediatorStep->Transactions.back();

        TTabletId tablet = rowset.GetValue<Schema::AffectedSet::DataShardID>();
        pushToAffected[txId].push_back(tablet);
        tx.PushToAffected.push_back(tablet);
        tx.Moderator = 0;
        if (!rowset.Next())
            return false;
    }

    for (auto& pr : mediatorSteps)
        planned.push_back(pr.second);

    return true;
}

}
}
