#include "schemeshard_impl.h"

namespace NKikimr::NSchemeShard {

struct TTxSchemeChangeRecordsCleanup : public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
    TTxSchemeChangeRecordsCleanup(TSchemeShard* self)
        : TTransactionBase(self)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);
        const ui64 minOrder = Self->GetMinSubscriberOrder();
        if (minOrder == 0) {
            return true;
        }
        return Self->DeleteAckedSchemeChangeRecords(db, 0, minOrder);
    }

    void Complete(const TActorContext&) override {}
};

struct TTxForceAdvanceSubscriber : public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
    TEvSchemeShard::TEvForceAdvanceSubscriber::TPtr Request;
    THolder<TEvSchemeShard::TEvForceAdvanceSubscriberResult> Result;

    TTxForceAdvanceSubscriber(TSchemeShard* self, TEvSchemeShard::TEvForceAdvanceSubscriber::TPtr& ev)
        : TTransactionBase(self)
        , Request(ev)
        , Result(MakeHolder<TEvSchemeShard::TEvForceAdvanceSubscriberResult>())
    {}

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        const auto& record = Request->Get()->Record;
        const TString& subscriberId = record.GetSubscriberId();

        NIceDb::TNiceDb db(txc.DB);

        auto rowset = db.Table<Schema::SchemeChangeSubscribers>().Key(subscriberId).Select();
        if (!rowset.IsReady()) {
            return false;
        }

        if (!rowset.IsValid()) {
            Result->Record.SetStatus(NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_NOT_REGISTERED);
            Result->Record.SetReason("Subscriber not registered: " + subscriberId);
            return true;
        }

        const ui64 oldMinOrder = Self->GetMinSubscriberOrder();

        const ui64 newOrder = Self->NextSchemeChangeOrder;
        const TInstant now = TInstant::Now();

        db.Table<Schema::SchemeChangeSubscribers>().Key(subscriberId).Update(
            NIceDb::TUpdate<Schema::SchemeChangeSubscribers::LastAckedOrder>(newOrder),
            NIceDb::TUpdate<Schema::SchemeChangeSubscribers::LastActivityAt>(now.MicroSeconds())
        );

        if (auto it = Self->Subscribers.find(subscriberId); it != Self->Subscribers.end()) {
            it->second.LastAckedOrder = newOrder;
            it->second.LastActivityAt = now;
        }

        if (!Self->DeleteAckedSchemeChangeRecords(db, oldMinOrder, Self->GetMinSubscriberOrder())) {
            return false;
        }

        Result->Record.SetStatus(NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);
        Result->Record.SetLastAckedOrder(newOrder);

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        ctx.Send(Request->Sender, Result.Release());
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxSchemeChangeRecordsCleanup() {
    return new TTxSchemeChangeRecordsCleanup(this);
}

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxForceAdvanceSubscriber(TEvSchemeShard::TEvForceAdvanceSubscriber::TPtr& ev) {
    return new TTxForceAdvanceSubscriber(this, ev);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvForceAdvanceSubscriber::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxForceAdvanceSubscriber(ev), ctx);
}

} // namespace NKikimr::NSchemeShard
