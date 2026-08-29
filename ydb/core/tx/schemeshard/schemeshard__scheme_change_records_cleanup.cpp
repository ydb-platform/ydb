#include "schemeshard_impl.h"

#include <ydb/core/base/appdata.h>

namespace NKikimr::NSchemeShard {

struct TTxSchemeChangeRecordsCleanup : public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
    bool HasMoreToCleanup = false;

    TTxSchemeChangeRecordsCleanup(TSchemeShard* self)
        : TTransactionBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_SCHEME_CHANGE_RECORDS_CLEANUP; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        HasMoreToCleanup = false;
        NIceDb::TNiceDb db(txc.DB);
        const ui64 minOrder = Self->GetMinSubscriberOrder(ctx.Now());
        if (minOrder == 0) {
            return true;
        }
        return Self->DeleteAckedSchemeChangeRecords(db, minOrder,
            Self->SchemeChangeCleanupBatchSize, HasMoreToCleanup);
    }

    void Complete(const TActorContext& ctx) override {
        Self->UpdateSchemeChangeGauges();
        ++Self->SchemeChangeCleanupTxCount;
        if (HasMoreToCleanup) {
            Self->EnqueueSchemeChangeRecordsCleanup(ctx);
        }
    }
};

struct TTxForceAdvanceSubscriber : public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
    TString SubscriberId;
    TActorId ReplyTo;
    THolder<TEvSchemeShard::TEvForceAdvanceSubscriberResult> Result;
    bool HasMoreToCleanup = false;

    TTxForceAdvanceSubscriber(TSchemeShard* self, TEvSchemeShard::TEvForceAdvanceSubscriber::TPtr& ev)
        : TTransactionBase(self)
        , SubscriberId(ev->Get()->Record.GetSubscriberId())
        , ReplyTo(ev->Sender)
        , Result(MakeHolder<TEvSchemeShard::TEvForceAdvanceSubscriberResult>())
    {}

    TTxForceAdvanceSubscriber(TSchemeShard* self, const TString& subscriberId, TActorId replyTo)
        : TTransactionBase(self)
        , SubscriberId(subscriberId)
        , ReplyTo(replyTo)
        , Result(MakeHolder<TEvSchemeShard::TEvForceAdvanceSubscriberResult>())
    {}

    TTxType GetTxType() const override { return TXTYPE_FORCE_ADVANCE_SCHEME_CHANGE_SUBSCRIBER; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        HasMoreToCleanup = false;
        const TString& subscriberId = SubscriberId;

        if (!AppData()->FeatureFlags.GetEnableSchemeChangeRecords()) {
            Result->Record.SetStatus(NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_INVALID_REQUEST);
            Result->Record.SetReason("Scheme change records are disabled");
            return true;
        }

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

        const ui64 oldOrder = rowset.GetValue<Schema::SchemeChangeSubscribers::LastAckedOrder>();
        // Use the visible tail, not the reserved one: the cursor must not sit
        // above a record an in-flight operation has yet to finalise.
        const ui64 newOrder = Max(oldOrder, Self->GetVisibleSchemeChangeTail());
        const TInstant now = ctx.Now();

        // Only mark Lost if records are actually skipped; force-advancing an
        // already-drained subscriber loses nothing.
        const bool losesRecords = newOrder > oldOrder;
        const auto newState = losesRecords
            ? NKikimrSchemeShard::TSchemeChangeSubscriberState::STATE_LOST
            : NKikimrSchemeShard::TSchemeChangeSubscriberState::STATE_READY;

        db.Table<Schema::SchemeChangeSubscribers>().Key(subscriberId).Update(
            NIceDb::TUpdate<Schema::SchemeChangeSubscribers::LastAckedOrder>(newOrder),
            NIceDb::TUpdate<Schema::SchemeChangeSubscribers::State>(newState),
            NIceDb::TUpdate<Schema::SchemeChangeSubscribers::LastActivityAtUs>(now.MicroSeconds())
        );

        if (auto it = Self->Subscribers.find(subscriberId); it != Self->Subscribers.end()) {
            it->second.LastAckedOrder = newOrder;
            it->second.State = newState;
            it->second.LastActivityAt = now;
        }

        if (!Self->DeleteAckedSchemeChangeRecords(db, Self->GetMinSubscriberOrder(ctx.Now()),
                Self->SchemeChangeCleanupBatchSize, HasMoreToCleanup)) {
            return false;
        }

        Result->Record.SetStatus(NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);
        Result->Record.SetLastAckedOrder(newOrder);

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        Self->UpdateSchemeChangeGauges();
        // Empty when driven from the monitoring page, which replies directly.
        if (ReplyTo) {
            ctx.Send(ReplyTo, Result.Release());
        }
        if (HasMoreToCleanup) {
            Self->EnqueueSchemeChangeRecordsCleanup(ctx);
        }
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxSchemeChangeRecordsCleanup() {
    return new TTxSchemeChangeRecordsCleanup(this);
}

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxForceAdvanceSubscriberFromMonitoring(
    const TString& subscriberId, TActorId replyTo)
{
    return new TTxForceAdvanceSubscriber(this, subscriberId, replyTo);
}

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxForceAdvanceSubscriber(TEvSchemeShard::TEvForceAdvanceSubscriber::TPtr& ev) {
    return new TTxForceAdvanceSubscriber(this, ev);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvForceAdvanceSubscriber::TPtr& ev, const TActorContext& ctx) {
    if (RejectSchemeChangeRequestIfDisabled<TEvSchemeShard::TEvForceAdvanceSubscriberResult>(ctx, ev->Sender)) {
        return;
    }
    Execute(CreateTxForceAdvanceSubscriber(ev), ctx);
}

} // namespace NKikimr::NSchemeShard
