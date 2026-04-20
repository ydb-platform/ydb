#include "schemeshard_impl.h"

namespace NKikimr::NSchemeShard {

struct TTxSchemeChangeRecordsCleanup : public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
    bool BatchWasFull = false;

    TTxSchemeChangeRecordsCleanup(TSchemeShard* self)
        : TTransactionBase(self)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);

        auto subRowset = db.Table<Schema::SchemeChangeSubscribers>().Range().Select();
        if (!subRowset.IsReady()) {
            return false;
        }

        ui64 minCursor = Max<ui64>();
        bool hasSubscribers = false;

        while (!subRowset.EndOfSet()) {
            ui64 cursor = subRowset.GetValue<Schema::SchemeChangeSubscribers::LastAckedSequenceId>();
            hasSubscribers = true;
            minCursor = Min(minCursor, cursor);

            if (!subRowset.Next()) {
                return false;
            }
        }

        if (!hasSubscribers) {
            minCursor = Self->NextSchemeChangeSequenceId;
        }

        if (minCursor == 0 || minCursor == Max<ui64>()) {
            return true;
        }

        ui64 deletedCount = 0;
        const ui64 maxDeletesPerBatch = 10000;

        auto logRowset = db.Table<Schema::SchemeChangeRecords>().Range().Select();
        if (!logRowset.IsReady()) {
            return false;
        }

        while (!logRowset.EndOfSet()) {
            ui64 seqId = logRowset.GetValue<Schema::SchemeChangeRecords::SequenceId>();
            if (seqId > minCursor) {
                break;
            }
            if (deletedCount >= maxDeletesPerBatch) {
                BatchWasFull = true;
                break;
            }

            db.Table<Schema::SchemeChangeRecords>().Key(seqId).Delete();
            db.Table<Schema::SchemeChangeRecordDetails>().Key(seqId).Delete();
            ++deletedCount;

            if (!logRowset.Next()) {
                return false;
            }
        }

        if (deletedCount > 0 && Self->SchemeChangeRecordCount >= deletedCount) {
            Self->SchemeChangeRecordCount -= deletedCount;
            Self->PersistUpdateSchemeChangeRecordCount(db);
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (BatchWasFull) {
            ctx.Schedule(TDuration::Seconds(5),
                new TEvSchemeShard::TEvWakeupToRunSchemeChangeRecordsCleanup());
        } else {
            Self->ScheduleSchemeChangeRecordsCleanup(ctx);
        }
    }
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
        TString subscriberId = record.GetSubscriberId();

        NIceDb::TNiceDb db(txc.DB);

        auto rowset = db.Table<Schema::SchemeChangeSubscribers>().Key(subscriberId).Select();
        if (!rowset.IsReady()) {
            return false;
        }

        if (!rowset.IsValid()) {
            Result->Record.SetStatus(NKikimrScheme::StatusPathDoesNotExist);
            Result->Record.SetReason("Subscriber not registered: " + subscriberId);
            return true;
        }

        ui64 newCursor = Self->NextSchemeChangeSequenceId;
        const TInstant now = TInstant::Now();

        db.Table<Schema::SchemeChangeSubscribers>().Key(subscriberId).Update(
            NIceDb::TUpdate<Schema::SchemeChangeSubscribers::LastAckedSequenceId>(newCursor),
            NIceDb::TUpdate<Schema::SchemeChangeSubscribers::LastActivityAt>(now.MicroSeconds())
        );

        if (auto it = Self->Subscribers.find(subscriberId); it != Self->Subscribers.end()) {
            it->second.LastAckedSequenceId = newCursor;
            it->second.LastActivityAt = now;
        }

        Result->Record.SetStatus(NKikimrScheme::StatusSuccess);
        Result->Record.SetNewCursor(newCursor);

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

void TSchemeShard::Handle(TEvSchemeShard::TEvWakeupToRunSchemeChangeRecordsCleanup::TPtr&, const TActorContext& ctx) {
    HandleWakeupToRunSchemeChangeRecordsCleanup(ctx);
}

void TSchemeShard::HandleWakeupToRunSchemeChangeRecordsCleanup(const TActorContext& ctx) {
    Execute(CreateTxSchemeChangeRecordsCleanup(), ctx);
}

void TSchemeShard::ScheduleSchemeChangeRecordsCleanup(const TActorContext& ctx) {
    ctx.Schedule(TDuration::Hours(1), new TEvSchemeShard::TEvWakeupToRunSchemeChangeRecordsCleanup());
}

} // namespace NKikimr::NSchemeShard
