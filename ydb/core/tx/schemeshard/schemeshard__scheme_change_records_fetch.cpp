#include "schemeshard_impl.h"

namespace NKikimr::NSchemeShard {

struct TTxRegisterSubscriber : public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
    TEvSchemeShard::TEvRegisterSubscriber::TPtr Request;
    THolder<TEvSchemeShard::TEvRegisterSubscriberResult> Result;

    TTxRegisterSubscriber(TSchemeShard* self, TEvSchemeShard::TEvRegisterSubscriber::TPtr& ev)
        : TTransactionBase(self)
        , Request(ev)
        , Result(MakeHolder<TEvSchemeShard::TEvRegisterSubscriberResult>())
    {}

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        const auto& record = Request->Get()->Record;
        TString subscriberId = record.GetSubscriberId();

        NIceDb::TNiceDb db(txc.DB);

        auto rowset = db.Table<Schema::SchemeChangeSubscribers>().Key(subscriberId).Select();
        if (!rowset.IsReady()) {
            return false;
        }

        if (rowset.IsValid()) {
            ui64 currentCursor = rowset.GetValue<Schema::SchemeChangeSubscribers::LastAckedSequenceId>();
            Result->Record.SetStatus(NKikimrScheme::StatusSuccess);
            Result->Record.SetCurrentSequenceId(currentCursor);
        } else {
            db.Table<Schema::SchemeChangeSubscribers>().Key(subscriberId).Update(
                NIceDb::TUpdate<Schema::SchemeChangeSubscribers::LastAckedSequenceId>(0),
                NIceDb::TUpdate<Schema::SchemeChangeSubscribers::LastActivityAt>(TInstant::Now().MicroSeconds())
            );
            Self->HasSchemeChangeSubscribers = true;
            Result->Record.SetStatus(NKikimrScheme::StatusSuccess);
            Result->Record.SetCurrentSequenceId(0);
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        ctx.Send(Request->Sender, Result.Release());
    }
};

struct TTxFetchSchemeChangeRecords : public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
    TEvSchemeShard::TEvFetchSchemeChangeRecords::TPtr Request;
    THolder<TEvSchemeShard::TEvFetchSchemeChangeRecordsResult> Result;

    TTxFetchSchemeChangeRecords(TSchemeShard* self, TEvSchemeShard::TEvFetchSchemeChangeRecords::TPtr& ev)
        : TTransactionBase(self)
        , Request(ev)
        , Result(MakeHolder<TEvSchemeShard::TEvFetchSchemeChangeRecordsResult>())
    {}

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        const auto& record = Request->Get()->Record;
        TString subscriberId = record.GetSubscriberId();
        ui64 afterSeqId = record.GetAfterSequenceId();
        ui32 maxCount = record.GetMaxCount();

        if (maxCount == 0 || maxCount > 1000) {
            maxCount = 1000;
        }

        NIceDb::TNiceDb db(txc.DB);

        auto subRowset = db.Table<Schema::SchemeChangeSubscribers>().Key(subscriberId).Select();
        if (!subRowset.IsReady()) {
            return false;
        }

        if (!subRowset.IsValid()) {
            Result->Record.SetStatus(NKikimrScheme::StatusPathDoesNotExist);
            Result->Record.SetReason("Subscriber not registered: " + subscriberId);
            return true;
        }

        ui64 storedCursor = subRowset.GetValue<Schema::SchemeChangeSubscribers::LastAckedSequenceId>();

        ui64 effectiveAfterSeqId = afterSeqId;
        ui64 skippedEntries = 0;
        if (storedCursor > afterSeqId) {
            skippedEntries = storedCursor - afterSeqId;
            effectiveAfterSeqId = storedCursor;
        }

        Y_ENSURE(effectiveAfterSeqId < Max<ui64>(), "effectiveAfterSeqId overflow");
        auto rowset = db.Table<Schema::SchemeChangeRecords>().GreaterOrEqual(effectiveAfterSeqId + 1).Select();
        if (!rowset.IsReady()) {
            return false;
        }

        ui32 count = 0;
        ui64 lastSeqId = effectiveAfterSeqId;
        bool hasMore = false;

        while (!rowset.EndOfSet()) {
            if (count >= maxCount) {
                hasMore = true;
                break;
            }

            auto* entry = Result->Record.AddEntries();
            ui64 seqId = rowset.GetValue<Schema::SchemeChangeRecords::SequenceId>();
            entry->SetSequenceId(seqId);
            entry->SetTxId(rowset.GetValue<Schema::SchemeChangeRecords::TxId>());
            entry->SetOperationType(rowset.GetValue<Schema::SchemeChangeRecords::OperationType>());
            entry->SetPathOwnerId(rowset.GetValue<Schema::SchemeChangeRecords::PathOwnerId>());
            entry->SetPathLocalId(rowset.GetValue<Schema::SchemeChangeRecords::PathLocalId>());
            entry->SetPathName(rowset.GetValue<Schema::SchemeChangeRecords::PathName>());
            entry->SetObjectType(rowset.GetValue<Schema::SchemeChangeRecords::ObjectType>());
            entry->SetStatus(rowset.GetValue<Schema::SchemeChangeRecords::Status>());
            entry->SetUserSID(rowset.GetValue<Schema::SchemeChangeRecords::UserSID>());
            entry->SetSchemaVersion(rowset.GetValue<Schema::SchemeChangeRecords::SchemaVersion>());
            entry->SetCompletedAt(rowset.GetValue<Schema::SchemeChangeRecords::CompletedAt>());
            entry->SetPlanStep(rowset.GetValueOrDefault<Schema::SchemeChangeRecords::PlanStep>(0));

            lastSeqId = seqId;
            ++count;

            if (!rowset.Next()) {
                return false;
            }
        }

        db.Table<Schema::SchemeChangeSubscribers>().Key(subscriberId).Update(
            NIceDb::TUpdate<Schema::SchemeChangeSubscribers::LastActivityAt>(TInstant::Now().MicroSeconds())
        );

        Result->Record.SetStatus(NKikimrScheme::StatusSuccess);
        Result->Record.SetLastSequenceId(lastSeqId);
        Result->Record.SetHasMore(hasMore);
        Result->Record.SetSkippedEntries(skippedEntries);

        ui64 minInFlightPlanStep = 0;
        for (const auto& [opId, txState] : Self->TxInFlight) {
            if (txState.PlanStep != InvalidStepId
                && txState.State != TTxState::Done
                && txState.State != TTxState::Aborted) {
                ui64 ps = ui64(txState.PlanStep.GetValue());
                if (minInFlightPlanStep == 0 || ps < minInFlightPlanStep) {
                    minInFlightPlanStep = ps;
                }
            }
        }
        Result->Record.SetMinInFlightPlanStep(minInFlightPlanStep);

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        ctx.Send(Request->Sender, Result.Release());
    }
};

struct TTxAckSchemeChangeRecords : public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
    TEvSchemeShard::TEvAckSchemeChangeRecords::TPtr Request;
    THolder<TEvSchemeShard::TEvAckSchemeChangeRecordsResult> Result;

    TTxAckSchemeChangeRecords(TSchemeShard* self, TEvSchemeShard::TEvAckSchemeChangeRecords::TPtr& ev)
        : TTransactionBase(self)
        , Request(ev)
        , Result(MakeHolder<TEvSchemeShard::TEvAckSchemeChangeRecordsResult>())
    {}

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        const auto& record = Request->Get()->Record;
        TString subscriberId = record.GetSubscriberId();
        ui64 upToSeqId = record.GetUpToSequenceId();

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

        ui64 currentCursor = rowset.GetValue<Schema::SchemeChangeSubscribers::LastAckedSequenceId>();

        ui64 newCursor = Max(currentCursor, upToSeqId);

        if (newCursor > Self->NextSchemeChangeSequenceId) {
            newCursor = Self->NextSchemeChangeSequenceId;
        }

        db.Table<Schema::SchemeChangeSubscribers>().Key(subscriberId).Update(
            NIceDb::TUpdate<Schema::SchemeChangeSubscribers::LastAckedSequenceId>(newCursor),
            NIceDb::TUpdate<Schema::SchemeChangeSubscribers::LastActivityAt>(TInstant::Now().MicroSeconds())
        );

        Result->Record.SetStatus(NKikimrScheme::StatusSuccess);
        Result->Record.SetNewCursor(newCursor);

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        ctx.Send(Request->Sender, Result.Release());
        ctx.Schedule(TDuration::Seconds(5),
            new TEvSchemeShard::TEvWakeupToRunSchemeChangeRecordsCleanup());
    }
};

struct TTxUnregisterSubscriber : public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
    TEvSchemeShard::TEvUnregisterSubscriber::TPtr Request;
    THolder<TEvSchemeShard::TEvUnregisterSubscriberResult> Result;

    TTxUnregisterSubscriber(TSchemeShard* self, TEvSchemeShard::TEvUnregisterSubscriber::TPtr& ev)
        : TTransactionBase(self)
        , Request(ev)
        , Result(MakeHolder<TEvSchemeShard::TEvUnregisterSubscriberResult>())
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

        db.Table<Schema::SchemeChangeSubscribers>().Key(subscriberId).Delete();

        // Check if any subscribers remain
        auto subRowset = db.Table<Schema::SchemeChangeSubscribers>().Range().Select();
        if (!subRowset.IsReady()) {
            return false;
        }

        bool hasRemaining = false;
        while (!subRowset.EndOfSet()) {
            TString id = subRowset.GetValue<Schema::SchemeChangeSubscribers::SubscriberId>();
            if (id != subscriberId) {
                hasRemaining = true;
                break;
            }
            if (!subRowset.Next()) {
                return false;
            }
        }

        Self->HasSchemeChangeSubscribers = hasRemaining;

        Result->Record.SetStatus(NKikimrScheme::StatusSuccess);

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        ctx.Send(Request->Sender, Result.Release());
        ctx.Schedule(TDuration::Seconds(5),
            new TEvSchemeShard::TEvWakeupToRunSchemeChangeRecordsCleanup());
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxUnregisterSubscriber(TEvSchemeShard::TEvUnregisterSubscriber::TPtr& ev) {
    return new TTxUnregisterSubscriber(this, ev);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvUnregisterSubscriber::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxUnregisterSubscriber(ev), ctx);
}

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxRegisterSubscriber(TEvSchemeShard::TEvRegisterSubscriber::TPtr& ev) {
    return new TTxRegisterSubscriber(this, ev);
}

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxFetchSchemeChangeRecords(TEvSchemeShard::TEvFetchSchemeChangeRecords::TPtr& ev) {
    return new TTxFetchSchemeChangeRecords(this, ev);
}

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxAckSchemeChangeRecords(TEvSchemeShard::TEvAckSchemeChangeRecords::TPtr& ev) {
    return new TTxAckSchemeChangeRecords(this, ev);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvRegisterSubscriber::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxRegisterSubscriber(ev), ctx);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvFetchSchemeChangeRecords::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxFetchSchemeChangeRecords(ev), ctx);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvAckSchemeChangeRecords::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxAckSchemeChangeRecords(ev), ctx);
}

} // namespace NKikimr::NSchemeShard
