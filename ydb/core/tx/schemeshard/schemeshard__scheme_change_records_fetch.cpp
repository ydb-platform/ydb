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
        const TString& subscriberId = record.GetSubscriberId();

        NIceDb::TNiceDb db(txc.DB);

        auto rowset = db.Table<Schema::SchemeChangeSubscribers>().Key(subscriberId).Select();
        if (!rowset.IsReady()) {
            return false;
        }

        if (rowset.IsValid()) {
            const ui64 currentOrder = rowset.GetValue<Schema::SchemeChangeSubscribers::LastAckedOrder>();
            Result->Record.SetStatus(NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);
            Result->Record.SetCurrentOrder(currentOrder);
        } else {
            const TInstant now = TInstant::Now();
            db.Table<Schema::SchemeChangeSubscribers>().Key(subscriberId).Update(
                NIceDb::TUpdate<Schema::SchemeChangeSubscribers::LastAckedOrder>(0),
                NIceDb::TUpdate<Schema::SchemeChangeSubscribers::LastActivityAt>(now.MicroSeconds())
            );
            TSchemeShard::TSubscriberInfo info;
            info.LastAckedOrder = 0;
            info.LastActivityAt = now;
            Self->Subscribers.emplace(subscriberId, info);
            Result->Record.SetStatus(NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);
            Result->Record.SetCurrentOrder(0);
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
        const TString& subscriberId = record.GetSubscriberId();
        const ui64 afterOrder = record.GetAfterOrder();
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
            Result->Record.SetStatus(NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_NOT_REGISTERED);
            Result->Record.SetReason("Subscriber not registered: " + subscriberId);
            return true;
        }

        const ui64 storedOrder = subRowset.GetValue<Schema::SchemeChangeSubscribers::LastAckedOrder>();

        ui64 effectiveAfterOrder = afterOrder;
        ui64 skippedEntries = 0;
        if (storedOrder > afterOrder) {
            skippedEntries = storedOrder - afterOrder;
            effectiveAfterOrder = storedOrder;
        }

        Y_ENSURE(effectiveAfterOrder < Max<ui64>(), "effectiveAfterOrder overflow");
        auto rowset = db.Table<Schema::SchemeChangeRecords>().GreaterOrEqual(effectiveAfterOrder + 1).Select();
        if (!rowset.IsReady()) {
            return false;
        }

        ui32 count = 0;
        bool hasMore = false;

        while (!rowset.EndOfSet()) {
            if (count >= maxCount) {
                hasMore = true;
                break;
            }

            auto* entry = Result->Record.AddEntries();
            ui64 order = rowset.GetValue<Schema::SchemeChangeRecords::Order>();
            entry->SetOrder(order);
            entry->SetTxId(rowset.GetValue<Schema::SchemeChangeRecords::TxId>());
            entry->SetOperationType(rowset.GetValue<Schema::SchemeChangeRecords::OperationType>());
            auto* pathId = entry->MutablePathId();
            pathId->SetOwnerId(rowset.GetValue<Schema::SchemeChangeRecords::PathOwnerId>());
            pathId->SetLocalId(rowset.GetValue<Schema::SchemeChangeRecords::PathLocalId>());
            entry->SetPath(rowset.GetValue<Schema::SchemeChangeRecords::Path>());
            entry->SetObjectType(rowset.GetValue<Schema::SchemeChangeRecords::ObjectType>());
            entry->SetStatus(rowset.GetValue<Schema::SchemeChangeRecords::Status>());
            entry->SetUserSID(rowset.GetValue<Schema::SchemeChangeRecords::UserSID>());
            entry->SetSchemaVersion(rowset.GetValue<Schema::SchemeChangeRecords::SchemaVersion>());
            entry->SetCompletedAtUs(rowset.GetValue<Schema::SchemeChangeRecords::CompletedAtUs>());
            entry->SetPlanStep(rowset.GetValueOrDefault<Schema::SchemeChangeRecords::PlanStep>(0));
            entry->SetBodySize(rowset.GetValueOrDefault<Schema::SchemeChangeRecords::BodySize>(0));

            ++count;

            if (!rowset.Next()) {
                return false;
            }
        }

        const TInstant now = TInstant::Now();
        db.Table<Schema::SchemeChangeSubscribers>().Key(subscriberId).Update(
            NIceDb::TUpdate<Schema::SchemeChangeSubscribers::LastActivityAt>(now.MicroSeconds())
        );
        if (auto it = Self->Subscribers.find(subscriberId); it != Self->Subscribers.end()) {
            it->second.LastActivityAt = now;
        }

        Result->Record.SetStatus(NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);
        Result->Record.SetHasMore(hasMore);
        Result->Record.SetSkippedEntries(skippedEntries);

        ui64 watermarkPlanStep = 0;
        for (const auto& [opId, txState] : Self->TxInFlight) {
            if (txState.PlanStep != InvalidStepId
                && txState.State != TTxState::Done
                && txState.State != TTxState::Aborted) {
                ui64 ps = ui64(txState.PlanStep.GetValue());
                if (watermarkPlanStep == 0 || ps < watermarkPlanStep) {
                    watermarkPlanStep = ps;
                }
            }
        }
        Result->Record.SetWatermarkPlanStep(watermarkPlanStep);

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
        const TString& subscriberId = record.GetSubscriberId();
        const ui64 upToOrder = record.GetUpToOrder();

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

        const ui64 currentOrder = rowset.GetValue<Schema::SchemeChangeSubscribers::LastAckedOrder>();

        ui64 newOrder = Max(currentOrder, upToOrder);

        if (newOrder > Self->NextSchemeChangeOrder) {
            newOrder = Self->NextSchemeChangeOrder;
        }

        const ui64 oldMinOrder = Self->GetMinSubscriberOrder();

        const TInstant now = TInstant::Now();
        db.Table<Schema::SchemeChangeSubscribers>().Key(subscriberId).Update(
            NIceDb::TUpdate<Schema::SchemeChangeSubscribers::LastAckedOrder>(newOrder),
            NIceDb::TUpdate<Schema::SchemeChangeSubscribers::LastActivityAt>(now.MicroSeconds())
        );

        if (auto it = Self->Subscribers.find(subscriberId); it != Self->Subscribers.end()) {
            it->second.LastAckedOrder = newOrder;
            it->second.LastActivityAt = now;
        }

        // Reactive cleanup: delete records that just became stale, in the
        // same tx. Background cleanup only runs at tablet boot.
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

        db.Table<Schema::SchemeChangeSubscribers>().Key(subscriberId).Delete();
        Self->Subscribers.erase(subscriberId);

        if (!Self->DeleteAckedSchemeChangeRecords(db, oldMinOrder, Self->GetMinSubscriberOrder())) {
            return false;
        }

        Result->Record.SetStatus(NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        ctx.Send(Request->Sender, Result.Release());
    }
};

struct TTxFetchSchemeChangeRecordBodies : public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
    TEvSchemeShard::TEvFetchSchemeChangeRecordBodies::TPtr Request;
    THolder<TEvSchemeShard::TEvFetchSchemeChangeRecordBodiesResult> Result;

    TTxFetchSchemeChangeRecordBodies(TSchemeShard* self, TEvSchemeShard::TEvFetchSchemeChangeRecordBodies::TPtr& ev)
        : TTransactionBase(self)
        , Request(ev)
        , Result(MakeHolder<TEvSchemeShard::TEvFetchSchemeChangeRecordBodiesResult>())
    {}

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        const auto& record = Request->Get()->Record;
        const TString& subscriberId = record.GetSubscriberId();

        NIceDb::TNiceDb db(txc.DB);

        // Subscriber-gated: bodies are pulled only by registered subscribers.
        if (!Self->Subscribers.contains(subscriberId)) {
            Result->Record.SetStatus(NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_NOT_REGISTERED);
            Result->Record.SetReason("Subscriber not registered: " + subscriberId);
            return true;
        }

        const auto& requestedOrders = record.GetOrders();
        if (requestedOrders.empty()) {
            Result->Record.SetStatus(NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);
            return true;
        }

        THashSet<ui64> requestedSet(requestedOrders.begin(), requestedOrders.end());
        ui64 minOrder = Max<ui64>();
        ui64 maxOrder = 0;
        for (ui64 o : requestedSet) {
            minOrder = Min(minOrder, o);
            maxOrder = Max(maxOrder, o);
        }

        // Single range scan on metadata, filter by requested set.
        THashSet<ui64> metaExisting;
        {
            auto metaRowset = db.Table<Schema::SchemeChangeRecords>()
                .GreaterOrEqual(minOrder)
                .LessOrEqual(maxOrder)
                .Select();
            if (!metaRowset.IsReady()) {
                return false;
            }
            while (!metaRowset.EndOfSet()) {
                ui64 order = metaRowset.GetValue<Schema::SchemeChangeRecords::Order>();
                if (requestedSet.contains(order)) {
                    metaExisting.insert(order);
                }
                if (!metaRowset.Next()) {
                    return false;
                }
            }
        }

        // Single range scan on bodies, filter by existing set.
        THashMap<ui64, TString> bodyByOrder;
        if (!metaExisting.empty()) {
            auto bodyRowset = db.Table<Schema::SchemeChangeRecordDetails>()
                .GreaterOrEqual(minOrder)
                .LessOrEqual(maxOrder)
                .Select();
            if (!bodyRowset.IsReady()) {
                return false;
            }
            while (!bodyRowset.EndOfSet()) {
                ui64 order = bodyRowset.GetValue<Schema::SchemeChangeRecordDetails::Order>();
                if (metaExisting.contains(order)) {
                    bodyByOrder.emplace(order, bodyRowset.GetValue<Schema::SchemeChangeRecordDetails::Body>());
                }
                if (!bodyRowset.Next()) {
                    return false;
                }
            }
        }

        // Emit one entry per requested occurrence (preserving order and duplicates)
        // for orders whose metadata exists. Body is empty when details row is absent.
        for (ui64 order : requestedOrders) {
            if (!metaExisting.contains(order)) {
                continue;
            }
            auto* entry = Result->Record.AddEntries();
            entry->SetOrder(order);
            auto it = bodyByOrder.find(order);
            if (it != bodyByOrder.end()) {
                entry->SetBody(it->second);
            }
        }

        Result->Record.SetStatus(NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        ctx.Send(Request->Sender, Result.Release());
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

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxFetchSchemeChangeRecordBodies(TEvSchemeShard::TEvFetchSchemeChangeRecordBodies::TPtr& ev) {
    return new TTxFetchSchemeChangeRecordBodies(this, ev);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvFetchSchemeChangeRecordBodies::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxFetchSchemeChangeRecordBodies(ev), ctx);
}

} // namespace NKikimr::NSchemeShard
