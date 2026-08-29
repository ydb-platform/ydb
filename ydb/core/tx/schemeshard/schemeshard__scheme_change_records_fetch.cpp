#include "schemeshard_impl.h"

#include <ydb/core/base/appdata.h>

#include <util/string/split.h>

namespace NKikimr::NSchemeShard {

namespace {

// An empty SubscriberId is dangerous: two consumers that both leave it unset
// silently share one cursor, so one's ack deletes records the other never saw.
template <class TResultRecord>
bool CheckSubscriberId(const TString& subscriberId, TResultRecord& result) {
    if (subscriberId.empty()) {
        result.SetStatus(NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_INVALID_REQUEST);
        result.SetReason("SubscriberId must not be empty");
        return false;
    }
    if (subscriberId.size() > TSchemeShard::MaxSchemeChangeSubscriberIdLength) {
        result.SetStatus(NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_INVALID_REQUEST);
        result.SetReason(TStringBuilder() << "SubscriberId exceeds "
            << TSchemeShard::MaxSchemeChangeSubscriberIdLength << " bytes");
        return false;
    }
    return true;
}

} // namespace

struct TTxRegisterSubscriber : public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
    TEvSchemeShard::TEvRegisterSubscriber::TPtr Request;
    THolder<TEvSchemeShard::TEvRegisterSubscriberResult> Result;

    TTxRegisterSubscriber(TSchemeShard* self, TEvSchemeShard::TEvRegisterSubscriber::TPtr& ev)
        : TTransactionBase(self)
        , Request(ev)
        , Result(MakeHolder<TEvSchemeShard::TEvRegisterSubscriberResult>())
    {}

    TTxType GetTxType() const override { return TXTYPE_REGISTER_SCHEME_CHANGE_SUBSCRIBER; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        const auto& record = Request->Get()->Record;
        const TString& subscriberId = record.GetSubscriberId();

        if (!AppData()->FeatureFlags.GetEnableSchemeChangeRecords()) {
            Result->Record.SetStatus(NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_INVALID_REQUEST);
            Result->Record.SetReason("Scheme change records are disabled");
            return true;
        }

        if (!CheckSubscriberId(subscriberId, Result->Record)) {
            return true;
        }

        // A plain (non-Ext) SubDomain can serve several coordinator timelines,
        // which would let a Bucketed record borrow a step from a foreign domain.
        if (Self->CountTransactionSupportingDomains() > 1) {
            Result->Record.SetStatus(NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_INVALID_REQUEST);
            Result->Record.SetReason(
                "Scheme change subscribers require a SchemeShard serving a single"
                " transaction-supporting domain; use an external subdomain");
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);

        auto rowset = db.Table<Schema::SchemeChangeSubscribers>().Key(subscriberId).Select();
        if (!rowset.IsReady()) {
            return false;
        }

        if (rowset.IsValid()) {
            const ui64 currentOrder = rowset.GetValue<Schema::SchemeChangeSubscribers::LastAckedOrder>();
            Result->Record.SetStatus(NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);
            Result->Record.SetCurrentOrder(currentOrder);
            auto it = Self->Subscribers.find(subscriberId);
            Result->Record.SetState(static_cast<NKikimrSchemeShard::TSchemeChangeSubscriberState::EState>(
                it != Self->Subscribers.end()
                    ? it->second.State
                    : NKikimrSchemeShard::TSchemeChangeSubscriberState::STATE_READY));
            return true;
        }

        // Cap only new ids, so re-registration after a restart stays idempotent.
        if (Self->Subscribers.size() >= Self->MaxSchemeChangeSubscribers) {
            Result->Record.SetStatus(NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_INVALID_REQUEST);
            Result->Record.SetReason(TStringBuilder()
                << "Too many scheme change subscribers (limit: "
                << Self->MaxSchemeChangeSubscribers << ")");
            return true;
        }

        // Use the visible tail, not the raw allocation counter: a subscriber
        // must start below rows already reserved by an in-flight DDL.
        const ui64 tail = Self->GetVisibleSchemeChangeTail();

        const ui64 floor = Self->GetMinSubscriberOrder(ctx.Now());

        ui64 startOrder = tail;
        ui64 skipped = 0;
        auto state = NKikimrSchemeShard::TSchemeChangeSubscriberState::STATE_READY;

        if (record.HasStartOrder()) {
            const ui64 requested = record.GetStartOrder();
            if (requested > tail) {
                Result->Record.SetStatus(NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_INVALID_REQUEST);
                Result->Record.SetReason(TStringBuilder()
                    << "StartOrder " << requested << " is beyond the log tail " << tail);
                return true;
            }
            if (requested < floor) {
                // Clamp up rather than reject: a new subscriber must never
                // widen the unacked window and stall DDL.
                startOrder = floor;
                skipped = floor - requested;
                state = NKikimrSchemeShard::TSchemeChangeSubscriberState::STATE_LOST;
            } else {
                startOrder = requested;
            }
        }

        const TInstant now = ctx.Now();
        db.Table<Schema::SchemeChangeSubscribers>().Key(subscriberId).Update(
            NIceDb::TUpdate<Schema::SchemeChangeSubscribers::LastAckedOrder>(startOrder),
            NIceDb::TUpdate<Schema::SchemeChangeSubscribers::LastActivityAtUs>(now.MicroSeconds()),
            NIceDb::TUpdate<Schema::SchemeChangeSubscribers::State>(state),
            NIceDb::TUpdate<Schema::SchemeChangeSubscribers::StartOrder>(startOrder)
        );

        TSchemeShard::TSubscriberInfo info;
        info.LastAckedOrder = startOrder;
        info.LastActivityAt = now;
        info.State = state;
        info.StartOrder = startOrder;
        Self->Subscribers.emplace(subscriberId, info);

        Result->Record.SetStatus(NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);
        Result->Record.SetCurrentOrder(startOrder);
        Result->Record.SetState(state);
        Result->Record.SetSkippedEntries(skipped);

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        Self->UpdateSchemeChangeGauges();
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

    TTxType GetTxType() const override { return TXTYPE_FETCH_SCHEME_CHANGE_RECORDS; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        const auto& record = Request->Get()->Record;
        const TString& subscriberId = record.GetSubscriberId();
        const ui64 afterOrder = record.GetAfterOrder();
        ui32 maxCount = record.GetMaxCount();

        if (!AppData()->FeatureFlags.GetEnableSchemeChangeRecords()) {
            Result->Record.SetStatus(NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_INVALID_REQUEST);
            Result->Record.SetReason("Scheme change records are disabled");
            return true;
        }

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
        bool subscriberLost = false;
        if (storedOrder > afterOrder) {
            skippedEntries = storedOrder - afterOrder;
            effectiveAfterOrder = storedOrder;
        }

        Y_ENSURE(effectiveAfterOrder < Max<ui64>(), "effectiveAfterOrder overflow");
        // Bound both ends: an unbounded GreaterOrEqual().Select() would precharge the
        // whole tail. No order exceeds NextSchemeChangeOrder, so no real row is excluded.
        auto rowset = db.Table<Schema::SchemeChangeRecords>()
            .GreaterOrEqual(effectiveAfterOrder + 1)
            .LessOrEqual(Self->NextSchemeChangeOrder)
            .Select();
        if (!rowset.IsReady()) {
            return false;
        }

        ui32 count = 0;
        bool hasMore = false;
        bool firstRow = true;
        bool blockedByPending = false;

        while (!rowset.EndOfSet()) {
            if (count >= maxCount) {
                hasMore = true;
                break;
            }

            Self->TabletCounters->Cumulative()[COUNTER_SCHEME_CHANGE_ROWS_SCANNED].Increment(1);
            ui64 order = rowset.GetValue<Schema::SchemeChangeRecords::Order>();

            // Stop, don't skip, at the first in-flight row: the cursor is one watermark,
            // so handing out a later record would let an ack skip past this one.
            if (rowset.GetValueOrDefault<Schema::SchemeChangeRecords::CompletedAtUs>(0) == 0) {
                blockedByPending = true;
                break;
            }

            auto* entry = Result->Record.AddEntries();

            // A first row above effectiveAfterOrder+1 means records in between
            // were swept while this subscriber still needed them; report it.
            if (firstRow) {
                firstRow = false;
                if (order > effectiveAfterOrder + 1) {
                    skippedEntries += order - 1 - effectiveAfterOrder;
                    subscriberLost = true;
                }
            }
            entry->SetOrder(order);
            entry->SetTxId(rowset.GetValue<Schema::SchemeChangeRecords::TxId>());
            entry->SetOperationType(rowset.GetValue<Schema::SchemeChangeRecords::OperationType>());
            auto* pathId = entry->MutablePathId();
            pathId->SetOwnerId(rowset.GetValue<Schema::SchemeChangeRecords::PathOwnerId>());
            pathId->SetLocalId(rowset.GetValue<Schema::SchemeChangeRecords::PathLocalId>());
            for (const auto& t : TSchemeShard::DecodeSchemeChangeTargets(rowset.GetValue<Schema::SchemeChangeRecords::Path>())) {
                auto* target = entry->AddTargets();
                target->SetPath(t.Path);
                for (const auto& src : t.SourcePaths) {
                    target->AddSourcePaths(src);
                }
            }
            entry->SetObjectType(rowset.GetValue<Schema::SchemeChangeRecords::ObjectType>());
            entry->SetStatus(rowset.GetValue<Schema::SchemeChangeRecords::Status>());
            entry->SetUserSID(rowset.GetValue<Schema::SchemeChangeRecords::UserSID>());
            entry->SetSchemaVersion(rowset.GetValue<Schema::SchemeChangeRecords::SchemaVersion>());
            entry->SetCompletedAtUs(rowset.GetValue<Schema::SchemeChangeRecords::CompletedAtUs>());
            entry->SetPlanStep(rowset.GetValueOrDefault<Schema::SchemeChangeRecords::PlanStep>(0));
            entry->SetBodySizeBytes(rowset.GetValueOrDefault<Schema::SchemeChangeRecords::BodySizeBytes>(0));
            entry->SetPositionKind(static_cast<NKikimrSchemeShard::TSchemeChangePosition::EKind>(
                rowset.GetValueOrDefault<Schema::SchemeChangeRecords::PositionKind>(
                    NKikimrSchemeShard::TSchemeChangePosition::KIND_EXACT)));
            {
                const TString redacted = rowset.GetValueOrDefault<Schema::SchemeChangeRecords::RedactedFields>(TString());
                if (!redacted.empty()) {
                    for (const auto& field : StringSplitter(redacted).Split('\n')) {
                        entry->AddRedactedFields(TString(field.Token()));
                    }
                }
            }

            ++count;

            if (!rowset.Next()) {
                return false;
            }
        }

        // Total-loss case: the loop above never ran, excluding blockedByPending
        // since an in-flight record is a not-yet, not a loss.
        if (count == 0 && !blockedByPending && effectiveAfterOrder < Self->GetVisibleSchemeChangeTail()) {
            skippedEntries += Self->GetVisibleSchemeChangeTail() - effectiveAfterOrder;
            subscriberLost = true;
            // Advance to the tail, or the same loss re-reports on every fetch.
            effectiveAfterOrder = Self->GetVisibleSchemeChangeTail();
        }

        const TInstant now = ctx.Now();
        if (subscriberLost) {
            db.Table<Schema::SchemeChangeSubscribers>().Key(subscriberId).Update(
                NIceDb::TUpdate<Schema::SchemeChangeSubscribers::LastAckedOrder>(effectiveAfterOrder),
                NIceDb::TUpdate<Schema::SchemeChangeSubscribers::LastActivityAtUs>(now.MicroSeconds()),
                NIceDb::TUpdate<Schema::SchemeChangeSubscribers::State>(
                    NKikimrSchemeShard::TSchemeChangeSubscriberState::STATE_LOST)
            );
        } else {
            db.Table<Schema::SchemeChangeSubscribers>().Key(subscriberId).Update(
                NIceDb::TUpdate<Schema::SchemeChangeSubscribers::LastActivityAtUs>(now.MicroSeconds())
            );
        }
        if (auto it = Self->Subscribers.find(subscriberId); it != Self->Subscribers.end()) {
            it->second.LastActivityAt = now;
            if (subscriberLost) {
                it->second.State = NKikimrSchemeShard::TSchemeChangeSubscriberState::STATE_LOST;
                it->second.LastAckedOrder = effectiveAfterOrder;
            }
        }

        Result->Record.SetStatus(NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);
        Result->Record.SetHasMore(hasMore);
        Result->Record.SetSkippedEntries(skippedEntries);
        if (auto it = Self->Subscribers.find(subscriberId); it != Self->Subscribers.end()) {
            Result->Record.SetState(
                static_cast<NKikimrSchemeShard::TSchemeChangeSubscriberState::EState>(it->second.State));
        }

        // Order is completion order while plan step is plan order, and they can
        // diverge, so a consumer needs this to know window (S_prev, S] is complete.
        Result->Record.SetClosedThroughPlanStep(Self->GetClosedThroughPlanStep());

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        Self->UpdateSchemeChangeGauges();
        ctx.Send(Request->Sender, Result.Release());
    }
};

struct TTxAckSchemeChangeRecords : public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
    TEvSchemeShard::TEvAckSchemeChangeRecords::TPtr Request;
    THolder<TEvSchemeShard::TEvAckSchemeChangeRecordsResult> Result;
    bool HasMoreToCleanup = false;

    TTxAckSchemeChangeRecords(TSchemeShard* self, TEvSchemeShard::TEvAckSchemeChangeRecords::TPtr& ev)
        : TTransactionBase(self)
        , Request(ev)
        , Result(MakeHolder<TEvSchemeShard::TEvAckSchemeChangeRecordsResult>())
    {}

    TTxType GetTxType() const override { return TXTYPE_ACK_SCHEME_CHANGE_RECORDS; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        HasMoreToCleanup = false;
        const auto& record = Request->Get()->Record;
        const TString& subscriberId = record.GetSubscriberId();
        const ui64 upToOrder = record.GetUpToOrder();

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

        const ui64 currentOrder = rowset.GetValue<Schema::SchemeChangeSubscribers::LastAckedOrder>();

        ui64 newOrder = Max(currentOrder, upToOrder);

        // Clamp to the visible tail, not the reserved one: a cursor above an
        // in-flight row would drop that record once it finalises.
        if (newOrder > Self->GetVisibleSchemeChangeTail()) {
            newOrder = Self->GetVisibleSchemeChangeTail();
        }

        const TInstant now = ctx.Now();
        db.Table<Schema::SchemeChangeSubscribers>().Key(subscriberId).Update(
            NIceDb::TUpdate<Schema::SchemeChangeSubscribers::LastAckedOrder>(newOrder),
            NIceDb::TUpdate<Schema::SchemeChangeSubscribers::LastActivityAtUs>(now.MicroSeconds())
        );

        if (auto it = Self->Subscribers.find(subscriberId); it != Self->Subscribers.end()) {
            it->second.LastAckedOrder = newOrder;
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
        ctx.Send(Request->Sender, Result.Release());
        if (HasMoreToCleanup) {
            Self->EnqueueSchemeChangeRecordsCleanup(ctx);
        }
    }
};

struct TTxUnregisterSubscriber : public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
    TEvSchemeShard::TEvUnregisterSubscriber::TPtr Request;
    THolder<TEvSchemeShard::TEvUnregisterSubscriberResult> Result;
    bool HasMoreToCleanup = false;

    TTxUnregisterSubscriber(TSchemeShard* self, TEvSchemeShard::TEvUnregisterSubscriber::TPtr& ev)
        : TTransactionBase(self)
        , Request(ev)
        , Result(MakeHolder<TEvSchemeShard::TEvUnregisterSubscriberResult>())
    {}

    TTxType GetTxType() const override { return TXTYPE_UNREGISTER_SCHEME_CHANGE_SUBSCRIBER; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        HasMoreToCleanup = false;
        const auto& record = Request->Get()->Record;
        const TString& subscriberId = record.GetSubscriberId();

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

        db.Table<Schema::SchemeChangeSubscribers>().Key(subscriberId).Delete();
        Self->Subscribers.erase(subscriberId);

        if (!Self->DeleteAckedSchemeChangeRecords(db, Self->GetMinSubscriberOrder(ctx.Now()),
                Self->SchemeChangeCleanupBatchSize, HasMoreToCleanup)) {
            return false;
        }

        Result->Record.SetStatus(NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        Self->UpdateSchemeChangeGauges();
        ctx.Send(Request->Sender, Result.Release());
        if (HasMoreToCleanup) {
            Self->EnqueueSchemeChangeRecordsCleanup(ctx);
        }
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

    TTxType GetTxType() const override { return TXTYPE_FETCH_SCHEME_CHANGE_RECORD_BODIES; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        const auto& record = Request->Get()->Record;
        const TString& subscriberId = record.GetSubscriberId();

        if (!AppData()->FeatureFlags.GetEnableSchemeChangeRecords()) {
            Result->Record.SetStatus(NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_INVALID_REQUEST);
            Result->Record.SetReason("Scheme change records are disabled");
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);

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

        // Reject rather than truncate: a truncated reply is indistinguishable
        // from "those records no longer exist".
        if ((ui64)requestedOrders.size() > Self->MaxSchemeChangeBodiesPerRequest) {
            Result->Record.SetStatus(NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_INVALID_REQUEST);
            Result->Record.SetReason(TStringBuilder()
                << "Too many orders requested: " << requestedOrders.size()
                << ", max " << Self->MaxSchemeChangeBodiesPerRequest);
            return true;
        }

        THashSet<ui64> requestedSet(requestedOrders.begin(), requestedOrders.end());

        // Point lookups, not a [min,max] span scan: cost is |requestedSet|.
        THashMap<ui64, TString> descriptionByOrder;
        THashSet<ui64> metaExisting;
        bool allReady = true;
        for (ui64 order : requestedSet) {
            Self->TabletCounters->Cumulative()[COUNTER_SCHEME_CHANGE_ROWS_SCANNED].Increment(1);
            auto row = db.Table<Schema::SchemeChangeRecords>().Key(order).Select();
            if (!row.IsReady()) {
                allReady = false;
                continue;
            }
            if (!row.IsValid()) {
                continue;
            }
            metaExisting.insert(order);
            auto desc = row.GetValueOrDefault<Schema::SchemeChangeRecords::Description>(TString());
            if (!desc.empty()) {
                descriptionByOrder.emplace(order, std::move(desc));
            }
        }

        THashMap<ui64, TString> bodyByOrder;
        for (ui64 order : metaExisting) {
            Self->TabletCounters->Cumulative()[COUNTER_SCHEME_CHANGE_ROWS_SCANNED].Increment(1);
            auto row = db.Table<Schema::SchemeChangeRecordDetails>().Key(order).Select();
            if (!row.IsReady()) {
                allReady = false;
                continue;
            }
            if (!row.IsValid()) {
                continue;
            }
            bodyByOrder.emplace(order, row.GetValue<Schema::SchemeChangeRecordDetails::Body>());
        }

        // Nothing written to Result yet, so restart here is clean.
        if (!allReady) {
            return false;
        }

        // Iterate requestedOrders (not metaExisting) to preserve request order and duplicates.
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
            auto descIt = descriptionByOrder.find(order);
            if (descIt != descriptionByOrder.end()) {
                entry->SetDescription(descIt->second);
            }
        }

        Result->Record.SetStatus(NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        Self->UpdateSchemeChangeGauges();
        ctx.Send(Request->Sender, Result.Release());
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxUnregisterSubscriber(TEvSchemeShard::TEvUnregisterSubscriber::TPtr& ev) {
    return new TTxUnregisterSubscriber(this, ev);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvUnregisterSubscriber::TPtr& ev, const TActorContext& ctx) {
    if (RejectSchemeChangeRequestIfDisabled<TEvSchemeShard::TEvUnregisterSubscriberResult>(ctx, ev->Sender)) {
        return;
    }
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
    if (RejectSchemeChangeRequestIfDisabled<TEvSchemeShard::TEvRegisterSubscriberResult>(ctx, ev->Sender)) {
        return;
    }
    Execute(CreateTxRegisterSubscriber(ev), ctx);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvFetchSchemeChangeRecords::TPtr& ev, const TActorContext& ctx) {
    if (RejectSchemeChangeRequestIfDisabled<TEvSchemeShard::TEvFetchSchemeChangeRecordsResult>(ctx, ev->Sender)) {
        return;
    }
    Execute(CreateTxFetchSchemeChangeRecords(ev), ctx);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvAckSchemeChangeRecords::TPtr& ev, const TActorContext& ctx) {
    if (RejectSchemeChangeRequestIfDisabled<TEvSchemeShard::TEvAckSchemeChangeRecordsResult>(ctx, ev->Sender)) {
        return;
    }
    Execute(CreateTxAckSchemeChangeRecords(ev), ctx);
}

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxFetchSchemeChangeRecordBodies(TEvSchemeShard::TEvFetchSchemeChangeRecordBodies::TPtr& ev) {
    return new TTxFetchSchemeChangeRecordBodies(this, ev);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvFetchSchemeChangeRecordBodies::TPtr& ev, const TActorContext& ctx) {
    if (RejectSchemeChangeRequestIfDisabled<TEvSchemeShard::TEvFetchSchemeChangeRecordBodiesResult>(ctx, ev->Sender)) {
        return;
    }
    Execute(CreateTxFetchSchemeChangeRecordBodies(ev), ctx);
}

} // namespace NKikimr::NSchemeShard
