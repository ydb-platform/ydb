#include "schemeshard_impl.h"

#include <ydb/core/base/auth.h>

namespace NKikimr::NSchemeShard {

namespace {

// Shared admission check for the mutating subscriber requests.
//
// These events arrive over a tablet pipe, which carries no caller identity, so
// the token is supplied in the request. Note that an EMPTY cluster-admin
// allowlist admits any token including none (auth.cpp IsTokenAllowedImpl), so
// on a cluster that never configured AdministrationAllowedSIDs this is a no-op
// -- which is also why the existing tokenless test helpers keep working.
template <class TResultRecord>
bool CheckSubscriberAdmin(const TString& userToken, TResultRecord& result) {
    if (IsAdministrator(AppData(), userToken)) {
        return true;
    }
    result.SetStatus(NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_ACCESS_DENIED);
    result.SetReason("Scheme change subscriber administration requires cluster admin rights");
    return false;
}

// SubscriberId is a persisted primary key and the unit of cursor ownership.
// An empty id is the dangerous case: two consumers that both leave it unset
// silently share one cursor, so one's ack deletes records the other never saw
// and SkippedEntries reports 0.
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

        if (!CheckSubscriberAdmin(record.GetUserToken(), Result->Record)
            || !CheckSubscriberId(subscriberId, Result->Record)) {
            return true;
        }

        // D7/Option A. A plain (non-Ext) SubDomain declares its own coordinators
        // as shards of THIS SchemeShard, so one tablet can serve several
        // coordinator timelines. Steps from different coordinators are
        // commensurate, but D6's borrow takes a GLOBAL max -- on a shared
        // SchemeShard a Bucketed record could borrow a step from a foreign
        // domain and fall out of its own window. Refusing registration keeps
        // the outbox single-timeline by construction; the accepted cost is that
        // such a tenant must be an external subdomain to be backed up.
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
            // Idempotent: report the existing cursor and state untouched.
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

        // Cap the subscriber count -- but only for a genuinely NEW id, so that
        // re-registration after a consumer restart stays idempotent.
        if (Self->Subscribers.size() >= Self->MaxSchemeChangeSubscribers) {
            Result->Record.SetStatus(NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_INVALID_REQUEST);
            Result->Record.SetReason(TStringBuilder()
                << "Too many scheme change subscribers (limit: "
                << Self->MaxSchemeChangeSubscribers << ")");
            return true;
        }

        // Orders are last-assigned, not next-to-assign
        // (schemeshard__scheme_change_records.cpp: `ui64 id = ++NextSchemeChangeOrder;`),
        // so the tail is that value exactly -- no +-1. The *visible* tail,
        // though: a subscriber registering while a DDL is in flight should
        // still receive that DDL's record once it finalises, so it must start
        // below the rows already reserved for it.
        const ui64 tail = Self->GetVisibleSchemeChangeTail();

        // Retention floor, evaluated BEFORE inserting the new row. Already
        // total and O(1): with no subscribers it returns the visible tail,
        // which is exactly the empty-log case. It tracks the actual deletion
        // floor rather than the physical oldest row, which lags during
        // batched cleanup.
        const ui64 floor = Self->GetMinSubscriberOrder(ctx.Now());

        ui64 startOrder = tail;
        ui64 skipped = 0;
        auto state = NKikimrSchemeShard::TSchemeChangeSubscriberState::STATE_READY;

        if (record.HasStartOrder()) {
            // TODO(rfc-0129 phase 1.6): gate this branch on admin auth.
            // Asking for history is privileged; the default (absent) form is not.
            const ui64 requested = record.GetStartOrder();
            if (requested > tail) {
                Result->Record.SetStatus(NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_INVALID_REQUEST);
                Result->Record.SetReason(TStringBuilder()
                    << "StartOrder " << requested << " is beyond the log tail " << tail);
                return true;
            }
            if (requested < floor) {
                // Clamp up rather than reject. Clamping satisfies the
                // anti-wedge invariant by construction -- the new subscriber
                // can never sit below the existing min, so it cannot widen
                // the unacked window and stall DDL. The hole is reported
                // loudly with the same contract the force-advance path uses.
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
            NIceDb::TUpdate<Schema::SchemeChangeSubscribers::LastActivityAt>(now.MicroSeconds()),
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
        auto rowset = db.Table<Schema::SchemeChangeRecords>().GreaterOrEqual(effectiveAfterOrder + 1).Select();
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

            ui64 order = rowset.GetValue<Schema::SchemeChangeRecords::Order>();

            // Stop at the first row whose operation is still in flight. Its
            // record was reserved at propose but carries neither identity nor
            // coordinator position yet, and CompletedAtUs == 0 is what says so.
            //
            // Stop rather than skip: the cursor is a single watermark, so
            // handing out a later record would let an ack advance past this one
            // and lose it. A pending row is a barrier, not a gap.
            if (rowset.GetValueOrDefault<Schema::SchemeChangeRecords::CompletedAtUs>(0) == 0) {
                blockedByPending = true;
                break;
            }

            auto* entry = Result->Record.AddEntries();

            // Physical gap detection. The scan starts at effectiveAfterOrder+1;
            // if the first surviving row sits above that, the records in
            // between were swept while this subscriber still needed them --
            // which is exactly what happens once staleness excludes it from
            // the retention floor. Report it rather than skipping silently:
            // excluding a stale subscriber is intended, losing its records
            // without telling it is not.
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
            entry->SetPath(rowset.GetValue<Schema::SchemeChangeRecords::Path>());
            entry->SetObjectType(rowset.GetValue<Schema::SchemeChangeRecords::ObjectType>());
            entry->SetStatus(rowset.GetValue<Schema::SchemeChangeRecords::Status>());
            entry->SetUserSID(rowset.GetValue<Schema::SchemeChangeRecords::UserSID>());
            entry->SetSchemaVersion(rowset.GetValue<Schema::SchemeChangeRecords::SchemaVersion>());
            entry->SetCompletedAtUs(rowset.GetValue<Schema::SchemeChangeRecords::CompletedAtUs>());
            entry->SetPlanStep(rowset.GetValueOrDefault<Schema::SchemeChangeRecords::PlanStep>(0));
            entry->SetBodySize(rowset.GetValueOrDefault<Schema::SchemeChangeRecords::BodySize>(0));
            entry->SetPositionKind(static_cast<NKikimrSchemeShard::TSchemeChangePosition::EKind>(
                rowset.GetValueOrDefault<Schema::SchemeChangeRecords::PositionKind>(
                    NKikimrSchemeShard::TSchemeChangePosition::KIND_EXACT)));

            ++count;

            if (!rowset.Next()) {
                return false;
            }
        }

        // Total-loss case. The partial-gap check above only fires when at
        // least one row survives; if every record above the cursor was swept
        // the scan returns nothing and the loop never runs. Orders are only
        // ever allocated when a record is written, so a cursor below the tail
        // with an empty result means those orders existed and are now gone.
        // ...but an in-flight record is not a loss, it is a not-yet. Orders
        // above the barrier are reserved rather than swept, so comparing the
        // cursor against the tail here would report every concurrent DDL as
        // lost records.
        if (count == 0 && !blockedByPending && effectiveAfterOrder < Self->GetVisibleSchemeChangeTail()) {
            skippedEntries += Self->GetVisibleSchemeChangeTail() - effectiveAfterOrder;
            subscriberLost = true;
            // Advance to the tail: leaving the cursor behind would re-report
            // the same loss on every subsequent fetch.
            effectiveAfterOrder = Self->GetVisibleSchemeChangeTail();
        }

        const TInstant now = ctx.Now();
        if (subscriberLost) {
            // Records this subscriber had not consumed were swept. Persist the
            // Lost marking so the loss survives a reboot and is not re-reported
            // as a fresh surprise on every subsequent fetch.
            db.Table<Schema::SchemeChangeSubscribers>().Key(subscriberId).Update(
                NIceDb::TUpdate<Schema::SchemeChangeSubscribers::LastAckedOrder>(effectiveAfterOrder),
                NIceDb::TUpdate<Schema::SchemeChangeSubscribers::LastActivityAt>(now.MicroSeconds()),
                NIceDb::TUpdate<Schema::SchemeChangeSubscribers::State>(
                    NKikimrSchemeShard::TSchemeChangeSubscriberState::STATE_LOST)
            );
        } else {
            db.Table<Schema::SchemeChangeSubscribers>().Key(subscriberId).Update(
                NIceDb::TUpdate<Schema::SchemeChangeSubscribers::LastActivityAt>(now.MicroSeconds())
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

        // Window closure, O(1). Replaces an O(|TxInFlight|) scan that ran on
        // every Fetch and was wrong three ways (off-by-one so the last DDL
        // before quiesce was never releasable; one long-running op pinned the
        // global min; PlanStep=0 ops sorted first).
        //
        // The SIGNAL is load-bearing and stays: Order is completion order while
        // ts is plan order, and they diverge across tablet transactions, so a
        // DDL planned before a sync point can be persisted after it. Without
        // this a consumer can never know that window (S_prev, S] is complete.
        Result->Record.SetClosedThroughPlanStep(Self->GetClosedThroughPlanStep());

        return true;
    }

    void Complete(const TActorContext& ctx) override {
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

        // Clamp to the visible tail, not the reserved one: a cursor parked
        // above an in-flight operation's row would drop that record the moment
        // it finalises.
        if (newOrder > Self->GetVisibleSchemeChangeTail()) {
            newOrder = Self->GetVisibleSchemeChangeTail();
        }

        const ui64 oldMinOrder = Self->GetMinSubscriberOrder(ctx.Now());

        const TInstant now = ctx.Now();
        db.Table<Schema::SchemeChangeSubscribers>().Key(subscriberId).Update(
            NIceDb::TUpdate<Schema::SchemeChangeSubscribers::LastAckedOrder>(newOrder),
            NIceDb::TUpdate<Schema::SchemeChangeSubscribers::LastActivityAt>(now.MicroSeconds())
        );

        if (auto it = Self->Subscribers.find(subscriberId); it != Self->Subscribers.end()) {
            it->second.LastAckedOrder = newOrder;
            it->second.LastActivityAt = now;
        }

        if (!Self->DeleteAckedSchemeChangeRecords(db, oldMinOrder, Self->GetMinSubscriberOrder(ctx.Now()),
                Self->SchemeChangeCleanupBatchSize, HasMoreToCleanup)) {
            return false;
        }

        Result->Record.SetStatus(NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);
        Result->Record.SetLastAckedOrder(newOrder);

        return true;
    }

    void Complete(const TActorContext& ctx) override {
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

        if (!CheckSubscriberAdmin(record.GetUserToken(), Result->Record)) {
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

        const ui64 oldMinOrder = Self->GetMinSubscriberOrder(ctx.Now());

        db.Table<Schema::SchemeChangeSubscribers>().Key(subscriberId).Delete();
        Self->Subscribers.erase(subscriberId);

        if (!Self->DeleteAckedSchemeChangeRecords(db, oldMinOrder, Self->GetMinSubscriberOrder(ctx.Now()),
                Self->SchemeChangeCleanupBatchSize, HasMoreToCleanup)) {
            return false;
        }

        Result->Record.SetStatus(NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);

        return true;
    }

    void Complete(const TActorContext& ctx) override {
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

        THashMap<ui64, TString> descriptionByOrder;
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
                    // Description lives on this table, so it rides along with
                    // the scan we are already doing -- no second pass.
                    auto desc = metaRowset.GetValueOrDefault<
                        Schema::SchemeChangeRecords::Description>(TString());
                    if (!desc.empty()) {
                        descriptionByOrder.emplace(order, std::move(desc));
                    }
                }
                if (!metaRowset.Next()) {
                    return false;
                }
            }
        }

        THashMap<ui64, TString> bodyByOrder;
        // (descriptionByOrder is filled by the metadata scan above)
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
