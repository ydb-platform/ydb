#include "tablet_impl.h"

namespace NKikimr {
namespace NKesus {

struct TKesusTablet::TTxSemaphoreAcquire : public TTxBase {
    const TActorId Sender;
    const ui64 Cookie;
    const NKikimrKesus::TEvAcquireSemaphore Record;

    TVector<TDelayedEvent> Events;

    TTxSemaphoreAcquire(TSelf* self, const TActorId& sender, ui64 cookie, const NKikimrKesus::TEvAcquireSemaphore& record)
        : TTxBase(self)
        , Sender(sender)
        , Cookie(cookie)
        , Record(record)
    {
        Self->AddSessionTx(Record.GetSessionId());
    }

    TTxType GetTxType() const override { return TXTYPE_SEMAPHORE_ACQUIRE; }

    void ReplyOk() {
        Events.emplace_back(Sender, Cookie,
            new TEvKesus::TEvAcquireSemaphoreResult(Record.GetProxyGeneration()));
    }

    void ReplyTimeout() {
        Events.emplace_back(Sender, Cookie,
            new TEvKesus::TEvAcquireSemaphoreResult(Record.GetProxyGeneration(), false));
    }

    void ReplyPending() {
        Events.emplace_back(Sender, Cookie,
            new TEvKesus::TEvAcquireSemaphorePending(Record.GetProxyGeneration()));
    }

    void ReplyError(Ydb::StatusIds::StatusCode status, const TString& reason) {
        Events.emplace_back(Sender, Cookie,
            new TEvKesus::TEvAcquireSemaphoreResult(Record.GetProxyGeneration(), status, reason));
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::KESUS_TABLET,
            "[" << Self->TabletID() << "] TTxSemaphoreAcquire::Execute (sender=" << Sender
                << ", cookie=" << Cookie << ", session=" << Record.GetSessionId()
                << ", semaphore=" << Record.GetName().Quote() << " count=" << Record.GetCount() << ")");

        auto* proxy = Self->Proxies.FindPtr(Sender);
        if (!proxy || proxy->Generation != Record.GetProxyGeneration()) {
            // World has changed by the time we executed
            ReplyError(
                Ydb::StatusIds::BAD_SESSION,
                proxy ? "ProxyGeneration mismatch" : "Proxy is not registered");
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);
        Self->PersistStrictMarker(db);

        ui64 sessionId = Record.GetSessionId();
        auto* session = Self->Sessions.FindPtr(sessionId);
        if (!session || session->OwnerProxy != proxy) {
            // Session destroyed or stolen by the time we executed
            ReplyError(
                session ? Ydb::StatusIds::BAD_SESSION : Ydb::StatusIds::SESSION_EXPIRED,
                session ? "Session not attached" : "Session does not exist");
            return true;
        }

        auto* semaphore = Self->SemaphoresByName.Value(Record.GetName(), nullptr);
        if (!semaphore && !Record.GetEphemeral()) {
            // Semaphore does not exist
            ReplyError(
                Ydb::StatusIds::NOT_FOUND,
                "Semaphore does not exist");
            return true;
        }

        if (semaphore && semaphore->Ephemeral != Record.GetEphemeral()) {
            // Semaphore ephemeral mode mismatch
            ReplyError(
                Ydb::StatusIds::PRECONDITION_FAILED,
                "Semaphore ephemeral mode mismatch");
            return true;
        }

        if (semaphore && Record.GetCount() > semaphore->Limit) {
            // This acquire will never succeed
            ReplyError(
                Ydb::StatusIds::BAD_REQUEST,
                "Cannot acquire more than allowed by the semaphore");
            return true;
        }

        if (!semaphore) {
            // Allocate a new semaphore
            Y_ABORT_UNLESS(Record.GetEphemeral());

            if (Self->Semaphores.size() >= MAX_SEMAPHORES_LIMIT) {
                ReplyError(
                    Ydb::StatusIds::PRECONDITION_FAILED,
                    "Too many semaphores already exist");
                return true;
            }

            ui64 semaphoreId = Self->NextSemaphoreId++;
            Y_ABORT_UNLESS(semaphoreId > 0);
            Y_ABORT_UNLESS(!Self->Semaphores.contains(semaphoreId));
            Self->PersistSysParam(db, Schema::SysParam_NextSemaphoreId, ToString(Self->NextSemaphoreId));
            semaphore = &Self->Semaphores[semaphoreId];
            semaphore->Id = semaphoreId;
            semaphore->Name = Record.GetName();
            semaphore->Limit = Max<ui64>();
            semaphore->Count = 0;
            semaphore->Ephemeral = true;
            Self->SemaphoresByName[semaphore->Name] = semaphore;
            db.Table<Schema::Semaphores>().Key(semaphoreId).Update(
                NIceDb::TUpdate<Schema::Semaphores::Name>(semaphore->Name),
                NIceDb::TUpdate<Schema::Semaphores::Limit>(semaphore->Limit),
                NIceDb::TUpdate<Schema::Semaphores::Ephemeral>(semaphore->Ephemeral));
            Self->TabletCounters->Simple()[COUNTER_SEMAPHORE_COUNT].Add(1);
            LOG_DEBUG_S(ctx, NKikimrServices::KESUS_TABLET,
                "[" << Self->TabletID() << "] Created new ephemeral semaphore "
                    << semaphoreId << " " << Record.GetName().Quote());
        }
        ui64 semaphoreId = semaphore->Id;

        if (auto* owner = session->OwnedSemaphores.FindPtr(semaphoreId)) {
            // Session already owns this semaphore
            if (Record.GetCount() > owner->Count) {
                // Increasing count is not allowed
                ReplyError(
                    Ydb::StatusIds::BAD_REQUEST,
                    "Increasing count is not allowed");
                return true;
            }
            if (owner->Count == Record.GetCount()) {
                // Count hasn't changed, persist data if it changed
                bool ownerChanged = false;
                if (owner->Data != Record.GetData()) {
                    // Only write data to the database if it has changed
                    owner->Data = Record.GetData();
                    db.Table<Schema::SessionSemaphores>().Key(sessionId, semaphoreId).Update(
                        NIceDb::TUpdate<Schema::SessionSemaphores::Data>(owner->Data));
                    ownerChanged = true;
                }
                ReplyOk();
                semaphore->NotifyWatchers(Events, false, ownerChanged);
                return true;
            }
            Y_ABORT_UNLESS(Record.GetCount() > 0);
            Y_ABORT_UNLESS(Record.GetCount() < owner->Count);
            semaphore->Count -= owner->Count;
            owner->Count = Record.GetCount();
            semaphore->Count += owner->Count;
            if (owner->Data != Record.GetData()) {
                // Only write data to the database if it has changed
                owner->Data = Record.GetData();
                db.Table<Schema::SessionSemaphores>().Key(sessionId, semaphoreId).Update(
                    NIceDb::TUpdate<Schema::SessionSemaphores::Count>(owner->Count),
                    NIceDb::TUpdate<Schema::SessionSemaphores::Data>(owner->Data));
            } else {
                db.Table<Schema::SessionSemaphores>().Key(sessionId, semaphoreId).Update(
                    NIceDb::TUpdate<Schema::SessionSemaphores::Count>(owner->Count));
            }
            ReplyOk();
            Self->DoProcessSemaphoreQueue(semaphore, Events, true);
            return true;
        }

        auto* waiter = session->WaitingSemaphores.FindPtr(semaphoreId);
        if (waiter) {
            // This session is already waiting for the semaphore
            Y_ABORT_UNLESS(semaphore->Waiters.contains(waiter->OrderId));
            if (Record.GetCount() > waiter->Count) {
                // Increasing count is not allowed
                ReplyError(
                    Ydb::StatusIds::BAD_REQUEST,
                    "Increasing count is not allowed");
                return true;
            }
            Y_ABORT_UNLESS(Record.GetCount() > 0);
            Y_ABORT_UNLESS(Record.GetCount() <= waiter->Count);
            session->ConsumeSemaphoreWaitCookie(semaphore, [&](ui64 cookie) {
                Events.emplace_back(proxy->ActorID, cookie,
                    new TEvKesus::TEvAcquireSemaphoreResult(
                        proxy->Generation,
                        Ydb::StatusIds::ABORTED,
                        "Operation superseded by another request"));
            });
            if (Record.GetTimeoutMillis() == 0 && !(semaphore->GetFirstOrderId() == waiter->OrderId && semaphore->CanAcquire(Record.GetCount()))) {
                // Optimization: fold timeout if this waiter won't be promoted immediately
                ReplyTimeout();
                Self->DoDeleteSessionSemaphore(db, semaphore, waiter, Events);
                session->WaitingSemaphores.erase(semaphoreId);
                Self->TabletCounters->Cumulative()[COUNTER_ACQUIRE_TIMEOUT].Increment(1);
                return true;
            }
            // Timeout is almost always changed, however avoid updating data unless it changed
            waiter->TimeoutMillis = Record.GetTimeoutMillis();
            waiter->Count = Record.GetCount();
            if (waiter->Data != Record.GetData()) {
                waiter->Data = Record.GetData();
                db.Table<Schema::SessionSemaphores>().Key(sessionId, semaphoreId).Update(
                    NIceDb::TUpdate<Schema::SessionSemaphores::TimeoutMillis>(waiter->TimeoutMillis),
                    NIceDb::TUpdate<Schema::SessionSemaphores::Count>(waiter->Count),
                    NIceDb::TUpdate<Schema::SessionSemaphores::Data>(waiter->Data));
            } else {
                db.Table<Schema::SessionSemaphores>().Key(sessionId, semaphoreId).Update(
                    NIceDb::TUpdate<Schema::SessionSemaphores::TimeoutMillis>(waiter->TimeoutMillis),
                    NIceDb::TUpdate<Schema::SessionSemaphores::Count>(waiter->Count));
            }
        } else {
            if (Record.GetTimeoutMillis() == 0 && !(semaphore->Waiters.empty() && semaphore->CanAcquire(Record.GetCount()))) {
                // Optimization: fold timeout if this waiter won't be promoted immediately
                ReplyTimeout();
                Self->TabletCounters->Cumulative()[COUNTER_ACQUIRE_TIMEOUT].Increment(1);
                return true;
            }
            // Create a new waiter
            ui64 orderId = Self->NextSemaphoreOrderId++;
            Y_ABORT_UNLESS(orderId > 0);
            Self->PersistSysParam(db, Schema::SysParam_NextSemaphoreOrderId, ToString(Self->NextSemaphoreOrderId));
            waiter = &session->WaitingSemaphores[semaphoreId];
            waiter->OrderId = orderId;
            waiter->SessionId = sessionId;
            waiter->TimeoutMillis = Record.GetTimeoutMillis();
            waiter->Count = Record.GetCount();
            waiter->Data = Record.GetData();
            db.Table<Schema::SessionSemaphores>().Key(sessionId, semaphoreId).Update(
                NIceDb::TUpdate<Schema::SessionSemaphores::OrderId>(orderId),
                NIceDb::TUpdate<Schema::SessionSemaphores::TimeoutMillis>(waiter->TimeoutMillis),
                NIceDb::TUpdate<Schema::SessionSemaphores::Count>(waiter->Count),
                NIceDb::TUpdate<Schema::SessionSemaphores::Data>(waiter->Data));
            semaphore->Waiters[orderId] = waiter;
            Self->TabletCounters->Simple()[COUNTER_SEMAPHORE_WAITER_COUNT].Add(1);
        }

        // Remember cookie for future responses
        session->SemaphoreWaitCookie[semaphore] = Cookie;

        // Reschedule with a new timeout
        waiter->TimeoutCookie.Detach();

        // If this is the first waiter it could affect semaphore progress
        if (semaphore->GetFirstOrderId() == waiter->OrderId) {
            Self->DoProcessSemaphoreQueue(semaphore, Events);
        }

        // Notify sender if session is now waiting for the semaphore
        if (session->WaitingSemaphores.contains(semaphoreId)) {
            Y_ABORT_UNLESS(Self->ScheduleWaiterTimeout(semaphoreId, waiter, ctx));
            ReplyPending();
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::KESUS_TABLET,
            "[" << Self->TabletID() << "] TTxSemaphoreAcquire::Complete (sender=" << Sender
                << ", cookie=" << Cookie << ")");
        Self->RemoveSessionTx(Record.GetSessionId());

        for (auto& ev : Events) {
            ctx.Send(ev.Recipient, ev.Event.Release(), 0, ev.Cookie);
        }
    }
};

void TKesusTablet::Handle(TEvKesus::TEvAcquireSemaphore::TPtr& ev) {
    const auto& record = ev->Get()->Record;
    VerifyKesusPath(record.GetKesusPath());
    TabletCounters->Cumulative()[COUNTER_REQS_SEMAPHORE_ACQUIRE].Increment(1);

    if (record.GetCount() <= 0) {
        Send(ev->Sender,
            new TEvKesus::TEvAcquireSemaphoreResult(
                record.GetProxyGeneration(),
                Ydb::StatusIds::BAD_REQUEST,
                "Acquire must have count > 0"),
            0, ev->Cookie);
        return;
    }

    if (record.GetTimeoutMillis() != Max<ui64>() && record.GetTimeoutMillis() > MAX_ACQUIRE_TIMEOUT.MilliSeconds()) {
        Send(ev->Sender,
            new TEvKesus::TEvAcquireSemaphoreResult(
                record.GetProxyGeneration(),
                Ydb::StatusIds::BAD_REQUEST,
                "Acquire timeout is out of range"),
            0, ev->Cookie);
        return;
    }

    if (record.GetData().size() > MAX_SEMAPHORE_DATA) {
        Send(ev->Sender,
            new TEvKesus::TEvAcquireSemaphoreResult(
                record.GetProxyGeneration(),
                Ydb::StatusIds::BAD_REQUEST,
                "Acquire data is too large"),
            0, ev->Cookie);
        return;
    }

    Execute(new TTxSemaphoreAcquire(this, ev->Sender, ev->Cookie, record), TActivationContext::AsActorContext());
}

}
}
