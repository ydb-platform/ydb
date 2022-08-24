#include "tablet_impl.h"

namespace NKikimr {
namespace NKesus {

struct TKesusTablet::TTxSemaphoreRelease : public TTxBase {
    const TActorId Sender;
    const ui64 Cookie;
    const NKikimrKesus::TEvReleaseSemaphore Record;

    TVector<TDelayedEvent> Events;

    TTxSemaphoreRelease(TSelf* self, const TActorId& sender, ui64 cookie, const NKikimrKesus::TEvReleaseSemaphore& record)
        : TTxBase(self)
        , Sender(sender)
        , Cookie(cookie)
        , Record(record)
    {
        Self->AddSessionTx(Record.GetSessionId());
    }

    TTxType GetTxType() const override { return TXTYPE_SEMAPHORE_RELEASE; }

    void ReplyOk(bool released = true) {
        Events.emplace_back(Sender, Cookie,
            new TEvKesus::TEvReleaseSemaphoreResult(Record.GetProxyGeneration(), released));
    }

    void ReplyError(Ydb::StatusIds::StatusCode status, const TString& reason) {
        Events.emplace_back(Sender, Cookie,
            new TEvKesus::TEvReleaseSemaphoreResult(Record.GetProxyGeneration(), status, reason));
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::KESUS_TABLET,
            "[" << Self->TabletID() << "] TTxSemaphoreRelease::Execute (sender=" << Sender
                << ", cookie=" << Cookie << ", name=" << Record.GetName().Quote() << ")");

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

        if (auto* semaphore = Self->SemaphoresByName.Value(Record.GetName(), nullptr)) {
            ui64 semaphoreId = semaphore->Id;

            if (auto* owner = session->OwnedSemaphores.FindPtr(semaphoreId)) {
                ReplyOk();
                Self->DoDeleteSessionSemaphore(db, semaphore, owner, Events);
                session->OwnedSemaphores.erase(semaphoreId);
                return true;
            }

            if (auto* waiter = session->WaitingSemaphores.FindPtr(semaphoreId)) {
                session->ConsumeSemaphoreWaitCookie(semaphore, [&](ui64 cookie) {
                    Events.emplace_back(proxy->ActorID, cookie,
                        new TEvKesus::TEvAcquireSemaphoreResult(
                            proxy->Generation,
                            Ydb::StatusIds::ABORTED,
                            "Operation superseded by another request"));
                });
                ReplyOk();
                Self->DoDeleteSessionSemaphore(db, semaphore, waiter, Events);
                session->WaitingSemaphores.erase(semaphoreId);
                return true;
            }
        }

        ReplyOk(false);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::KESUS_TABLET,
            "[" << Self->TabletID() << "] TTxSemaphoreRelease::Complete (sender=" << Sender
                << ", cookie=" << Cookie << ")");
        Self->RemoveSessionTx(Record.GetSessionId());

        for (auto& ev : Events) {
            ctx.Send(ev.Recipient, ev.Event.Release(), 0, ev.Cookie);
        }
    }
};

void TKesusTablet::Handle(TEvKesus::TEvReleaseSemaphore::TPtr& ev) {
    const auto& record = ev->Get()->Record;
    VerifyKesusPath(record.GetKesusPath());
    TabletCounters->Cumulative()[COUNTER_REQS_SEMAPHORE_RELEASE].Increment(1);

    Execute(new TTxSemaphoreRelease(this, ev->Sender, ev->Cookie, record), TActivationContext::AsActorContext());
}

}
}
