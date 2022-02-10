#include "tablet_impl.h"

namespace NKikimr {
namespace NKesus {

struct TKesusTablet::TTxSessionDestroy : public TTxBase {
    const TActorId Sender;
    const ui64 Cookie;
    const NKikimrKesus::TEvDestroySession Record;

    TVector<TDelayedEvent> Events;

    TTxSessionDestroy(TSelf* self, const TActorId& sender, ui64 cookie, const NKikimrKesus::TEvDestroySession& record)
        : TTxBase(self)
        , Sender(sender)
        , Cookie(cookie)
        , Record(record)
    {
        Self->AddSessionTx(Record.GetSessionId());
    }

    TTxType GetTxType() const override { return TXTYPE_SESSION_DESTROY; }

    void ReplyOk() {
        Events.emplace_back(Sender, Cookie,
            new TEvKesus::TEvDestroySessionResult(Record.GetProxyGeneration()));
    }

    void ReplyError(Ydb::StatusIds::StatusCode status, const TString& reason) {
        Events.emplace_back(Sender, Cookie,
            new TEvKesus::TEvDestroySessionResult(Record.GetProxyGeneration(), status, reason));
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::KESUS_TABLET,
            "[" << Self->TabletID() << "] TTxSessionDestroy::Execute (sender=" << Sender
                << ", cookie=" << Cookie << ", session=" << Record.GetSessionId() << ")");

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
        if (!session) {
            // Session destroyed by the time we executed
            ReplyError(
                Ydb::StatusIds::SESSION_EXPIRED,
                "Session does not exist");
            return true;
        }

        if (session->OwnerProxy != proxy && session->LastOwnerProxy) {
            // Notify the last owner, unless it's this proxy
            Events.emplace_back(session->LastOwnerProxy, 0,
                new TEvKesus::TEvSessionExpired(sessionId));
        }

        ReplyOk();
        Self->DoDeleteSession(db, session, Events);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::KESUS_TABLET,
            "[" << Self->TabletID() << "] TTxSessionDestroy::Complete (sender=" << Sender
                << ", cookie=" << Cookie << ")");
        Self->RemoveSessionTx(Record.GetSessionId());

        for (auto& ev : Events) {
            ctx.Send(ev.Recipient, ev.Event.Release(), 0, ev.Cookie);
        }
    }
};

void TKesusTablet::Handle(TEvKesus::TEvDestroySession::TPtr& ev) {
    const auto& record = ev->Get()->Record;
    VerifyKesusPath(record.GetKesusPath());
    TabletCounters->Cumulative()[COUNTER_REQS_SESSION_DESTROY].Increment(1);

    Execute(new TTxSessionDestroy(this, ev->Sender, ev->Cookie, record), TActivationContext::AsActorContext());
}

}
}
