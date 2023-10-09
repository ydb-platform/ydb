#include "tablet_impl.h"

namespace NKikimr {
namespace NKesus {

struct TKesusTablet::TTxSessionDetach : public TTxBase {
    const TActorId Sender;
    const ui64 Cookie;
    const NKikimrKesus::TEvDetachSession Record;

    THolder<TEvKesus::TEvDetachSessionResult> Reply;

    explicit TTxSessionDetach(TSelf* self, const TActorId& sender, ui64 cookie, const NKikimrKesus::TEvDetachSession& record)
        : TTxBase(self)
        , Sender(sender)
        , Cookie(cookie)
        , Record(record)
    {
        Self->AddSessionTx(Record.GetSessionId());
    }

    TTxType GetTxType() const override { return TXTYPE_SESSION_DETACH; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        Y_UNUSED(txc);
        LOG_DEBUG_S(ctx, NKikimrServices::KESUS_TABLET,
            "[" << Self->TabletID() << "] TTxSessionDetach::Execute (sender=" << Sender
                << ", cookie=" << Cookie << ", session=" << Record.GetSessionId() << ")");

        auto* proxy = Self->Proxies.FindPtr(Sender);
        if (!proxy || proxy->Generation != Record.GetProxyGeneration()) {
            // World has changed by the time we executed
            Reply.Reset(new TEvKesus::TEvDetachSessionResult(
                Record.GetProxyGeneration(),
                Ydb::StatusIds::BAD_SESSION,
                proxy ? "ProxyGeneration mismatch" : "Proxy is not registered"));
            return true;
        }

        ui64 sessionId = Record.GetSessionId();
        auto* session = Self->Sessions.FindPtr(sessionId);
        if (!session || session->OwnerProxy != proxy) {
            // Session destroyed or stolen by the time we executed
            Reply.Reset(new TEvKesus::TEvDetachSessionResult(
                Record.GetProxyGeneration(),
                session ? Ydb::StatusIds::BAD_SESSION : Ydb::StatusIds::SESSION_EXPIRED,
                session ? "Session not attached" : "Session does not exist"));
            return true;
        }

        Y_ABORT_UNLESS(Self->ScheduleSessionTimeout(session, ctx));
        Reply.Reset(new TEvKesus::TEvDetachSessionResult(Record.GetProxyGeneration()));
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::KESUS_TABLET,
            "[" << Self->TabletID() << "] TTxSessionDetach::Complete (sender=" << Sender
                << ", cookie=" << Cookie << ")");
        Self->RemoveSessionTx(Record.GetSessionId());

        Y_ABORT_UNLESS(Reply);

        ctx.Send(Sender, Reply.Release(), 0, Cookie);
    }
};

void TKesusTablet::Handle(TEvKesus::TEvDetachSession::TPtr& ev) {
    const auto& record = ev->Get()->Record;
    VerifyKesusPath(record.GetKesusPath());
    TabletCounters->Cumulative()[COUNTER_REQS_SESSION_DETACH].Increment(1);

    auto* proxy = Proxies.FindPtr(ev->Sender);
    if (!proxy || proxy->Generation != record.GetProxyGeneration()) {
        Send(ev->Sender,
            new TEvKesus::TEvDetachSessionResult(
                record.GetProxyGeneration(),
                Ydb::StatusIds::BAD_SESSION,
                proxy ? "ProxyGeneration mismatch" : "Proxy is not registered"),
            0, ev->Cookie);
        return;
    }

    ui64 sessionId = record.GetSessionId();
    if (IsSessionFastPathAllowed(sessionId)) {
        auto* session = Sessions.FindPtr(sessionId);
        if (!session || session->OwnerProxy != proxy) {
            Send(ev->Sender,
                new TEvKesus::TEvDetachSessionResult(
                    record.GetProxyGeneration(),
                    session ? Ydb::StatusIds::BAD_SESSION : Ydb::StatusIds::SESSION_EXPIRED,
                    session ? "Session not attached" : "Session does not exist"),
                0, ev->Cookie);
            return;
        }

        // avoid unnecessary transactions
        LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::KESUS_TABLET,
            "[" << TabletID() << "] Fast-path detach session=" << sessionId
                << " from sender=" << ev->Sender << ", cookie=" << ev->Cookie);

        Y_ABORT_UNLESS(ScheduleSessionTimeout(session, TActivationContext::AsActorContext()));
        Send(ev->Sender,
            new TEvKesus::TEvDetachSessionResult(record.GetProxyGeneration()),
            0, ev->Cookie);
        return;
    }

    Execute(new TTxSessionDetach(this, ev->Sender, ev->Cookie, record), TActivationContext::AsActorContext());
}

}
}
