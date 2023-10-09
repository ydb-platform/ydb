#include "tablet_impl.h"

namespace NKikimr {
namespace NKesus {

struct TKesusTablet::TTxSessionAttach : public TTxBase {
    const TActorId Sender;
    const ui64 Cookie;
    const NKikimrKesus::TEvAttachSession Record;
    ui64 SessionId;

    TActorId PreviousOwner;
    ui64 PreviousGeneration = 0;
    ui64 PreviousCookie = 0;
    THolder<TEvKesus::TEvAttachSessionResult> Reply;

    explicit TTxSessionAttach(TSelf* self, const TActorId& sender, ui64 cookie, const NKikimrKesus::TEvAttachSession& record)
        : TTxBase(self)
        , Sender(sender)
        , Cookie(cookie)
        , Record(record)
        , SessionId(Record.GetSessionId())
    {
        if (SessionId != 0) {
            Self->AddSessionTx(SessionId);
        }
    }

    TTxType GetTxType() const override { return TXTYPE_SESSION_ATTACH; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::KESUS_TABLET,
            "[" << Self->TabletID() << "] TTxSessionAttach::Execute (sender=" << Sender
                << ", cookie=" << Cookie << ", session=" << SessionId
                << ", seqNo=" << Record.GetSeqNo() << ")");

        auto* proxy = Self->Proxies.FindPtr(Sender);
        if (!proxy || proxy->Generation != Record.GetProxyGeneration()) {
            // World has changed by the time we executed
            Reply.Reset(new TEvKesus::TEvAttachSessionResult(
                Record.GetProxyGeneration(),
                Record.GetSessionId(),
                Ydb::StatusIds::BAD_SESSION,
                proxy ? "ProxyGeneration mismatch" : "Proxy is not registered"));
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);
        if (Self->UseStrictAttach()) {
            Self->PersistStrictMarker(db);
        }

        TSessionInfo* session;
        if (SessionId != 0) {
            session = Self->Sessions.FindPtr(SessionId);
            if (!session) {
                Reply.Reset(new TEvKesus::TEvAttachSessionResult(
                    Record.GetProxyGeneration(),
                    Record.GetSessionId(),
                    Ydb::StatusIds::SESSION_EXPIRED,
                    "Session does not exist"));
                return true;
            }

            if (Record.GetSeqNo() < session->LastOwnerSeqNo) {
                Reply.Reset(new TEvKesus::TEvAttachSessionResult(
                    Record.GetProxyGeneration(),
                    Record.GetSessionId(),
                    Ydb::StatusIds::BAD_SESSION,
                    "Attach request is out of sequence"));
                return true;
            }

            if (session->ProtectionKey && session->ProtectionKey != Record.GetProtectionKey()) {
                Reply.Reset(new TEvKesus::TEvAttachSessionResult(
                    Record.GetProxyGeneration(),
                    Record.GetSessionId(),
                    Ydb::StatusIds::BAD_SESSION,
                    "Attach request protection key mismatch"));
                return true;
            }

            auto row = db.Table<Schema::Sessions>().Key(SessionId);
            if (session->TimeoutMillis != Record.GetTimeoutMillis()) {
                session->TimeoutMillis = Record.GetTimeoutMillis();
                row.Update(NIceDb::TUpdate<Schema::Sessions::TimeoutMillis>(session->TimeoutMillis));
            }
            if (session->Description != Record.GetDescription()) {
                session->Description = Record.GetDescription();
                row.Update(NIceDb::TUpdate<Schema::Sessions::Description>(session->Description));
            }
        } else {
            if (Self->Sessions.size() >= MAX_SESSIONS_LIMIT) {
                Reply.Reset(new TEvKesus::TEvAttachSessionResult(
                    Record.GetProxyGeneration(),
                    Record.GetSessionId(),
                    Ydb::StatusIds::PRECONDITION_FAILED,
                    "Too many sessions already exist"));
                return true;
            }

            SessionId = Self->NextSessionId++;
            Y_ABORT_UNLESS(SessionId > 0);
            Y_ABORT_UNLESS(!Self->Sessions.contains(SessionId));
            Self->PersistSysParam(db, Schema::SysParam_NextSessionId, ToString(Self->NextSessionId));
            session = &Self->Sessions[SessionId];
            session->Id = SessionId;
            session->TimeoutMillis = Record.GetTimeoutMillis();
            session->Description = Record.GetDescription();
            session->ProtectionKey = Record.GetProtectionKey();
            Self->AddSessionTx(SessionId);
            Self->TabletCounters->Simple()[COUNTER_SESSION_COUNT].Add(1);
            db.Table<Schema::Sessions>().Key(SessionId).Update(
                NIceDb::TUpdate<Schema::Sessions::TimeoutMillis>(session->TimeoutMillis),
                NIceDb::TUpdate<Schema::Sessions::Description>(session->Description),
                NIceDb::TUpdate<Schema::Sessions::ProtectionKey>(session->ProtectionKey));
            LOG_DEBUG_S(ctx, NKikimrServices::KESUS_TABLET,
                "[" << Self->TabletID() << "] Created new session " << SessionId);
        }

        // Set new owner and cancel timeout
        if (session->OwnerProxy && session->OwnerProxy != proxy) {
            PreviousOwner = session->OwnerProxy->ActorID;
            PreviousGeneration = session->OwnerProxy->Generation;
            PreviousCookie = session->OwnerCookie;
            Self->TabletCounters->Cumulative()[COUNTER_SESSION_STOLEN].Increment(1);
        }
        if (!session->OwnerProxy) {
            Self->TabletCounters->Simple()[COUNTER_SESSION_ACTIVE_COUNT].Add(1);
        }
        session->AttachProxy(proxy, Cookie, Record.GetSeqNo());

        Reply.Reset(new TEvKesus::TEvAttachSessionResult(proxy->Generation, SessionId));
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::KESUS_TABLET,
            "[" << Self->TabletID() << "] TTxSessionAttach::Complete (sender=" << Sender
                << ", cookie=" << Cookie << ", session=" << SessionId << ")");

        if (SessionId != 0) {
            Self->RemoveSessionTx(SessionId);
        }

        if (PreviousOwner) {
            ctx.Send(PreviousOwner, new TEvKesus::TEvSessionStolen(PreviousGeneration, SessionId), 0, PreviousCookie);
        }

        Y_ABORT_UNLESS(Reply);
        ctx.Send(Sender, std::move(Reply), 0, Cookie);

        Self->ScheduleSelfCheck(ctx);
    }
};

void TKesusTablet::Handle(TEvKesus::TEvAttachSession::TPtr& ev) {
    const auto& record = ev->Get()->Record;
    VerifyKesusPath(record.GetKesusPath());
    TabletCounters->Cumulative()[COUNTER_REQS_SESSION_ATTACH].Increment(1);

    auto* proxy = Proxies.FindPtr(ev->Sender);
    if (!proxy || proxy->Generation != record.GetProxyGeneration()) {
        Send(ev->Sender,
            new TEvKesus::TEvAttachSessionResult(
                record.GetProxyGeneration(),
                record.GetSessionId(),
                Ydb::StatusIds::BAD_SESSION,
                proxy ? "ProxyGeneration mismatch" : "Proxy is not registered"),
            0, ev->Cookie);
        return;
    }

    if (record.GetTimeoutMillis() > MAX_SESSION_TIMEOUT.MilliSeconds()) {
        Send(ev->Sender,
            new TEvKesus::TEvAttachSessionResult(
                record.GetProxyGeneration(),
                record.GetSessionId(),
                Ydb::StatusIds::BAD_REQUEST,
                "Session timeout is out of range"),
            0, ev->Cookie);
        return;
    }

    if (record.GetDescription().size() > MAX_DESCRIPTION_SIZE) {
        Send(ev->Sender,
            new TEvKesus::TEvAttachSessionResult(
                record.GetProxyGeneration(),
                record.GetSessionId(),
                Ydb::StatusIds::BAD_REQUEST,
                "Description is too large"),
            0, ev->Cookie);
        return;
    }

    if (record.GetProtectionKey().size() > MAX_PROTECTION_KEY_SIZE) {
        Send(ev->Sender,
            new TEvKesus::TEvAttachSessionResult(
                record.GetProxyGeneration(),
                record.GetSessionId(),
                Ydb::StatusIds::BAD_REQUEST,
                "Protection key is too large"),
            0, ev->Cookie);
        return;
    }

    ui64 sessionId = record.GetSessionId();
    if (IsSessionFastPathAllowed(sessionId) && !UseStrictAttach()) {
        // Attaching to an existing session
        auto* session = Sessions.FindPtr(sessionId);
        if (!session) {
            Send(ev->Sender,
                new TEvKesus::TEvAttachSessionResult(
                    record.GetProxyGeneration(),
                    record.GetSessionId(),
                    Ydb::StatusIds::SESSION_EXPIRED,
                    "Session does not exist"),
                0, ev->Cookie);
            return;
        }

        if (record.GetSeqNo() < session->LastOwnerSeqNo) {
            Send(ev->Sender,
                new TEvKesus::TEvAttachSessionResult(
                    record.GetProxyGeneration(),
                    record.GetSessionId(),
                    Ydb::StatusIds::BAD_SESSION,
                    "Attach request is out of sequence"),
                0, ev->Cookie);
            return;
        }

        if (session->ProtectionKey && session->ProtectionKey != record.GetProtectionKey()) {
            Send(ev->Sender,
                new TEvKesus::TEvAttachSessionResult(
                    record.GetProxyGeneration(),
                    record.GetSessionId(),
                    Ydb::StatusIds::BAD_SESSION,
                    "Attach request protection key mismatch"),
                0, ev->Cookie);
            return;
        }

        if (session->TimeoutMillis == record.GetTimeoutMillis() &&
            session->Description == record.GetDescription())
        {
            // POTENTIALLY UNSAFE OPTIMIZATION
            // If this session doesn't have any pending transactions and
            // nothing in the underlying database is changing, then we may
            // give ownership to the requesting proxy immediately. This
            // may be unsafe if another copy of the tablet is up, making us
            // a stale leader, and in which case we shouldn't be allowed to
            // make decisions.
            // However if we assume that only one session uses SessionId at a
            // time this successful attachment is fine. Eventually proxy
            // will reconnect to the new leader anyway and situation where
            // two sessions successfully attached is not possible.
            // Unless they use the same SessionId that is.
            LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::KESUS_TABLET,
                "[" << TabletID() << "] Fast-path attach session=" << sessionId
                    << " to sender=" << ev->Sender << ", cookie=" << ev->Cookie
                    << ", seqNo=" << record.GetSeqNo());

            if (session->OwnerProxy && session->OwnerProxy != proxy) {
                // Notify old proxy that its session has been stolen
                Send(session->OwnerProxy->ActorID,
                    new TEvKesus::TEvSessionStolen(session->OwnerProxy->Generation, sessionId),
                    0, session->OwnerCookie);
                TabletCounters->Cumulative()[COUNTER_SESSION_STOLEN].Increment(1);
            }
            if (!session->OwnerProxy) {
                TabletCounters->Simple()[COUNTER_SESSION_ACTIVE_COUNT].Add(1);
            }
            session->AttachProxy(proxy, ev->Cookie, record.GetSeqNo());
            Send(ev->Sender,
                new TEvKesus::TEvAttachSessionResult(proxy->Generation, sessionId),
                0, ev->Cookie);
            return;
        }
    }

    Execute(new TTxSessionAttach(this, ev->Sender, ev->Cookie, record), TActivationContext::AsActorContext());
}

}
}
