#include "tablet_impl.h"

namespace NKikimr {
namespace NKesus {

struct TKesusTablet::TTxSemaphoreDescribe : public TTxBase {
    const TActorId Sender;
    const ui64 Cookie;
    const NKikimrKesus::TEvDescribeSemaphore Record;

    THolder<TEvKesus::TEvDescribeSemaphoreChanged> Notification;
    THolder<TEvKesus::TEvDescribeSemaphoreResult> Reply;
    ui64 NotificationCookie = 0;

    TTxSemaphoreDescribe(TSelf* self, const TActorId& sender, ui64 cookie, const NKikimrKesus::TEvDescribeSemaphore& record)
        : TTxBase(self)
        , Sender(sender)
        , Cookie(cookie)
        , Record(record)
    {
        Self->AddSessionTx(Record.GetSessionId());
    }

    TTxType GetTxType() const override { return TXTYPE_SEMAPHORE_DESCRIBE; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::KESUS_TABLET,
            "[" << Self->TabletID() << "] TTxSemaphoreDescribe::Execute (sender=" << Sender
                << ", cookie=" << Cookie << ", name=" << Record.GetName().Quote() << ")");

        TProxyInfo* proxy = nullptr;
        TSessionInfo* session = nullptr;

        NIceDb::TNiceDb db(txc.DB);

        if (Record.GetProxyGeneration() != 0 || Record.GetSessionId() != 0) {
            proxy = Self->Proxies.FindPtr(Sender);
            if (!proxy || proxy->Generation != Record.GetProxyGeneration()) {
                Reply.Reset(new TEvKesus::TEvDescribeSemaphoreResult(
                    Record.GetProxyGeneration(),
                    Ydb::StatusIds::BAD_SESSION,
                    proxy ? "ProxyGeneration mismatch" : "Proxy is not registered"));
                return true;
            }

            if (Self->UseStrictRead()) {
                Self->PersistStrictMarker(db);
            }

            session = Self->Sessions.FindPtr(Record.GetSessionId());
            if (!session || session->OwnerProxy != proxy) {
                Reply.Reset(new TEvKesus::TEvDescribeSemaphoreResult(
                    Record.GetProxyGeneration(),
                    session ? Ydb::StatusIds::BAD_SESSION : Ydb::StatusIds::SESSION_EXPIRED,
                    session ? "Session not attached" : "Session does not exist"));
                return true;
            }
        } else if (Record.GetWatchData() || Record.GetWatchOwners()) {
            Reply.Reset(new TEvKesus::TEvDescribeSemaphoreResult(
                Record.GetProxyGeneration(),
                Ydb::StatusIds::BAD_REQUEST,
                "Watches cannot be used without an active session"));
            return true;
        } else {
            if (Self->UseStrictRead()) {
                Self->PersistStrictMarker(db);
            }
        }

        auto* semaphore = Self->SemaphoresByName.Value(Record.GetName(), nullptr);
        if (!semaphore) {
            Reply.Reset(new TEvKesus::TEvDescribeSemaphoreResult(
                Record.GetProxyGeneration(),
                Ydb::StatusIds::NOT_FOUND,
                TStringBuilder() << "Semaphore not found"));
            return true;
        }

        Reply.Reset(new TEvKesus::TEvDescribeSemaphoreResult(Record.GetProxyGeneration()));
        auto* desc = Reply->Record.MutableSemaphoreDescription();
        desc->set_name(semaphore->Name);
        desc->set_data(semaphore->Data);
        desc->set_count(semaphore->Count);
        desc->set_limit(semaphore->Limit);
        desc->set_ephemeral(semaphore->Ephemeral);

        if (Record.GetIncludeOwners()) {
            for (const auto* owner : semaphore->Owners) {
                auto* p = desc->add_owners();
                p->set_order_id(owner->OrderId);
                p->set_session_id(owner->SessionId);
                p->set_count(owner->Count);
                p->set_data(owner->Data);
            }
        }

        if (Record.GetIncludeWaiters()) {
            for (const auto& kv : semaphore->Waiters) {
                auto* waiter = kv.second;
                auto* p = desc->add_waiters();
                p->set_order_id(waiter->OrderId);
                p->set_session_id(waiter->SessionId);
                p->set_timeout_millis(waiter->TimeoutMillis);
                p->set_count(waiter->Count);
                p->set_data(waiter->Data);
            }
        }

        if (Record.GetWatchData() || Record.GetWatchOwners()) {
            bool replacingWatch =
                semaphore->DataWatchers.erase(session) |
                semaphore->OwnersWatchers.erase(session);
            if (replacingWatch) {
                NotificationCookie = session->RemoveSemaphoreWatchCookie(semaphore);
                Notification.Reset(new TEvKesus::TEvDescribeSemaphoreChanged(proxy->Generation, false, false));
            }

            session->SemaphoreWatchCookie[semaphore] = Cookie;
            if (Record.GetWatchData()) {
                semaphore->DataWatchers.insert(session);
            }
            if (Record.GetWatchOwners()) {
                semaphore->OwnersWatchers.insert(session);
            }
            Reply->Record.SetWatchAdded(true);
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::KESUS_TABLET,
            "[" << Self->TabletID() << "] TTxSemaphoreDescribe::Complete (sender=" << Sender
                << ", cookie=" << Cookie << ")");
        Self->RemoveSessionTx(Record.GetSessionId());

        if (Notification) {
            ctx.Send(Sender, Notification.Release(), 0, NotificationCookie);
        }

        Y_ABORT_UNLESS(Reply);
        ctx.Send(Sender, Reply.Release(), 0, Cookie);
    }
};

void TKesusTablet::Handle(TEvKesus::TEvDescribeSemaphore::TPtr& ev) {
    const auto& record = ev->Get()->Record;
    VerifyKesusPath(record.GetKesusPath());

    Execute(new TTxSemaphoreDescribe(this, ev->Sender, ev->Cookie, record), TActivationContext::AsActorContext());
}

}
}
