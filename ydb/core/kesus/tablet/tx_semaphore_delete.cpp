#include "tablet_impl.h"

namespace NKikimr {
namespace NKesus {

struct TKesusTablet::TTxSemaphoreDelete : public TTxBase {
    const TActorId Sender;
    const ui64 Cookie;
    const NKikimrKesus::TEvDeleteSemaphore Record;

    TVector<TDelayedEvent> Events;

    TTxSemaphoreDelete(TSelf* self, const TActorId& sender, ui64 cookie, const NKikimrKesus::TEvDeleteSemaphore& record)
        : TTxBase(self)
        , Sender(sender)
        , Cookie(cookie)
        , Record(record)
    {
        Self->AddSessionTx(Record.GetSessionId());
    }

    TTxType GetTxType() const override { return TXTYPE_SEMAPHORE_DELETE; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::KESUS_TABLET,
            "[" << Self->TabletID() << "] TTxSemaphoreDelete::Execute (sender=" << Sender
                << ", cookie=" << Cookie << ", name=" << Record.GetName().Quote()
                << ", force=" << Record.GetForce() << ")");

        NIceDb::TNiceDb db(txc.DB);

        if (Record.GetProxyGeneration() != 0 || Record.GetSessionId() != 0) {
            auto* proxy = Self->Proxies.FindPtr(Sender);
            if (!proxy || proxy->Generation != Record.GetProxyGeneration()) {
                Events.emplace_back(Sender, Cookie,
                    new TEvKesus::TEvDeleteSemaphoreResult(
                        Record.GetProxyGeneration(),
                        Ydb::StatusIds::BAD_SESSION,
                        proxy ? "ProxyGeneration mismatch" : "Proxy is not registered"));
                return true;
            }

            Self->PersistStrictMarker(db);

            auto* session = Self->Sessions.FindPtr(Record.GetSessionId());
            if (!session || session->OwnerProxy != proxy) {
                Events.emplace_back(Sender, Cookie,
                    new TEvKesus::TEvDeleteSemaphoreResult(
                        Record.GetProxyGeneration(),
                        session ? Ydb::StatusIds::BAD_SESSION : Ydb::StatusIds::SESSION_EXPIRED,
                        session ? "Session not attached" : "Session does not exist"));
                return true;
            }
        } else {
            Self->PersistStrictMarker(db);
        }

        auto* semaphore = Self->SemaphoresByName.Value(Record.GetName(), nullptr);
        if (!semaphore) {
            Events.emplace_back(Sender, Cookie,
                new TEvKesus::TEvDeleteSemaphoreResult(
                    Record.GetProxyGeneration(),
                    Ydb::StatusIds::NOT_FOUND,
                    "Semaphore does not exist"));
            return true;
        }

        if (semaphore->Ephemeral) {
            Events.emplace_back(Sender, Cookie,
                new TEvKesus::TEvDeleteSemaphoreResult(
                    Record.GetProxyGeneration(),
                    Ydb::StatusIds::PRECONDITION_FAILED,
                    "Cannot delete ephemeral semaphores"));
            return true;
        }

        if (!semaphore->IsEmpty() && !Record.GetForce()) {
            Events.emplace_back(Sender, Cookie,
                new TEvKesus::TEvDeleteSemaphoreResult(
                    Record.GetProxyGeneration(),
                    Ydb::StatusIds::PRECONDITION_FAILED,
                    "Semaphore not empty"));
            return true;
        }

        Self->DoDeleteSemaphore(db, semaphore, Events);
        Events.emplace_back(Sender, Cookie,
            new TEvKesus::TEvDeleteSemaphoreResult(Record.GetProxyGeneration()));
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::KESUS_TABLET,
            "[" << Self->TabletID() << "] TTxSemaphoreDelete::Complete (sender=" << Sender
                << ", cookie=" << Cookie << ")");
        Self->RemoveSessionTx(Record.GetSessionId());

        for (auto& ev : Events) {
            ctx.Send(ev.Recipient, ev.Event.Release(), 0, ev.Cookie);
        }
    }
};

void TKesusTablet::Handle(TEvKesus::TEvDeleteSemaphore::TPtr& ev) {
    const auto& record = ev->Get()->Record;
    VerifyKesusPath(record.GetKesusPath());
    TabletCounters->Cumulative()[COUNTER_REQS_SEMAPHORE_DELETE].Increment(1);

    Execute(new TTxSemaphoreDelete(this, ev->Sender, ev->Cookie, record), TActivationContext::AsActorContext());
}

}
}
