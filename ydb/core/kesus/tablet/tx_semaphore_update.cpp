#include "tablet_impl.h"

namespace NKikimr {
namespace NKesus {

struct TKesusTablet::TTxSemaphoreUpdate : public TTxBase {
    const TActorId Sender;
    const ui64 Cookie;
    const NKikimrKesus::TEvUpdateSemaphore Record;

    TVector<TDelayedEvent> Events;
    THolder<TEvKesus::TEvUpdateSemaphoreResult> Reply;

    TTxSemaphoreUpdate(TSelf* self, const TActorId& sender, ui64 cookie, const NKikimrKesus::TEvUpdateSemaphore& record)
        : TTxBase(self)
        , Sender(sender)
        , Cookie(cookie)
        , Record(record)
    {
        Self->AddSessionTx(Record.GetSessionId());
    }

    TTxType GetTxType() const override { return TXTYPE_SEMAPHORE_UPDATE; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::KESUS_TABLET,
            "[" << Self->TabletID() << "] TTxSemaphoreUpdate::Execute (sender=" << Sender
                << ", cookie=" << Cookie << ", name=" << Record.GetName().Quote() << ")");

        NIceDb::TNiceDb db(txc.DB);

        if (Record.GetProxyGeneration() != 0 || Record.GetSessionId() != 0) {
            auto* proxy = Self->Proxies.FindPtr(Sender);
            if (!proxy || proxy->Generation != Record.GetProxyGeneration()) {
                Events.emplace_back(Sender, Cookie,
                    new TEvKesus::TEvUpdateSemaphoreResult(
                        Record.GetProxyGeneration(),
                        Ydb::StatusIds::BAD_SESSION,
                        proxy ? "ProxyGeneration mismatch" : "Proxy is not registered"));
                return true;
            }

            Self->PersistStrictMarker(db);

            auto* session = Self->Sessions.FindPtr(Record.GetSessionId());
            if (!session || session->OwnerProxy != proxy) {
                Events.emplace_back(Sender, Cookie,
                    new TEvKesus::TEvUpdateSemaphoreResult(
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
                new TEvKesus::TEvUpdateSemaphoreResult(
                    Record.GetProxyGeneration(),
                    Ydb::StatusIds::NOT_FOUND,
                    "Semaphore does not exist"));
            return true;
        }

        bool dataChanged = false;
        if (semaphore->Data != Record.GetData()) {
            semaphore->Data = Record.GetData();
            db.Table<Schema::Semaphores>().Key(semaphore->Id).Update(
                NIceDb::TUpdate<Schema::Semaphores::Data>(semaphore->Data));
            dataChanged = true;
        }
        Events.emplace_back(Sender, Cookie,
            new TEvKesus::TEvUpdateSemaphoreResult(Record.GetProxyGeneration()));
        semaphore->NotifyWatchers(Events, dataChanged, false);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::KESUS_TABLET,
            "[" << Self->TabletID() << "] TTxSemaphoreUpdate::Complete (sender=" << Sender
                << ", cookie=" << Cookie << ")");
        Self->RemoveSessionTx(Record.GetSessionId());

        for (auto& ev : Events) {
            ctx.Send(ev.Recipient, ev.Event.Release(), 0, ev.Cookie);
        }
    }
};

void TKesusTablet::Handle(TEvKesus::TEvUpdateSemaphore::TPtr& ev) {
    const auto& record = ev->Get()->Record;
    VerifyKesusPath(record.GetKesusPath());

    if (record.GetName().size() > MAX_SEMAPHORE_NAME) {
        Send(ev->Sender,
            new TEvKesus::TEvUpdateSemaphoreResult(
                record.GetProxyGeneration(),
                Ydb::StatusIds::BAD_REQUEST,
                "Semaphore name is too long"),
            0, ev->Cookie);
        return;
    }

    if (record.GetData().size() > MAX_SEMAPHORE_DATA) {
        Send(ev->Sender,
            new TEvKesus::TEvUpdateSemaphoreResult(
                record.GetProxyGeneration(),
                Ydb::StatusIds::BAD_REQUEST,
                "Semaphore data is too large"),
            0, ev->Cookie);
    }

    Execute(new TTxSemaphoreUpdate(this, ev->Sender, ev->Cookie, record), TActivationContext::AsActorContext());
}

}
}
