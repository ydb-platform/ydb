#include "tablet_impl.h"

namespace NKikimr {
namespace NKesus {

struct TKesusTablet::TTxSemaphoreCreate : public TTxBase {
    const TActorId Sender;
    const ui64 Cookie;
    const NKikimrKesus::TEvCreateSemaphore Record;

    THolder<TEvKesus::TEvCreateSemaphoreResult> Reply;

    TTxSemaphoreCreate(TSelf* self, const TActorId& sender, ui64 cookie, const NKikimrKesus::TEvCreateSemaphore& record)
        : TTxBase(self)
        , Sender(sender)
        , Cookie(cookie)
        , Record(record)
    {
        Self->AddSessionTx(Record.GetSessionId());
    }

    TTxType GetTxType() const override { return TXTYPE_SEMAPHORE_CREATE; }

    void ReplyOk() {
        Reply.Reset(new TEvKesus::TEvCreateSemaphoreResult(Record.GetProxyGeneration()));
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::KESUS_TABLET,
            "[" << Self->TabletID() << "] TTxSemaphoreCreate::Execute (sender=" << Sender
                << ", cookie=" << Cookie << ", name=" << Record.GetName().Quote()
                << ", limit=" << Record.GetLimit() << ")");

        NIceDb::TNiceDb db(txc.DB);

        if (Record.GetProxyGeneration() != 0 || Record.GetSessionId() != 0) {
            auto* proxy = Self->Proxies.FindPtr(Sender);
            if (!proxy || proxy->Generation != Record.GetProxyGeneration()) {
                Reply.Reset(new TEvKesus::TEvCreateSemaphoreResult(
                    Record.GetProxyGeneration(),
                    Ydb::StatusIds::BAD_SESSION,
                    proxy ? "ProxyGeneration mismatch" : "Proxy is not registered"));
                return true;
            }

            Self->PersistStrictMarker(db);

            auto* session = Self->Sessions.FindPtr(Record.GetSessionId());
            if (!session || session->OwnerProxy != proxy) {
                Reply.Reset(new TEvKesus::TEvCreateSemaphoreResult(
                    Record.GetProxyGeneration(),
                    session ? Ydb::StatusIds::BAD_SESSION : Ydb::StatusIds::SESSION_EXPIRED,
                    session ? "Session not attached" : "Session does not exist"));
                return true;
            }
        } else {
            Self->PersistStrictMarker(db);
        }

        auto* semaphore = Self->SemaphoresByName.Value(Record.GetName(), nullptr);
        if (semaphore) {
            if (semaphore->Ephemeral) {
                Reply.Reset(new TEvKesus::TEvCreateSemaphoreResult(
                    Record.GetProxyGeneration(),
                    Ydb::StatusIds::PRECONDITION_FAILED,
                    "Cannot create semaphore due to ephemeral locks"));
            } else {
                Reply.Reset(new TEvKesus::TEvCreateSemaphoreResult(
                    Record.GetProxyGeneration(),
                    Ydb::StatusIds::ALREADY_EXISTS,
                    "Semaphore already exists"));
            }
            return true;
        }

        if (Self->Semaphores.size() >= MAX_SEMAPHORES_LIMIT) {
            Reply.Reset(new TEvKesus::TEvCreateSemaphoreResult(
                Record.GetProxyGeneration(),
                Ydb::StatusIds::PRECONDITION_FAILED,
                "Too many semaphores already exist"));
            return true;
        }

        ui64 semaphoreId = Self->NextSemaphoreId++;
        Y_ABORT_UNLESS(semaphoreId > 0);
        Y_ABORT_UNLESS(!Self->Semaphores.contains(semaphoreId));
        Self->PersistSysParam(db, Schema::SysParam_NextSemaphoreId, ToString(Self->NextSemaphoreId));
        semaphore = &Self->Semaphores[semaphoreId];
        semaphore->Id = semaphoreId;
        semaphore->Name = Record.GetName();
        semaphore->Data = Record.GetData();
        semaphore->Limit = Record.GetLimit();
        semaphore->Count = 0;
        semaphore->Ephemeral = false;
        Self->SemaphoresByName[semaphore->Name] = semaphore;
        db.Table<Schema::Semaphores>().Key(semaphoreId).Update(
            NIceDb::TUpdate<Schema::Semaphores::Name>(semaphore->Name),
            NIceDb::TUpdate<Schema::Semaphores::Data>(semaphore->Data),
            NIceDb::TUpdate<Schema::Semaphores::Limit>(semaphore->Limit),
            NIceDb::TUpdate<Schema::Semaphores::Ephemeral>(semaphore->Ephemeral));
        Self->TabletCounters->Simple()[COUNTER_SEMAPHORE_COUNT].Add(1);
        LOG_DEBUG_S(ctx, NKikimrServices::KESUS_TABLET,
            "[" << Self->TabletID() << "] Created new semaphore "
                << semaphoreId << " " << Record.GetName().Quote());
        ReplyOk();
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::KESUS_TABLET,
            "[" << Self->TabletID() << "] TTxSemaphoreCreate::Complete (sender=" << Sender
                << ", cookie=" << Cookie << ")");
        Self->RemoveSessionTx(Record.GetSessionId());

        Y_ABORT_UNLESS(Reply);
        ctx.Send(Sender, Reply.Release(), 0, Cookie);
    }
};

void TKesusTablet::Handle(TEvKesus::TEvCreateSemaphore::TPtr& ev) {
    const auto& record = ev->Get()->Record;
    VerifyKesusPath(record.GetKesusPath());
    TabletCounters->Cumulative()[COUNTER_REQS_SEMAPHORE_CREATE].Increment(1);

    if (record.GetLimit() <= 0) {
        Send(ev->Sender,
            new TEvKesus::TEvCreateSemaphoreResult(
                record.GetProxyGeneration(),
                Ydb::StatusIds::BAD_REQUEST,
                "Semaphores must have limit > 0"),
            0, ev->Cookie);
        return;
    }

    if (record.GetName().size() > MAX_SEMAPHORE_NAME) {
        Send(ev->Sender,
            new TEvKesus::TEvCreateSemaphoreResult(
                record.GetProxyGeneration(),
                Ydb::StatusIds::BAD_REQUEST,
                "Semaphore name is too long"),
            0, ev->Cookie);
        return;
    }

    if (record.GetData().size() > MAX_SEMAPHORE_DATA) {
        Send(ev->Sender,
            new TEvKesus::TEvCreateSemaphoreResult(
                record.GetProxyGeneration(),
                Ydb::StatusIds::BAD_REQUEST,
                "Semaphore data is too large"),
            0, ev->Cookie);
        return;
    }

    Execute(new TTxSemaphoreCreate(this, ev->Sender, ev->Cookie, record), TActivationContext::AsActorContext());
}

}
}
