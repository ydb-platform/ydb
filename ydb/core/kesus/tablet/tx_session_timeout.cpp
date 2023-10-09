#include "tablet_impl.h"

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/appdata.h>

namespace NKikimr {
namespace NKesus {

struct TKesusTablet::TTxSessionTimeout : public TTxBase {
    const ui64 SessionId;
    TSchedulerCookieHolder Cookie;

    TVector<TDelayedEvent> Events;

    TTxSessionTimeout(TSelf* self, ui64 sessionId, ISchedulerCookie* cookie)
        : TTxBase(self)
        , SessionId(sessionId)
        , Cookie(cookie)
    {
        Self->AddSessionTx(SessionId);
    }

    TTxType GetTxType() const override { return TXTYPE_SESSION_TIMEOUT; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::KESUS_TABLET,
            "[" << Self->TabletID() << "] TTxSessionTimeout::Execute (session=" << SessionId << ")");

        if (!Cookie.DetachEvent()) {
            // Timeout has been cancelled
            return true;
        }

        auto* session = Self->Sessions.FindPtr(SessionId);
        if (!session) {
            // Session already destroyed
            return true;
        }

        Y_ABORT_UNLESS(!session->OwnerProxy, "Timeout on attached session %" PRIu64, SessionId);

        if (session->LastOwnerProxy) {
            Events.emplace_back(session->LastOwnerProxy, 0,
                new TEvKesus::TEvSessionExpired(SessionId));
        }

        NIceDb::TNiceDb db(txc.DB);
        Self->DoDeleteSession(db, session, Events);

        Self->TabletCounters->Cumulative()[COUNTER_SESSION_TIMEOUT].Increment(1);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::KESUS_TABLET,
            "[" << Self->TabletID() << "] TTxSessionTimeout::Complete (session=" << SessionId << ")");
        Self->RemoveSessionTx(SessionId);

        for (auto& ev : Events) {
            ctx.Send(ev.Recipient, ev.Event.Release(), 0, ev.Cookie);
        }
    }
};

bool TKesusTablet::ScheduleSessionTimeout(TSessionInfo* session, const TActorContext& ctx, TDuration gracePeriod) {
    if (session->TimeoutCookie.Get()) {
        return false; // already scheduled
    }
    if (session->DetachProxy()) {
        TabletCounters->Simple()[COUNTER_SESSION_ACTIVE_COUNT].Add(-1);
    }
    if (session->TimeoutMillis == Max<ui64>()) {
        // Don't bother scheduling an infinite timeout
        session->ScheduledTimeoutDeadline = TInstant::Max();
        return true;
    }
    TDuration timeout = TDuration::MilliSeconds(session->TimeoutMillis) + gracePeriod;
    session->TimeoutCookie.Reset(ISchedulerCookie::Make3Way());
    CreateLongTimer(ctx, timeout,
        new IEventHandle(SelfId(), SelfId(),
            new TEvPrivate::TEvSessionTimeout(session->Id, session->TimeoutCookie.Get())),
        AppData(ctx)->SystemPoolId,
        session->TimeoutCookie.Get());
    session->ScheduledTimeoutDeadline = ctx.Now() + timeout;
    TabletCounters->Cumulative()[COUNTER_SCHEDULED_SESSION_TIMEOUT].Increment(1);
    return true;
}

void TKesusTablet::Handle(TEvPrivate::TEvSessionTimeout::TPtr& ev) {
    auto* msg = ev->Get();
    auto* session = Sessions.FindPtr(msg->SessionId);
    if (session && session->TimeoutCookie == msg->Cookie) {
        // Only run transaction if session still exists and cookie is still valid
        Execute(new TTxSessionTimeout(this, msg->SessionId, msg->Cookie.Release()), TActivationContext::AsActorContext());
    }
}

}
}
