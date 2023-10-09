#include "tablet_impl.h"

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/appdata.h>

namespace NKikimr {
namespace NKesus {

struct TKesusTablet::TTxSemaphoreTimeout : public TTxBase {
    const ui64 SessionId;
    const ui64 SemaphoreId;
    TSchedulerCookieHolder Cookie;

    TVector<TDelayedEvent> Events;

    TTxSemaphoreTimeout(TSelf* self, ui64 sessionId, ui64 semaphoreId, ISchedulerCookie* cookie)
        : TTxBase(self)
        , SessionId(sessionId)
        , SemaphoreId(semaphoreId)
        , Cookie(cookie)
    {
        Self->AddSessionTx(sessionId);
    }

    TTxType GetTxType() const override { return TXTYPE_SEMAPHORE_TIMEOUT; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::KESUS_TABLET,
            "[" << Self->TabletID() << "] TTxSemaphoreTimeout::Execute (session=" << SessionId
                << ", semaphore=" << SemaphoreId << ")");

        if (!Cookie.DetachEvent()) {
            // Timeout has been cancelled
            return true;
        }

        auto* session = Self->Sessions.FindPtr(SessionId);
        if (!session) {
            // Session already destroyed
            return true;
        }

        auto* waiter = session->WaitingSemaphores.FindPtr(SemaphoreId);
        if (!waiter) {
            // Session no longer waiting for this semaphore
            return true;
        }

        auto* semaphore = Self->Semaphores.FindPtr(SemaphoreId);
        Y_ABORT_UNLESS(semaphore);

        NIceDb::TNiceDb db(txc.DB);

        if (auto* proxy = session->OwnerProxy) {
            session->ConsumeSemaphoreWaitCookie(semaphore, [&](ui64 cookie) {
                Events.emplace_back(proxy->ActorID, cookie,
                    new TEvKesus::TEvAcquireSemaphoreResult(proxy->Generation, false));
            });
        }

        Self->DoDeleteSessionSemaphore(db, semaphore, waiter, Events);
        session->WaitingSemaphores.erase(SemaphoreId);

        Self->TabletCounters->Cumulative()[COUNTER_ACQUIRE_TIMEOUT].Increment(1);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::KESUS_TABLET,
            "[" << Self->TabletID() << "] TTxSemaphoreTimeout::Complete (session=" << SessionId
                << ", semaphore=" << SemaphoreId << ")");
        Self->RemoveSessionTx(SessionId);

        for (auto& ev : Events) {
            ctx.Send(ev.Recipient, ev.Event.Release(), 0, ev.Cookie);
        }
    }
};

bool TKesusTablet::ScheduleWaiterTimeout(ui64 semaphoreId, TSemaphoreWaiterInfo* waiter, const TActorContext& ctx) {
    if (waiter->TimeoutCookie.Get()) {
        return false; // already scheduled
    }
    if (waiter->TimeoutMillis == Max<ui64>()) {
        // Don't bother scheduling an infinite timeout
        waiter->ScheduledTimeoutDeadline = TInstant::Max();
        return true;
    }
    TDuration timeout = TDuration::MilliSeconds(waiter->TimeoutMillis);
    waiter->TimeoutCookie.Reset(ISchedulerCookie::Make3Way());
    CreateLongTimer(ctx, timeout,
        new IEventHandle(SelfId(), SelfId(),
            new TEvPrivate::TEvAcquireSemaphoreTimeout(waiter->SessionId, semaphoreId, waiter->TimeoutCookie.Get())),
        AppData(ctx)->SystemPoolId,
        waiter->TimeoutCookie.Get());
    waiter->ScheduledTimeoutDeadline = ctx.Now() + timeout;
    TabletCounters->Cumulative()[COUNTER_SCHEDULED_ACQUIRE_TIMEOUT].Increment(1);
    return true;
}

void TKesusTablet::Handle(TEvPrivate::TEvAcquireSemaphoreTimeout::TPtr& ev) {
    auto* msg = ev->Get();
    auto* session = Sessions.FindPtr(msg->SessionId);
    if (!session) {
        return;
    }
    auto* waiter = session->WaitingSemaphores.FindPtr(msg->SemaphoreId);
    if (!waiter) {
        return;
    }
    if (waiter->TimeoutCookie == msg->Cookie) {
        // Only run transaction if waiter still exists and cookie is still valid
        Execute(new TTxSemaphoreTimeout(this, msg->SessionId, msg->SemaphoreId, msg->Cookie.Release()), TActivationContext::AsActorContext());
    }
}

}
}
