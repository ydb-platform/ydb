#include "long_timer.h"
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/services/services.pb.h>

namespace NActors {

class TLongTimer : public TActor<TLongTimer> {
    const static ui64 ThresholdSec = 15;

    TMonotonic StartTime;
    TMonotonic SignalTime;
    TAutoPtr<IEventHandle> Ev;
    TSchedulerCookieHolder Cookie;

    void PoisonPill(const TActorContext &ctx) {
        return Die(ctx);
    }

    void Round(const TActorContext &ctx) {
        if (Cookie.Get() && !Cookie.Get()->IsArmed())
            return Die(ctx);

        const TMonotonic now = ctx.Monotonic();
        if (SignalTime <= now) {
            if (!Cookie.Get() || Cookie.Detach())
                ctx.ExecutorThread.Send(Ev);
            return Die(ctx);
        }

        const TDuration delta = SignalTime - now;
        if (delta <= TDuration::Seconds(ThresholdSec)) {
            ctx.ExecutorThread.Schedule(SignalTime, Ev, Cookie.Release());
            return Die(ctx);
        }

        ctx.Schedule(TDuration::Seconds(ThresholdSec), new TEvents::TEvWakeup());
    }
public:
    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::ACTORLIB_LONG_TIMER;
    }

    TLongTimer(TMonotonic startTime, TMonotonic signalTime, TAutoPtr<IEventHandle> ev, ISchedulerCookie *cookie)
        : TActor(&TThis::StateFunc)
        , StartTime(startTime)
        , SignalTime(signalTime)
        , Ev(ev)
        , Cookie(cookie)
    {}

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            CFunc(TEvents::TEvPoisonPill::EventType, PoisonPill);
            CFunc(TEvents::TEvWakeup::EventType, Round);
        }
    }

    static TActorId Create(
        const TActivationContext &ctx,
        TDuration delta,
        TAutoPtr<IEventHandle> ev,
        ui32 poolId,
        ISchedulerCookie *cookie,
        const TActorId& parentId)
    {
        if (delta.Seconds() < ThresholdSec) { // direct scheduling w/o creating actor
            ctx.ExecutorThread.Schedule(delta, ev, cookie);
            return TActorId();
        }

        TMonotonic now = ctx.Monotonic();
        TMonotonic signalTime = now + delta;
        ui64 semirandomNumber = parentId.LocalId();
        const TActorId timerActorID = ctx.ExecutorThread.ActorSystem->Register(new TLongTimer(now, signalTime, ev, cookie), TMailboxType::HTSwap, poolId, semirandomNumber, parentId);
        ctx.ExecutorThread.Schedule(TDuration::Seconds(ThresholdSec), new IEventHandle(timerActorID, timerActorID, new TEvents::TEvWakeup()));

        return timerActorID;
    }

    static TActorId Create(
        TDuration delta,
        TAutoPtr<IEventHandle> ev,
        ui32 poolId,
        ISchedulerCookie *cookie)
    {
        if (delta.Seconds() < ThresholdSec) { // direct scheduling w/o creating actor
            TActivationContext::Schedule(delta, ev, cookie);
            return TActorId();
        }

        TMonotonic now = TActivationContext::Monotonic();
        TMonotonic signalTime = now + delta;
        const TActorId timerActorID = TActivationContext::Register(new TLongTimer(now, signalTime, ev, cookie), TActorId(), TMailboxType::HTSwap, poolId);
        TActivationContext::Schedule(TDuration::Seconds(ThresholdSec), new IEventHandle(timerActorID, timerActorID, new TEvents::TEvWakeup()));

        return timerActorID;
    }
};

TActorId CreateLongTimer(const TActorContext &ctx, TDuration delta, TAutoPtr<IEventHandle> ev, ui32 poolId, ISchedulerCookie *cookie) {
    return TLongTimer::Create(ctx, delta, ev, poolId, cookie, ctx.SelfID);
}

TActorId CreateLongTimer(TDuration delta, TAutoPtr<IEventHandle> ev, ui32 poolId, ISchedulerCookie *cookie) {
    return TLongTimer::Create(delta, ev, poolId, cookie);
}

}
