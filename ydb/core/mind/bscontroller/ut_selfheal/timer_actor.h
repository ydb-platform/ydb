#pragma once

#include "defs.h"

#include "events.h"

struct TEvArmTimer : TEventLocal<TEvArmTimer, EvArmTimer> {
    const TDuration Timeout;

    TEvArmTimer(TDuration timeout)
        : Timeout(timeout)
    {}
};

struct TEvTimer : TEventLocal<TEvTimer, EvTimer> {
    std::unique_ptr<IEventHandle> HitEvent;

    TEvTimer(const IEventHandle& ev)
        : HitEvent(new IEventHandle(TEvents::TSystem::Wakeup, 0, ev.Sender, TActorId(), {}, ev.Cookie))
    {}

    void Hit() {
        TActivationContext::Send(HitEvent.release());
    }
};

class TTimerActor : public TActor<TTimerActor> {
public:
    TTimerActor()
        : TActor(&TThis::StateFunc)
    {}

    void Handle(TEvArmTimer::TPtr ev) {
        Schedule(ev->Get()->Timeout, new TEvTimer(*ev));
    }

    void Handle(TEvTimer::TPtr ev) {
        ev->Get()->Hit();
    }

    STRICT_STFUNC(StateFunc, {
        hFunc(TEvArmTimer, Handle);
        hFunc(TEvTimer, Handle);
    })
};
