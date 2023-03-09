#pragma once

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/hfunc.h>

namespace NActors {

template <typename EventType>
struct TActorFutureCallback : TActor<TActorFutureCallback<EventType>> {
    using TCallback = std::function<void(TAutoPtr<TEventHandleFat<EventType>>&)>;
    using TBase = TActor<TActorFutureCallback<EventType>>;
    TCallback Callback;

    static constexpr IActor::EActivityType ActorActivityType() {
        return IActor::ACTOR_FUTURE_CALLBACK;
    }

    TActorFutureCallback(TCallback&& callback)
        : TBase(&TActorFutureCallback::StateWaitForEvent)
        , Callback(std::move(callback))
    {}

    STRICT_STFUNC(StateWaitForEvent,
        HFunc(EventType, Handle)
    )

    void Handle(typename EventType::TPtr ev, const TActorContext& ctx) {
        Callback(ev);
        TBase::Die(ctx);
    }
};

template <typename EventType>
struct TActorFutureCallbackLight : TActor<TActorFutureCallbackLight<EventType>> {
    using TCallback = std::function<void(TAutoPtr<EventType>&)>;
    using TBase = TActor<TActorFutureCallbackLight<EventType>>;
    TCallback Callback;

    static constexpr IActor::EActivityType ActorActivityType() {
        return IActor::ACTOR_FUTURE_CALLBACK;
    }

    TActorFutureCallbackLight(TCallback&& callback)
        : TBase(&TActorFutureCallbackLight::StateWaitForEvent)
        , Callback(std::move(callback))
    {}

    STRICT_LIGHTFN(StateWaitForEvent,
        hFunc(EventType, Handle)
    )

    void Handle(typename EventType::TPtr ev) {
        Callback(ev);
        TBase::PassAway();
    }
};

} // NActors
