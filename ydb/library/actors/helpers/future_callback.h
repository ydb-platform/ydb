#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NActors {

template <typename EventType>
struct TActorFutureCallback : TActor<TActorFutureCallback<EventType>> {
    using TCallback = std::function<void(TAutoPtr<TEventHandle<EventType>>&)>;
    using TBase = TActor<TActorFutureCallback<EventType>>;
    TCallback Callback;

    static constexpr IActor::EActivityType ActorActivityType() {
        return IActor::EActivityType::ACTOR_FUTURE_CALLBACK;
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

} // NActors
