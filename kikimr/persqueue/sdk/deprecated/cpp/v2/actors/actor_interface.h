#pragma once
#include "fake_actor.h"

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/actorid.h>
#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/actors/core/events.h>

namespace NPersQueue {

struct IActorInterface {
    virtual ~IActorInterface() = default;

    // Sender of all messages from PQLib objects.
    virtual NActors::TActorId GetActorID() const noexcept = 0;
};

class TActorHolder : public IActorInterface
{
protected:
    TActorHolder(NActors::TActorSystem* actorSystem, const NActors::TActorId& parentActorID)
        : ActorSystem(actorSystem)
        , ParentActorID(parentActorID)
    {
        ActorID = ActorSystem->Register(new TFakeActor());
    }

    ~TActorHolder() {
        ActorSystem->Send(new NActors::IEventHandle(ActorID, ActorID, new NActors::TEvents::TEvPoisonPill()));
    }

    template <class TResponseEvent>
    void Subscribe(ui64 requestId, NThreading::TFuture<typename TResponseEvent::TResponseType>&& future) {
        auto handler = [
            requestId = requestId,
            actorSystem = ActorSystem,
            actorID = ActorID,
            parentActorID = ParentActorID
        ](const NThreading::TFuture<typename TResponseEvent::TResponseType>& future) {
            actorSystem->Send(new NActors::IEventHandle(
                parentActorID,
                actorID,
                new TResponseEvent(typename TResponseEvent::TResponseType(future.GetValue()), requestId)
            ));
        };
        future.Subscribe(handler);
    }

public:
    NActors::TActorId GetActorID() const noexcept override {
        return ActorID;
    }

protected:
    NActors::TActorSystem* ActorSystem;
    NActors::TActorId ParentActorID;
    NActors::TActorId ActorID;
};

} // namespace NPersQueue
