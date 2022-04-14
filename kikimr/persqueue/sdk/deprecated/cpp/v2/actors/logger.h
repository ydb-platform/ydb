#pragma once
#include "responses.h"
#include "actor_interface.h"
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/logger.h>

namespace NPersQueue {

template <ui32 EventTypeId>
class TActorLogger
    : public TActorHolder
    , public ILogger
{
public:
    using TLogEvent = TEvPQLibLog<EventTypeId>;

public:
    explicit TActorLogger(NActors::TActorSystem* actorSystem, const NActors::TActorId& parentActorID, int level)
        : TActorHolder(actorSystem, parentActorID)
        , Level(level)
    {
    }

    ~TActorLogger() override = default;

    void Log(const TString& msg, const TString& sourceId, const TString& sessionId, int level) override {
        if (!IsEnabled(level)) {
            return;
        }
        ActorSystem->Send(ParentActorID, new TLogEvent(
            msg,
            sourceId,
            sessionId,
            static_cast<NActors::NLog::EPriority>(level)
        ));
    }

    bool IsEnabled(int level) const override {
        return level <= Level;
    }

private:
    int Level;
};

}



