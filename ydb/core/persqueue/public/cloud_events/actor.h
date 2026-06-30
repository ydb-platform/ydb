#pragma once

#include "cloud_events.h"
#include "events_writer.h"

#include <ydb/core/base/events.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NPQ::NCloudEvents {

class TCloudEventsActor : public NActors::TActorBootstrapped<TCloudEventsActor> {
public:
    TCloudEventsActor();
    explicit TCloudEventsActor(IEventsWriter::TPtr eventsWriter);

    void Bootstrap();

private:
    IEventsWriter::TPtr EventsWriter;

    void Handle(TCloudEvent::TPtr& ev);

    STRICT_STFUNC(StateWork,
        hFunc(TCloudEvent, Handle);

        cFunc(TKikimrEvents::TEvPoisonPill::EventType, PassAway);
    )
};

} // namespace NKikimr::NPQ::NCloudEvents
