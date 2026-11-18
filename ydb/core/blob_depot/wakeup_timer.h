#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NBlobDepot {

    // An earlier deadline supersedes the outstanding event. Its cookie identifies stale wakeups.
    class TWakeupTimer {
        NActors::TMonotonic Deadline = NActors::TMonotonic::Max();

    public:
        void Schedule(NActors::TMonotonic when, NActors::TActorId recipient, ui32 eventType) {
            if (when < Deadline) {
                Deadline = when;
                NActors::TActivationContext::Schedule(when, new NActors::IEventHandle(eventType, 0,
                    recipient, {}, nullptr, when.GetValue()));
            }
        }

        bool Consume(ui64 cookie) {
            if (cookie != Deadline.GetValue()) {
                return false;
            }
            Deadline = NActors::TMonotonic::Max();
            return true;
        }
    };

}
