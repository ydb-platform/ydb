#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>

#include <utility>

namespace NKikimr::NBlobDepot {

    // One outstanding wakeup for successive I/O deadlines, which only move forward.
    class TS3RequestTimer {
        TMonotonic Deadline;
        bool Scheduled = false;

    public:
        static constexpr TDuration Timeout = TDuration::Minutes(2);

        void Arm(TMonotonic deadline = NActors::TActivationContext::Monotonic() + Timeout) {
            Deadline = deadline;
            if (!std::exchange(Scheduled, true)) {
                NActors::TActivationContext::AsActorContext().Schedule(deadline, new NActors::TEvents::TEvWakeup);
            }
        }

        void Clear() { Deadline = {}; }

        void SendWithTimeout(NActors::TActorId recipient, NActors::IEventBase* request) {
            Arm();
            NActors::TActivationContext::AsActorContext().Send(recipient, request, NActors::IEventHandle::FlagTrackDelivery);
        }

        bool OnWakeup() {
            Scheduled = false;
            if (!Deadline) {
                return false;
            }
            if (Deadline <= NActors::TActivationContext::Monotonic()) {
                Clear();
                return true;
            }
            Arm(Deadline);
            return false;
        }
    };

}
