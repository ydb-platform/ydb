#pragma once

namespace NActors {
    template<typename TEvent>
    class TWatchdogTimer {
        using TCallback = std::function<void()>;

        const TDuration Timeout;
        const TCallback Callback;

        TMonotonic TriggerTimestamp = TMonotonic::Max();
        bool EventScheduled = false;
        ui32 Iteration;

        static constexpr ui32 NumIterationsBeforeFiring = 2;

    public:
        TWatchdogTimer(TDuration timeout, TCallback callback)
            : Timeout(timeout)
            , Callback(std::move(callback))
        {}

        void Rearm(const TActorIdentity& actor) {
            if (Timeout != TDuration::Zero() && Timeout != TDuration::Max()) {
                TriggerTimestamp = TActivationContext::Monotonic() + Timeout;
                Iteration = 0;
                Schedule(actor);
            }
        }

        void Disarm() {
            TriggerTimestamp = TMonotonic::Max();
        }

        bool Armed() const {
            return TriggerTimestamp != TMonotonic::Max();
        }

        void operator()(typename TEvent::TPtr& ev) {
            Y_DEBUG_ABORT_UNLESS(EventScheduled);
            EventScheduled = false;
            if (!Armed()) {
                // just do nothing
            } else if (TActivationContext::Monotonic() < TriggerTimestamp) {
                // the time hasn't come yet
                Schedule(TActorIdentity(ev->Recipient));
            } else if (Iteration < NumIterationsBeforeFiring) {
                // time has come, but we will still give actor a chance to process some messages and rearm timer
                ++Iteration;
                TActivationContext::Send(ev.Release()); // send this event into queue once more
                EventScheduled = true;
            } else {
                // no chance to disarm, fire callback
                Disarm();
                Callback();
            }
        }

    private:
        void Schedule(const TActorIdentity& actor) {
            Y_DEBUG_ABORT_UNLESS(Armed());
            if (!EventScheduled) {
                actor.Schedule(TriggerTimestamp, new TEvent);
                EventScheduled = true;
            }
        }
    };

}
