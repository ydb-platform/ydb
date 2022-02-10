#pragma once

namespace NActors {
    template <typename TEvent> 
    class TWatchdogTimer {
        using TCallback = std::function<void()>;

        const TDuration Timeout;
        const TCallback Callback;

        TInstant LastResetTimestamp;
        TEvent* ExpectedEvent = nullptr; 
        ui32 Iteration = 0;

        static constexpr ui32 NumIterationsBeforeFiring = 2;

    public:
        TWatchdogTimer(TDuration timeout, TCallback callback)
            : Timeout(timeout)
            , Callback(std::move(callback))
        { 
        } 

        void Arm(const TActorIdentity& actor) {
            if (Timeout != TDuration::Zero() && Timeout != TDuration::Max()) {
                Schedule(Timeout, actor);
                Reset();
            }
        }

        void Reset() {
            LastResetTimestamp = TActivationContext::Now();
        }

        void Disarm() {
            ExpectedEvent = nullptr;
        }

        void operator()(typename TEvent::TPtr& ev) {
            if (ev->Get() == ExpectedEvent) {
                const TInstant now = TActivationContext::Now();
                const TInstant barrier = LastResetTimestamp + Timeout;
                if (now < barrier) {
                    // the time hasn't come yet
                    Schedule(barrier - now, TActorIdentity(ev->Recipient));
                } else if (Iteration < NumIterationsBeforeFiring) {
                    // time has come, but we will still give actor a chance to process some messages and rearm timer
                    ++Iteration;
                    TActivationContext::Send(ev.Release()); // send this event into queue once more
                } else {
                    // no chance to disarm, fire callback
                    Callback();
                    ExpectedEvent = nullptr;
                    Iteration = 0;
                }
            }
        }

    private:
        void Schedule(TDuration timeout, const TActorIdentity& actor) {
            auto ev = MakeHolder<TEvent>();
            ExpectedEvent = ev.Get();
            Iteration = 0;
            actor.Schedule(timeout, ev.Release());
        }
    };

} 
