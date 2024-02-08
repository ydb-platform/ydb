#pragma once

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NActors {

    class TSlowpokeActor : public TActorBootstrapped<TSlowpokeActor> {
        const TDuration Duration;
        const TDuration SleepMin;
        const TDuration SleepMax;
        const TDuration RescheduleMin;
        const TDuration RescheduleMax;

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::INTERCONNECT_COMMON;
        }

        TSlowpokeActor(TDuration duration, TDuration sleepMin, TDuration sleepMax, TDuration rescheduleMin, TDuration rescheduleMax)
            : Duration(duration)
            , SleepMin(sleepMin)
            , SleepMax(sleepMax)
            , RescheduleMin(rescheduleMin)
            , RescheduleMax(rescheduleMax)
        {}

        void Bootstrap(const TActorContext& ctx) {
            Become(&TThis::StateFunc, ctx, Duration, new TEvents::TEvPoisonPill);
            HandleWakeup(ctx);
        }

        void HandleWakeup(const TActorContext& ctx) {
            Sleep(RandomDuration(SleepMin, SleepMax));
            ctx.Schedule(RandomDuration(RescheduleMin, RescheduleMax), new TEvents::TEvWakeup);
        }

        static TDuration RandomDuration(TDuration min, TDuration max) {
            return min + TDuration::FromValue(RandomNumber<ui64>(max.GetValue() - min.GetValue() + 1));
        }

        STRICT_STFUNC(StateFunc,
            CFunc(TEvents::TSystem::PoisonPill, Die)
            CFunc(TEvents::TSystem::Wakeup, HandleWakeup)
        )
    };

} // NActors
