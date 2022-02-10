#pragma once

#include "actor.h"
#include "event_local.h"
#include "events.h"
#include "scheduler_basic.h"

namespace NActors {
    struct TEvSchedulerInitialize : TEventLocal<TEvSchedulerInitialize, TEvents::TSystem::Bootstrap> {
        TVector<NSchedulerQueue::TReader*> ScheduleReaders;
        volatile ui64* CurrentTimestamp;
        volatile ui64* CurrentMonotonic;

        TEvSchedulerInitialize(const TVector<NSchedulerQueue::TReader*>& scheduleReaders, volatile ui64* currentTimestamp, volatile ui64* currentMonotonic)
            : ScheduleReaders(scheduleReaders)
            , CurrentTimestamp(currentTimestamp)
            , CurrentMonotonic(currentMonotonic)
        {
        }
    };

    IActor* CreateSchedulerActor(const TSchedulerConfig& cfg);

    inline TActorId MakeSchedulerActorId() {
        char x[12] = {'s', 'c', 'h', 'e', 'd', 'u', 'l', 'e', 'r', 's', 'e', 'r'};
        return TActorId(0, TStringBuf(x, 12));
    }

}
