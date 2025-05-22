#pragma once

#include "defs.h"

//#include "actorsystem.h"
#include "event.h"
#include "executor_pool.h"
#include "lease.h"
#include "mailbox.h"
#include "mon_stats.h"
#include "thread_context.h"

#include <ydb/library/actors/util/cpumask.h>
#include <ydb/library/actors/util/datetime.h>
#include <ydb/library/actors/util/intrinsics.h>
#include <ydb/library/actors/util/thread.h>


namespace NActors {
    struct TSharedExecutorThreadCtx;

    struct TCpuSensor {
        ui64 Value = 0;

        ui64 GetDiff() {
            ui64 prev = std::exchange(Value, ThreadCPUTime());
            return Value - prev;
        }
    };
    
    struct TExecutionStats {
        TExecutorThreadStats* Stats = nullptr; // pool stats
        TCpuSensor CpuSensor;


        TExecutionStats();

        ~TExecutionStats();

#ifdef ACTORSLIB_COLLECT_EXEC_STATS
        void GetCurrentStats(TExecutorThreadStats& statsCopy) const {
            statsCopy = TExecutorThreadStats();
            statsCopy.Aggregate(*Stats);
        }

        void SetCurrentActivationTime(ui32 activityType, i64 elapsed) {
            RelaxedStore(&Stats->CurrentActivationTime.LastActivity, activityType);
            RelaxedStore(&Stats->CurrentActivationTime.TimeUs, (elapsed > 0 ? elapsed : 0));
        }

        void AddElapsedCycles(ui32 activityType, i64 elapsed) {
            if (Y_LIKELY(elapsed > 0) && activityType != Max<ui32>()) {
                Y_DEBUG_ABORT_UNLESS(activityType < Stats->MaxActivityType());
                RelaxedStore(&Stats->ElapsedTicks, RelaxedLoad(&Stats->ElapsedTicks) + elapsed);
                RelaxedStore(&Stats->ElapsedTicksByActivity[activityType], RelaxedLoad(&Stats->ElapsedTicksByActivity[activityType]) + elapsed);
            }
        }

        void AddParkedCycles(i64 elapsed) {
            if (Y_LIKELY(elapsed > 0)) {
                RelaxedStore(&Stats->ParkedTicks, RelaxedLoad(&Stats->ParkedTicks) + elapsed);
            }
        }

        void AddOveraddedCpuUs(i64 elapsed) {
            if (Y_LIKELY(elapsed > 0)) {
                RelaxedStore(&Stats->OveraddedCpuUs, RelaxedLoad(&Stats->OveraddedCpuUs) + elapsed);
                RelaxedStore(&Stats->CpuUs, (ui64)RelaxedLoad(&Stats->CpuUs) + elapsed);
            }
        }

        void IncrementSentEvents() {
            RelaxedStore(&Stats->SentEvents, RelaxedLoad(&Stats->SentEvents) + 1);
        }

        void IncrementPreemptedEvents() {
            RelaxedStore(&Stats->PreemptedEvents, RelaxedLoad(&Stats->PreemptedEvents) + 1);
        }

        void DecrementActorsAliveByActivity(ui32 activityType) {
            if (activityType >= Stats->MaxActivityType()) {
                activityType = 0;
            }
            RelaxedStore(&Stats->ActorsAliveByActivity[activityType], Stats->ActorsAliveByActivity[activityType] - 1);
        }

        inline void IncrementNonDeliveredEvents() {
            RelaxedStore(&Stats->NonDeliveredEvents, RelaxedLoad(&Stats->NonDeliveredEvents) + 1);
        }

        inline void IncrementMailboxPushedOutByTailSending() {
            RelaxedStore(&Stats->MailboxPushedOutByTailSending, RelaxedLoad(&Stats->MailboxPushedOutByTailSending) + 1);
        }

        inline void IncrementMailboxPushedOutBySoftPreemption() {
            RelaxedStore(&Stats->MailboxPushedOutBySoftPreemption, RelaxedLoad(&Stats->MailboxPushedOutBySoftPreemption) + 1);
        }

        inline void IncrementMailboxPushedOutByTime() {
            RelaxedStore(&Stats->MailboxPushedOutByTime, RelaxedLoad(&Stats->MailboxPushedOutByTime) + 1);
        }

        inline void IncrementMailboxPushedOutByEventCount() {
            RelaxedStore(&Stats->MailboxPushedOutByEventCount, RelaxedLoad(&Stats->MailboxPushedOutByEventCount) + 1);
        }

        inline void IncrementEmptyMailboxActivation() {
            RelaxedStore(&Stats->EmptyMailboxActivation, RelaxedLoad(&Stats->EmptyMailboxActivation) + 1);
        }

        double AddActivationStats(i64 scheduleTs, i64 deliveredTs) {
            i64 ts = deliveredTs > scheduleTs ? deliveredTs - scheduleTs : 0;
            double usec = NHPTimer::GetSeconds(ts) * 1000000.0;
            Stats->ActivationTimeHistogram.Add(usec);
            RelaxedStore(&Stats->WorstActivationTimeUs, Max(Stats->WorstActivationTimeUs, (ui64)usec));
            return usec;
        }

        ui64 AddEventDeliveryStats(i64 sentTs, i64 deliveredTs) {
            ui64 usecDeliv = deliveredTs > sentTs ? NHPTimer::GetSeconds(deliveredTs - sentTs) * 1000000 : 0;
            Stats->EventDeliveryTimeHistogram.Add(usecDeliv);
            return usecDeliv;
        }

        i64 AddEventProcessingStats(i64 deliveredTs, i64 processedTs, ui32 activityType, ui64 scheduled) {
            i64 elapsed = Max<i64>(0, processedTs - deliveredTs);
            ui64 usecElapsed = NHPTimer::GetSeconds(elapsed) * 1000000;
            activityType = (activityType >= Stats->MaxActivityType()) ? 0 : activityType;
            Stats->EventProcessingCountHistogram.Add(usecElapsed);
            Stats->EventProcessingTimeHistogram.Add(usecElapsed, elapsed);
            RelaxedStore(&Stats->ReceivedEvents, RelaxedLoad(&Stats->ReceivedEvents) + 1);
            RelaxedStore(&Stats->ReceivedEventsByActivity[activityType], RelaxedLoad(&Stats->ReceivedEventsByActivity[activityType]) + 1);
            RelaxedStore(&Stats->ScheduledEventsByActivity[activityType], RelaxedLoad(&Stats->ScheduledEventsByActivity[activityType]) + scheduled);
            return elapsed;
        }

        void UpdateActorsStats(size_t dyingActorsCnt, IExecutorPool* pool) {
            if (dyingActorsCnt) {
                AtomicAdd(pool->DestroyedActors, dyingActorsCnt);
            }
            RelaxedStore(&Stats->PoolDestroyedActors, (ui64)RelaxedLoad(&pool->DestroyedActors));
            RelaxedStore(&Stats->PoolActorRegistrations, (ui64)RelaxedLoad(&pool->ActorRegistrations));
            RelaxedStore(&Stats->PoolAllocatedMailboxes, pool->GetMailboxTable()->GetAllocatedMailboxCountFast());
        }

        void UpdateThreadTime() {
            ui64 cpuUs = CpuSensor.GetDiff();
            ui64 overaddedCpuUs = RelaxedLoad(&Stats->OveraddedCpuUs);
            if (cpuUs < overaddedCpuUs) {
                RelaxedStore(&Stats->OveraddedCpuUs, overaddedCpuUs - cpuUs);
            } else if (overaddedCpuUs > 0) {
                RelaxedStore(&Stats->OveraddedCpuUs, (ui64)0);
                RelaxedStore(&Stats->CpuUs, (ui64)RelaxedLoad(&Stats->CpuUs) + cpuUs - overaddedCpuUs);
            } else {
                RelaxedStore(&Stats->CpuUs, (ui64)RelaxedLoad(&Stats->CpuUs) + cpuUs);
            }
            RelaxedStore(&Stats->SafeElapsedTicks, (ui64)RelaxedLoad(&Stats->ElapsedTicks));
            RelaxedStore(&Stats->SafeParkedTicks, (ui64)RelaxedLoad(&Stats->ParkedTicks));
        }

        void IncreaseNotEnoughCpuExecutions() {
            RelaxedStore(&Stats->NotEnoughCpuExecutions,
                    (ui64)RelaxedLoad(&Stats->NotEnoughCpuExecutions) + 1);
        }
#else
        void GetCurrentStats(TExecutorThreadStats&) const {}
        void SetCurrentActivationTime(ui32, i64) {}
        inline void AddElapsedCycles(ui32, i64) {}
        inline void AddParkedCycles(i64) {}
        inline void IncrementSentEvents() {}
        inline void IncrementPreemptedEvents() {}
        inline void IncrementMailboxPushedOutByTailSending() {}
        inline void IncrementMailboxPushedOutBySoftPreemption() {}
        inline void IncrementMailboxPushedOutByTime() {}
        inline void IncrementMailboxPushedOutByEventCount() {}
        inline void IncrementEmptyMailboxActivation() {}
        void DecrementActorsAliveByActivity(ui32) {}
        void IncrementNonDeliveredEvents() {}
        double AddActivationStats(i64, i64) { return 0; }
        ui64 AddEventDeliveryStats(i64, i64) { return 0; }
        i64 AddEventProcessingStats(i64, i64, ui32, ui64) { return 0; }
        void UpdateActorsStats(size_t, IExecutorPool*) {}
        void UpdateThreadTime() {}
        void IncreaseNotEnoughCpuExecutions() {}
#endif

        void Switch(TExecutorThreadStats* stats)
        {
            Stats = stats;
        }
    };
}
