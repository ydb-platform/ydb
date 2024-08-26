#pragma once

#include "defs.h"

//#include "actorsystem.h"
#include "event.h"
#include "executor_pool.h"
#include "lease.h"
#include "mailbox.h"
#include "mon_stats.h"

#include <ydb/library/actors/util/cpumask.h>
#include <ydb/library/actors/util/datetime.h>
#include <ydb/library/actors/util/intrinsics.h>
#include <ydb/library/actors/util/thread.h>

#include <library/cpp/lwtrace/shuttle.h>

namespace NActors {
    struct TSharedExecutorThreadCtx;

    struct TCpuSensor {
        ui64 Value = 0;

        ui64 GetDiff() {
            ui64 prev = std::exchange(Value, ThreadCPUTime());
            return Value - prev;
        }
    };
    
    struct TWorkerContext {
        TWorkerId WorkerId;
        const TCpuId CpuId;
        TLease Lease;
        IExecutorPool* Executor = nullptr;
        TMailboxTable* MailboxTable = nullptr;
        ui64 TimePerMailboxTs = 0;
        ui32 EventsPerMailbox = 0;
        ui64 SoftDeadlineTs = ui64(-1);
        TExecutorThreadStats* Stats = &WorkerStats; // pool stats
        TExecutorThreadStats WorkerStats;
        TPoolId PoolId = MaxPools;
        mutable NLWTrace::TOrbit Orbit;
        bool IsNeededToWaitNextActivation = true;
        i64 HPStart = 0;
        ui32 ExecutedEvents = 0;
        TSharedExecutorThreadCtx *SharedThread = nullptr;
        TCpuSensor CpuSensor;
        

        TWorkerContext(TWorkerId workerId, TCpuId cpuId)
            : WorkerId(workerId)
            , CpuId(cpuId)
            , Lease(WorkerId, NeverExpire)
        {}

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
            if (Y_LIKELY(elapsed > 0)) {
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

        void AddBlockedCycles(i64 elapsed) {
            RelaxedStore(&Stats->BlockedTicks, RelaxedLoad(&Stats->BlockedTicks) + elapsed);
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
            i64 elapsed = processedTs - deliveredTs;
            ui64 usecElapsed = NHPTimer::GetSeconds(elapsed) * 1000000;
            activityType = (activityType >= Stats->MaxActivityType()) ? 0 : activityType;
            Stats->EventProcessingCountHistogram.Add(usecElapsed);
            Stats->EventProcessingTimeHistogram.Add(usecElapsed, elapsed);
            RelaxedStore(&Stats->ReceivedEvents, RelaxedLoad(&Stats->ReceivedEvents) + 1);
            RelaxedStore(&Stats->ReceivedEventsByActivity[activityType], RelaxedLoad(&Stats->ReceivedEventsByActivity[activityType]) + 1);
            RelaxedStore(&Stats->ScheduledEventsByActivity[activityType], RelaxedLoad(&Stats->ScheduledEventsByActivity[activityType]) + scheduled);
            return elapsed;
        }

        void UpdateActorsStats(size_t dyingActorsCnt) {
            if (dyingActorsCnt) {
                AtomicAdd(Executor->DestroyedActors, dyingActorsCnt);
            }
            RelaxedStore(&Stats->PoolDestroyedActors, (ui64)RelaxedLoad(&Executor->DestroyedActors));
            RelaxedStore(&Stats->PoolActorRegistrations, (ui64)RelaxedLoad(&Executor->ActorRegistrations));
            RelaxedStore(&Stats->PoolAllocatedMailboxes, MailboxTable->GetAllocatedMailboxCount());
        }

        void UpdateThreadTime() {
            RelaxedStore(&Stats->SafeElapsedTicks, (ui64)RelaxedLoad(&Stats->ElapsedTicks));
            RelaxedStore(&Stats->CpuUs, (ui64)RelaxedLoad(&Stats->CpuUs) + CpuSensor.GetDiff());
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
        inline void AddBlockedCycles(i64) {}
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

        void Switch(IExecutorPool* executor,
                    TMailboxTable* mailboxTable,
                    ui64 timePerMailboxTs,
                    ui32 eventsPerMailbox,
                    ui64 softDeadlineTs,
                    TExecutorThreadStats* stats)
        {
            Executor = executor;
            MailboxTable = mailboxTable;
            TimePerMailboxTs = timePerMailboxTs;
            EventsPerMailbox = eventsPerMailbox;
            SoftDeadlineTs = softDeadlineTs;
            Stats = stats;
            PoolId = Executor ? Executor->PoolId : MaxPools;
        }

        void SwitchToIdle() {
            Executor = nullptr;
            MailboxTable = nullptr;
            //Stats = &WorkerStats; // TODO: in actorsystem 2.0 idle stats cannot be related to specific pool
            PoolId = MaxPools;
        }
    };
}
