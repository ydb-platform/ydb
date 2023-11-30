#pragma once

#include "actorsystem.h"
#include "monotonic.h"
#include "scheduler_queue.h"
#include <ydb/library/actors/util/queue_chunk.h>
#include <library/cpp/threading/future/legacy_future.h>
#include <util/generic/hash.h>
#include <util/generic/map.h>

namespace NActors {

    class TBasicSchedulerThread: public ISchedulerThread {
        // TODO: replace with NUMA-local threads and per-thread schedules
        const TSchedulerConfig Config;

        struct TMonCounters;
        const THolder<TMonCounters> MonCounters;

        TActorSystem* ActorSystem;
        volatile ui64* CurrentTimestamp;
        volatile ui64* CurrentMonotonic;

        ui32 TotalReaders;
        TArrayHolder<NSchedulerQueue::TReader*> Readers;

        volatile bool StopFlag;

        typedef TMap<ui64, TAutoPtr<NSchedulerQueue::TQueueType>> TMomentMap; // intrasecond queues
        typedef THashMap<ui64, TAutoPtr<TMomentMap>> TScheduleMap;            // over-second schedule

        TScheduleMap ScheduleMap;

        THolder<NThreading::TLegacyFuture<void, false>> MainCycle;

        static const ui64 IntrasecondThreshold = 1048576; // ~second

        void CycleFunc();

    public:
        TBasicSchedulerThread(const TSchedulerConfig& config = TSchedulerConfig());
        ~TBasicSchedulerThread();

        void Prepare(TActorSystem* actorSystem, volatile ui64* currentTimestamp, volatile ui64* currentMonotonic) override;
        void PrepareSchedules(NSchedulerQueue::TReader** readers, ui32 scheduleReadersCount) override;

        void PrepareStart() override;
        void Start() override;
        void PrepareStop() override;
        void Stop() override;
    };

    class TMockSchedulerThread: public ISchedulerThread {
    public:
        virtual ~TMockSchedulerThread() override {
        }

        void Prepare(TActorSystem* actorSystem, volatile ui64* currentTimestamp, volatile ui64* currentMonotonic) override {
            Y_UNUSED(actorSystem);
            *currentTimestamp = TInstant::Now().MicroSeconds();
            *currentMonotonic = GetMonotonicMicroSeconds();
        }

        void PrepareSchedules(NSchedulerQueue::TReader** readers, ui32 scheduleReadersCount) override {
            Y_UNUSED(readers);
            Y_UNUSED(scheduleReadersCount);
        }

        void Start() override {
        }

        void PrepareStop() override {
        }

        void Stop() override {
        }
    };

    ISchedulerThread* CreateSchedulerThread(const TSchedulerConfig& cfg);

}
