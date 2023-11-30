#include "scheduler_basic.h"
#include "scheduler_queue.h"
#include "actor.h"

#include <ydb/library/actors/util/datetime.h>
#include <ydb/library/actors/util/thread.h>

#ifdef BALLOC
#include <library/cpp/balloc/optional/operators.h>
#endif

namespace NActors {

    struct TBasicSchedulerThread::TMonCounters {
        NMonitoring::TDynamicCounters::TCounterPtr TimeDelayMs;
        NMonitoring::TDynamicCounters::TCounterPtr QueueSize;
        NMonitoring::TDynamicCounters::TCounterPtr EventsSent;
        NMonitoring::TDynamicCounters::TCounterPtr EventsDropped;
        NMonitoring::TDynamicCounters::TCounterPtr EventsAdded;
        NMonitoring::TDynamicCounters::TCounterPtr Iterations;
        NMonitoring::TDynamicCounters::TCounterPtr Sleeps;
        NMonitoring::TDynamicCounters::TCounterPtr ElapsedMicrosec;

        TMonCounters(const NMonitoring::TDynamicCounterPtr& counters)
            : TimeDelayMs(counters->GetCounter("Scheduler/TimeDelayMs", false))
            , QueueSize(counters->GetCounter("Scheduler/QueueSize", false))
            , EventsSent(counters->GetCounter("Scheduler/EventsSent", true))
            , EventsDropped(counters->GetCounter("Scheduler/EventsDropped", true))
            , EventsAdded(counters->GetCounter("Scheduler/EventsAdded", true))
            , Iterations(counters->GetCounter("Scheduler/Iterations", true))
            , Sleeps(counters->GetCounter("Scheduler/Sleeps", true))
            , ElapsedMicrosec(counters->GetCounter("Scheduler/ElapsedMicrosec", true))
        { }
    };

    TBasicSchedulerThread::TBasicSchedulerThread(const TSchedulerConfig& config)
        : Config(config)
        , MonCounters(Config.MonCounters ? new TMonCounters(Config.MonCounters) : nullptr)
        , ActorSystem(nullptr)
        , CurrentTimestamp(nullptr)
        , CurrentMonotonic(nullptr)
        , TotalReaders(0)
        , StopFlag(false)
        , ScheduleMap(3600)
    {
        Y_ABORT_UNLESS(!Config.UseSchedulerActor, "Cannot create scheduler thread because Config.UseSchedulerActor# true");
    }

    TBasicSchedulerThread::~TBasicSchedulerThread() {
        Y_ABORT_UNLESS(!MainCycle);
    }

    void TBasicSchedulerThread::CycleFunc() {
#ifdef BALLOC
        ThreadDisableBalloc();
#endif
        ::SetCurrentThreadName("Scheduler");

        ui64 currentMonotonic = RelaxedLoad(CurrentMonotonic);
        ui64 throttledMonotonic = currentMonotonic;

        ui64 activeTick = AlignUp<ui64>(throttledMonotonic, IntrasecondThreshold);
        TAutoPtr<TMomentMap> activeSec;

        NHPTimer::STime hpprev = GetCycleCountFast();
        ui64 nextTimestamp = TInstant::Now().MicroSeconds();
        ui64 nextMonotonic = Max(currentMonotonic, GetMonotonicMicroSeconds());

        while (!AtomicLoad(&StopFlag)) {
            {
                const ui64 delta = nextMonotonic - throttledMonotonic;
                const ui64 elapsedDelta = nextMonotonic - currentMonotonic;
                const ui64 threshold = Max(Min(Config.ProgressThreshold, 2 * elapsedDelta), ui64(1));

                throttledMonotonic = (delta > threshold) ? throttledMonotonic + threshold : nextMonotonic;

                if (MonCounters) {
                    *MonCounters->TimeDelayMs = (nextMonotonic - throttledMonotonic) / 1000;
                }
            }
            AtomicStore(CurrentTimestamp, nextTimestamp);
            AtomicStore(CurrentMonotonic, nextMonotonic);
            currentMonotonic = nextMonotonic;

            if (MonCounters) {
                ++*MonCounters->Iterations;
            }

            bool somethingDone = false;

            // first step - send everything triggered on schedule
            ui64 eventsSent = 0;
            ui64 eventsDropped = 0;
            for (;;) {
                while (!!activeSec && !activeSec->empty()) {
                    TMomentMap::iterator it = activeSec->begin();
                    if (it->first <= throttledMonotonic) {
                        if (NSchedulerQueue::TQueueType* q = it->second.Get()) {
                            while (NSchedulerQueue::TEntry* x = q->Reader.Pop()) {
                                somethingDone = true;
                                Y_DEBUG_ABORT_UNLESS(x->InstantMicroseconds <= activeTick);
                                IEventHandle* ev = x->Ev;
                                ISchedulerCookie* cookie = x->Cookie;
                                // TODO: lazy send with backoff queue to not hang over contended mailboxes
                                if (cookie) {
                                    if (cookie->Detach()) {
                                        ActorSystem->Send(ev);
                                        ++eventsSent;
                                    } else {
                                        delete ev;
                                        ++eventsDropped;
                                    }
                                } else {
                                    ActorSystem->Send(ev);
                                    ++eventsSent;
                                }
                            }
                        }
                        activeSec->erase(it);
                    } else
                        break;
                }

                if (activeTick <= throttledMonotonic) {
                    Y_DEBUG_ABORT_UNLESS(!activeSec || activeSec->empty());
                    activeSec.Destroy();
                    activeTick += IntrasecondThreshold;
                    TScheduleMap::iterator it = ScheduleMap.find(activeTick);
                    if (it != ScheduleMap.end()) {
                        activeSec = it->second;
                        ScheduleMap.erase(it);
                    }
                    continue;
                }

                // ok, if we are here - then nothing is ready, so send step complete
                break;
            }

            // second step - collect everything from queues

            ui64 eventsAdded = 0;
            for (ui32 i = 0; i != TotalReaders; ++i) {
                while (NSchedulerQueue::TEntry* x = Readers[i]->Pop()) {
                    somethingDone = true;
                    const ui64 instant = AlignUp<ui64>(x->InstantMicroseconds, Config.ResolutionMicroseconds);
                    IEventHandle* const ev = x->Ev;
                    ISchedulerCookie* const cookie = x->Cookie;

                    // check is cookie still valid? looks like it will hurt performance w/o sagnificant memory save

                    if (instant <= activeTick) {
                        if (!activeSec)
                            activeSec.Reset(new TMomentMap());
                        TAutoPtr<NSchedulerQueue::TQueueType>& queue = (*activeSec)[instant];
                        if (!queue)
                            queue.Reset(new NSchedulerQueue::TQueueType());
                        queue->Writer.Push(instant, ev, cookie);
                    } else {
                        const ui64 intrasecond = AlignUp<ui64>(instant, IntrasecondThreshold);
                        TAutoPtr<TMomentMap>& msec = ScheduleMap[intrasecond];
                        if (!msec)
                            msec.Reset(new TMomentMap());
                        TAutoPtr<NSchedulerQueue::TQueueType>& queue = (*msec)[instant];
                        if (!queue)
                            queue.Reset(new NSchedulerQueue::TQueueType());
                        queue->Writer.Push(instant, ev, cookie);
                    }

                    ++eventsAdded;
                }
            }

            NHPTimer::STime hpnow = GetCycleCountFast();

            if (MonCounters) {
                *MonCounters->QueueSize -= eventsSent + eventsDropped;
                *MonCounters->QueueSize += eventsAdded;
                *MonCounters->EventsSent += eventsSent;
                *MonCounters->EventsDropped += eventsDropped;
                *MonCounters->EventsAdded += eventsAdded;
                *MonCounters->ElapsedMicrosec += NHPTimer::GetSeconds(hpnow - hpprev) * 1000000;
            }

            hpprev = hpnow;
            nextTimestamp = TInstant::Now().MicroSeconds();
            nextMonotonic = Max(currentMonotonic, GetMonotonicMicroSeconds());

            // ok complete, if nothing left - sleep
            if (!somethingDone) {
                const ui64 nextInstant = AlignDown<ui64>(throttledMonotonic + Config.ResolutionMicroseconds, Config.ResolutionMicroseconds);
                if (nextMonotonic >= nextInstant) // already in next time-slice
                    continue;

                const ui64 delta = nextInstant - nextMonotonic;
                if (delta < Config.SpinThreshold) // not so much time left, just spin
                    continue;

                if (MonCounters) {
                    ++*MonCounters->Sleeps;
                }

                NanoSleep(delta * 1000); // ok, looks like we should sleep a bit.

                // Don't count sleep in elapsed microseconds
                hpprev = GetCycleCountFast();
                nextTimestamp = TInstant::Now().MicroSeconds();
                nextMonotonic = Max(currentMonotonic, GetMonotonicMicroSeconds());
            }
        }
        // ok, die!
    }

    void TBasicSchedulerThread::Prepare(TActorSystem* actorSystem, volatile ui64* currentTimestamp, volatile ui64* currentMonotonic) {
        ActorSystem = actorSystem;
        CurrentTimestamp = currentTimestamp;
        CurrentMonotonic = currentMonotonic;
        *CurrentTimestamp = TInstant::Now().MicroSeconds();
        *CurrentMonotonic = GetMonotonicMicroSeconds();
    }

    void TBasicSchedulerThread::PrepareSchedules(NSchedulerQueue::TReader** readers, ui32 scheduleReadersCount) {
        Y_ABORT_UNLESS(scheduleReadersCount > 0);
        TotalReaders = scheduleReadersCount;
        Readers.Reset(new NSchedulerQueue::TReader*[scheduleReadersCount]);
        Copy(readers, readers + scheduleReadersCount, Readers.Get());
    }

    void TBasicSchedulerThread::PrepareStart() {
        // Called after actor system is initialized, but before executor threads
        // are started, giving us a chance to update current timestamp with a
        // more recent value, taking initialization time into account. This is
        // safe to do, since scheduler thread is not started yet, so no other
        // threads are updating time concurrently.
        AtomicStore(CurrentTimestamp, TInstant::Now().MicroSeconds());
        AtomicStore(CurrentMonotonic, Max(RelaxedLoad(CurrentMonotonic), GetMonotonicMicroSeconds()));
    }

    void TBasicSchedulerThread::Start() {
        MainCycle.Reset(new NThreading::TLegacyFuture<void, false>(std::bind(&TBasicSchedulerThread::CycleFunc, this)));
    }

    void TBasicSchedulerThread::PrepareStop() {
        AtomicStore(&StopFlag, true);
    }

    void TBasicSchedulerThread::Stop() {
        MainCycle->Get();
        MainCycle.Destroy();
    }

}

#ifdef __linux__

namespace NActors {
    ISchedulerThread* CreateSchedulerThread(const TSchedulerConfig& config) {
        if (config.UseSchedulerActor) {
            return new TMockSchedulerThread();
        } else {
            return new TBasicSchedulerThread(config);
        }
    }

}

#else //  __linux__

namespace NActors {
    ISchedulerThread* CreateSchedulerThread(const TSchedulerConfig& config) {
        return new TBasicSchedulerThread(config);
    }
}

#endif // __linux__
