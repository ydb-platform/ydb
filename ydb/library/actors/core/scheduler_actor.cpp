#include "actor_bootstrapped.h"
#include "hfunc.h"
#include "probes.h"
#include "scheduler_actor.h"
#include "scheduler_queue.h"

#include <ydb/library/actors/interconnect/poller_actor.h>
#include <util/system/hp_timer.h>

#ifdef __linux__
#include <sys/timerfd.h>
#include <errno.h>

LWTRACE_USING(ACTORLIB_PROVIDER);

namespace NActors {
    class TTimerDescriptor: public TSharedDescriptor {
        const int Descriptor;

    public:
        TTimerDescriptor()
            : Descriptor(timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK))
        {
            Y_ABORT_UNLESS(Descriptor != -1, "timerfd_create() failed with %s", strerror(errno));
        }

        ~TTimerDescriptor() override {
            close(Descriptor);
        }

        int GetDescriptor() override {
            return Descriptor;
        }
    };

    class TSchedulerActor: public TActor<TSchedulerActor> {
        const TSchedulerConfig Cfg;
        TIntrusivePtr<TSharedDescriptor> TimerDescriptor;

        TVector<NSchedulerQueue::TReader*> Readers;

        TActorId PollerActor;
        TPollerToken::TPtr PollerToken;

        ui64 RealTime;
        ui64 MonotonicTime;

        ui64 ActiveTick;
        typedef TMap<ui64, TAutoPtr<NSchedulerQueue::TQueueType>> TMomentMap; // intrasecond queues
        typedef THashMap<ui64, TAutoPtr<TMomentMap>> TScheduleMap;            // over-second schedule

        TScheduleMap ScheduleMap;

        THolder<NThreading::TLegacyFuture<void, false>> MainCycle;

        static const ui64 IntrasecondThreshold = 1048576; // ~second
        TAutoPtr<TMomentMap> ActiveSec;
        volatile ui64* CurrentTimestamp = nullptr;
        volatile ui64* CurrentMonotonic = nullptr;
        TDeque<TAutoPtr<IEventHandle>> EventsToBeSent;

    public:
        static constexpr IActor::EActivityType ActorActivityType() {
            return IActor::EActivityType::ACTOR_SYSTEM_SCHEDULER_ACTOR;
        }

        TSchedulerActor(const TSchedulerConfig& cfg)
            : TActor(&TSchedulerActor::StateFunc)
            , Cfg(cfg)
            , TimerDescriptor(new TTimerDescriptor())
            , PollerActor(MakePollerActorId())
        {
            Y_ASSERT(Cfg.ResolutionMicroseconds != 0);
            Y_ASSERT(Cfg.ProgressThreshold != 0);
            Become(&TSchedulerActor::StateFunc);
        }

        void Handle(TEvSchedulerInitialize::TPtr& ev, const TActorContext& ctx) {
            const TEvSchedulerInitialize& evInitialize = *ev->Get();
            Y_ASSERT(evInitialize.ScheduleReaders.size() != 0);
            Readers.resize(evInitialize.ScheduleReaders.size());
            Copy(evInitialize.ScheduleReaders.begin(), evInitialize.ScheduleReaders.end(), Readers.begin());

            Y_ASSERT(evInitialize.CurrentTimestamp != nullptr);
            CurrentTimestamp = evInitialize.CurrentTimestamp;

            Y_ASSERT(evInitialize.CurrentMonotonic != nullptr);
            CurrentMonotonic = evInitialize.CurrentMonotonic;

            struct itimerspec new_time;
            memset(&new_time, 0, sizeof(new_time));
            new_time.it_value.tv_nsec = Cfg.ResolutionMicroseconds * 1000;
            new_time.it_interval.tv_nsec = Cfg.ResolutionMicroseconds * 1000;
            int ret = timerfd_settime(TimerDescriptor->GetDescriptor(), 0, &new_time, NULL);
            Y_ABORT_UNLESS(ret != -1, "timerfd_settime() failed with %s", strerror(errno));
            const bool success = ctx.Send(PollerActor, new TEvPollerRegister(TimerDescriptor, SelfId(), {}));
            Y_ABORT_UNLESS(success);

            RealTime = RelaxedLoad(CurrentTimestamp);
            MonotonicTime = RelaxedLoad(CurrentMonotonic);

            ActiveTick = AlignUp<ui64>(MonotonicTime, IntrasecondThreshold);
        }

        void Handle(TEvPollerRegisterResult::TPtr ev, const TActorContext& ctx) {
            PollerToken = ev->Get()->PollerToken;
            HandleSchedule(ctx);
        }

        void UpdateTime() {
            RealTime = TInstant::Now().MicroSeconds();
            MonotonicTime = Max(MonotonicTime, GetMonotonicMicroSeconds());
            AtomicStore(CurrentTimestamp, RealTime);
            AtomicStore(CurrentMonotonic, MonotonicTime);
        }

        void TryUpdateTime(NHPTimer::STime* lastTimeUpdate) {
            NHPTimer::STime hpnow;
            GetTimeFast(&hpnow);
            const ui64 elapsedCycles = hpnow > *lastTimeUpdate ? hpnow - *lastTimeUpdate : 0;
            if (elapsedCycles > Cfg.ResolutionMicroseconds * (NHPTimer::GetCyclesPerSecond() / IntrasecondThreshold)) {
                UpdateTime();
                GetTimeFast(lastTimeUpdate);
            }
        }

        void HandleSchedule(const TActorContext& ctx) {
            for (;;) {
                NHPTimer::STime schedulingStart;
                GetTimeFast(&schedulingStart);
                NHPTimer::STime lastTimeUpdate = schedulingStart;

                ui64 expired;
                ssize_t bytesRead;
                bytesRead = read(TimerDescriptor->GetDescriptor(), &expired, sizeof(expired));
                if (bytesRead == -1) {
                    if (errno == EAGAIN) {
                        PollerToken->Request(true, false);
                        break;
                    } else if (errno == EINTR) {
                        continue;
                    }
                }
                Y_ABORT_UNLESS(bytesRead == sizeof(expired), "Error while reading from timerfd, strerror# %s", strerror(errno));
                UpdateTime();

                ui32 eventsGottenFromQueues = 0;
                // collect everything from queues
                for (ui32 i = 0; i != Readers.size(); ++i) {
                    while (NSchedulerQueue::TEntry* x = Readers[i]->Pop()) {
                        const ui64 instant = AlignUp<ui64>(x->InstantMicroseconds, Cfg.ResolutionMicroseconds);
                        IEventHandle* const ev = x->Ev;
                        ISchedulerCookie* const cookie = x->Cookie;

                        // check is cookie still valid? looks like it will hurt performance w/o sagnificant memory save

                        if (instant <= ActiveTick) {
                            if (!ActiveSec)
                                ActiveSec.Reset(new TMomentMap());
                            TAutoPtr<NSchedulerQueue::TQueueType>& queue = (*ActiveSec)[instant];
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
                        ++eventsGottenFromQueues;
                        TryUpdateTime(&lastTimeUpdate);
                    }
                }

                ui64 eventSchedulingErrorUs = 0;
                // send everything triggered on schedule
                for (;;) {
                    while (!!ActiveSec && !ActiveSec->empty()) {
                        TMomentMap::iterator it = ActiveSec->begin();
                        if (it->first <= MonotonicTime) {
                            if (NSchedulerQueue::TQueueType* q = it->second.Get()) {
                                while (NSchedulerQueue::TEntry* x = q->Reader.Pop()) {
                                    Y_DEBUG_ABORT_UNLESS(x->InstantMicroseconds <= ActiveTick);
                                    if (eventSchedulingErrorUs == 0 && MonotonicTime > x->InstantMicroseconds) {
                                        eventSchedulingErrorUs = MonotonicTime - x->InstantMicroseconds;
                                    }
                                    IEventHandle* ev = x->Ev;
                                    ISchedulerCookie* cookie = x->Cookie;
                                    if (cookie) {
                                        if (cookie->Detach()) {
                                            EventsToBeSent.push_back(ev);
                                        } else {
                                            delete ev;
                                        }
                                    } else {
                                        EventsToBeSent.push_back(ev);
                                    }
                                    TryUpdateTime(&lastTimeUpdate);
                                }
                            }
                            ActiveSec->erase(it);
                        } else {
                            break;
                        }
                    }

                    if (ActiveTick <= MonotonicTime) {
                        Y_DEBUG_ABORT_UNLESS(!ActiveSec || ActiveSec->empty());
                        ActiveSec.Destroy();
                        ActiveTick += IntrasecondThreshold;
                        TScheduleMap::iterator it = ScheduleMap.find(ActiveTick);
                        if (it != ScheduleMap.end()) {
                            ActiveSec = it->second;
                            ScheduleMap.erase(it);
                        }
                        continue;
                    }

                    // ok, if we are here - then nothing is ready, so send step complete
                    break;
                }

                // Send all from buffer queue
                const ui64 eventsToBeSentSize = EventsToBeSent.size();
                ui32 sentCount = 0;
                if (eventsToBeSentSize > Cfg.RelaxedSendThresholdEventsPerCycle) {
                    sentCount = Cfg.RelaxedSendPaceEventsPerCycle +
                        (eventsToBeSentSize - Cfg.RelaxedSendThresholdEventsPerCycle) / 2;
                } else {
                    sentCount = Min(eventsToBeSentSize, Cfg.RelaxedSendPaceEventsPerCycle);
                }
                for (ui32 i = 0; i < sentCount; ++i) {
                    ctx.Send(EventsToBeSent.front().Release());
                    EventsToBeSent.pop_front();
                }

                NHPTimer::STime hpnow;
                GetTimeFast(&hpnow);
                const ui64 processingTime = hpnow > schedulingStart ? hpnow - schedulingStart : 0;
                const ui64 elapsedTimeMicroseconds = processingTime / (NHPTimer::GetCyclesPerSecond() / IntrasecondThreshold);
                LWPROBE(ActorsystemScheduler, elapsedTimeMicroseconds, expired, eventsGottenFromQueues, sentCount,
                        eventsToBeSentSize, eventSchedulingErrorUs);
                TryUpdateTime(&lastTimeUpdate);
            }
        }

        STRICT_STFUNC(StateFunc,
            HFunc(TEvSchedulerInitialize, Handle)
            CFunc(TEvPollerReady::EventType, HandleSchedule)
            CFunc(TEvents::TSystem::PoisonPill, Die)
            HFunc(TEvPollerRegisterResult, Handle)
        )
    };

    IActor* CreateSchedulerActor(const TSchedulerConfig& cfg) {
        if (cfg.UseSchedulerActor) {
            return new TSchedulerActor(cfg);
        } else {
            return nullptr;
        }
    }

}

#else // linux

namespace NActors {
    IActor* CreateSchedulerActor(const TSchedulerConfig& cfg) {
        Y_UNUSED(cfg);
        return nullptr;
    }

}

#endif // linux
