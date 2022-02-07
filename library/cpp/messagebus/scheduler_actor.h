#pragma once

#include "local_tasks.h"

#include <library/cpp/messagebus/actor/actor.h>
#include <library/cpp/messagebus/actor/what_thread_does_guard.h>
#include <library/cpp/messagebus/scheduler/scheduler.h>

#include <util/system/mutex.h>

namespace NBus {
    namespace NPrivate {
        template <typename TThis, typename TTag = NActor::TDefaultTag>
        class TScheduleActor {
            typedef NActor::TActor<TThis, TTag> TActorForMe;

        private:
            TScheduler* const Scheduler;

            TMutex Mutex;

            TInstant ScheduleTime;

        public:
            TLocalTasks Alarm;

        private:
            struct TScheduleItemImpl: public IScheduleItem {
                TIntrusivePtr<TThis> Thiz;

                TScheduleItemImpl(TIntrusivePtr<TThis> thiz, TInstant when)
                    : IScheduleItem(when)
                    , Thiz(thiz)
                {
                }

                void Do() override {
                    {
                        TWhatThreadDoesAcquireGuard<TMutex> guard(Thiz->Mutex, "scheduler actor: acquiring lock for Do");

                        if (Thiz->ScheduleTime == TInstant::Max()) {
                            // was already fired
                            return;
                        }

                        Thiz->ScheduleTime = TInstant::Max();
                    }

                    Thiz->Alarm.AddTask();
                    Thiz->GetActorForMe()->Schedule();
                }
            };

        public:
            TScheduleActor(TScheduler* scheduler)
                : Scheduler(scheduler)
                , ScheduleTime(TInstant::Max())
            {
            }

            /// call Act(TTag) at specified time, unless it is already scheduled at earlier time.
            void ScheduleAt(TInstant when) {
                TWhatThreadDoesAcquireGuard<TMutex> guard(Mutex, "scheduler: acquiring lock for ScheduleAt");

                if (when > ScheduleTime) {
                    // already scheduled
                    return;
                }

                ScheduleTime = when;
                Scheduler->Schedule(new TScheduleItemImpl(GetThis(), when));
            }

        private:
            TThis* GetThis() {
                return static_cast<TThis*>(this);
            }

            TActorForMe* GetActorForMe() {
                return static_cast<TActorForMe*>(GetThis());
            }
        };

    }
}
