#pragma once

#include "executor.h"
#include "tasks.h"
#include "what_thread_does.h"

#include <util/system/yassert.h>

namespace NActor {
    class IActor: protected  IWorkItem {
    public:
        // TODO: make private
        TTasks Tasks;

    public:
        virtual void ScheduleHereV() = 0;
        virtual void ScheduleV() = 0;
        virtual void ScheduleHereAtMostOnceV() = 0;

        // TODO: make private
        virtual void RefV() = 0;
        virtual void UnRefV() = 0;

        // mute warnings
        ~IActor() override {
        }
    };

    struct TDefaultTag {};

    template <typename TThis, typename TTag = TDefaultTag>
    class TActor: public IActor {
    private:
        TExecutor* const Executor;

    public:
        TActor(TExecutor* executor)
            : Executor(executor)
        {
        }

        void AddTaskFromActorLoop() {
            bool schedule = Tasks.AddTask();
            // TODO: check thread id
            Y_ASSERT(!schedule);
        }

        /**
     * Schedule actor.
     *
     * If actor is sleeping, then actor will be executed right now.
     * If actor is executing right now, it will be executed one more time.
     * If this method is called multiple time, actor will be re-executed no more than one more time.
     */
        void Schedule() {
            if (Tasks.AddTask()) {
                EnqueueWork();
            }
        }

        /**
     * Schedule actor, execute it in current thread.
     *
     * If actor is running, continue executing where it is executing.
     * If actor is sleeping, execute it in current thread.
     *
     * Operation is useful for tasks that are likely to complete quickly.
     */
        void ScheduleHere() {
            if (Tasks.AddTask()) {
                Loop();
            }
        }

        /**
     * Schedule actor, execute in current thread no more than once.
     *
     * If actor is running, continue executing where it is executing.
     * If actor is sleeping, execute one iteration here, and if actor got new tasks,
     * reschedule it in worker pool.
     */
        void ScheduleHereAtMostOnce() {
            if (Tasks.AddTask()) {
                bool fetched = Tasks.FetchTask();
                Y_ABORT_UNLESS(fetched, "happens");

                DoAct();

                // if someone added more tasks, schedule them
                if (Tasks.FetchTask()) {
                    bool added = Tasks.AddTask();
                    Y_ABORT_UNLESS(!added, "happens");
                    EnqueueWork();
                }
            }
        }

        void ScheduleHereV() override {
            ScheduleHere();
        }
        void ScheduleV() override {
            Schedule();
        }
        void ScheduleHereAtMostOnceV() override {
            ScheduleHereAtMostOnce();
        }
        void RefV() override {
            GetThis()->Ref();
        }
        void UnRefV() override {
            GetThis()->UnRef();
        }

    private:
        TThis* GetThis() {
            return static_cast<TThis*>(this);
        }

        void EnqueueWork() {
            GetThis()->Ref();
            Executor->EnqueueWork({this});
        }

        void DoAct() {
            WHAT_THREAD_DOES_PUSH_POP_CURRENT_FUNC();

            GetThis()->Act(TTag());
        }

        void Loop() {
            // TODO: limit number of iterations
            while (Tasks.FetchTask()) {
                DoAct();
            }
        }

        void DoWork() override {
            Y_ASSERT(GetThis()->RefCount() >= 1);
            Loop();
            GetThis()->UnRef();
        }
    };

}
