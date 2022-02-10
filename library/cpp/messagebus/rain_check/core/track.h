#pragma once

#include "spawn.h"
#include "task.h"

#include <library/cpp/messagebus/async_result.h>
#include <library/cpp/messagebus/actor/queue_in_actor.h>
#include <library/cpp/messagebus/misc/atomic_box.h>

#include <util/generic/intrlist.h>
#include <util/system/event.h>

namespace NRainCheck {
    class TTaskTracker;

    namespace NPrivate {
        struct ITaskFactory {
            virtual TIntrusivePtr<TTaskRunnerBase> NewTask(ISubtaskListener*) = 0;
            virtual ~ITaskFactory() {
            }
        };

        struct TTaskTrackerReceipt: public ISubtaskListener, public TIntrusiveListItem<TTaskTrackerReceipt> {
            TTaskTracker* const TaskTracker;
            TIntrusivePtr<TTaskRunnerBase> Task;

            TTaskTrackerReceipt(TTaskTracker* taskTracker)
                : TaskTracker(taskTracker)
            {
            }

            void SetDone() override;

            TString GetStatusSingleLine();
        };

        struct TTaskTrackerStatus {
            ui32 Size;
        };

    }

    class TTaskTracker
       : public TAtomicRefCount<TTaskTracker>,
          public NActor::TActor<TTaskTracker>,
          public NActor::TQueueInActor<TTaskTracker, NPrivate::ITaskFactory*>,
          public NActor::TQueueInActor<TTaskTracker, NPrivate::TTaskTrackerReceipt*>,
          public NActor::TQueueInActor<TTaskTracker, TAsyncResult<NPrivate::TTaskTrackerStatus>*> {
        friend struct NPrivate::TTaskTrackerReceipt;

    private:
        TAtomicBox<bool> ShutdownFlag;
        TSystemEvent ShutdownEvent;

        TIntrusiveList<NPrivate::TTaskTrackerReceipt> Tasks;

        template <typename TItem>
        NActor::TQueueInActor<TTaskTracker, TItem>* GetQueue() {
            return this;
        }

    public:
        TTaskTracker(NActor::TExecutor* executor);
        ~TTaskTracker() override;

        void Shutdown();

        void ProcessItem(NActor::TDefaultTag, NActor::TDefaultTag, NPrivate::ITaskFactory*);
        void ProcessItem(NActor::TDefaultTag, NActor::TDefaultTag, NPrivate::TTaskTrackerReceipt*);
        void ProcessItem(NActor::TDefaultTag, NActor::TDefaultTag, TAsyncResult<NPrivate::TTaskTrackerStatus>*);

        void Act(NActor::TDefaultTag);

        template <typename TTask, typename TEnv, typename TParam>
        void Spawn(TEnv* env, TParam param) {
            struct TTaskFactory: public NPrivate::ITaskFactory {
                TEnv* const Env;
                TParam Param;

                TTaskFactory(TEnv* env, TParam param)
                    : Env(env)
                    , Param(param)
                {
                }

                TIntrusivePtr<TTaskRunnerBase> NewTask(ISubtaskListener* subtaskListener) override {
                    return NRainCheck::SpawnTask<TTask>(Env, Param, subtaskListener).Get();
                }
            };

            GetQueue<NPrivate::ITaskFactory*>()->EnqueueAndSchedule(new TTaskFactory(env, param));
        }

        ui32 Size();
    };

}
