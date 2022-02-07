#pragma once

#include "coro.h"
#include "simple.h"
#include "task.h"

namespace NRainCheck {
    namespace NPrivate {
        void SpawnTaskImpl(TTaskRunnerBase* task);

        template <typename TTask, typename ITask, typename TRunner, typename TEnv, typename TParam>
        TIntrusivePtr<TRunner> SpawnTaskWithRunner(TEnv* env, TParam param1, ISubtaskListener* subtaskListener) {
            static_assert((std::is_base_of<ITask, TTask>::value), "expect (std::is_base_of<ITask, TTask>::value)");
            TIntrusivePtr<TRunner> task(new TRunner(env, subtaskListener, new TTask(env, param1)));
            NPrivate::SpawnTaskImpl(task.Get());
            return task;
        }

        template <typename TTask, typename ITask, typename TRunner, typename TEnv>
        void SpawnSubtaskWithRunner(TEnv* env, TSubtaskCompletion* completion) {
            static_assert((std::is_base_of<ITask, TTask>::value), "expect (std::is_base_of<ITask, TTask>::value)");
            TTaskRunnerBase* current = TTaskRunnerBase::CurrentTask();
            completion->SetRunning(current);
            NPrivate::SpawnTaskImpl(new TRunner(env, completion, new TTask(env)));
        }

        template <typename TTask, typename ITask, typename TRunner, typename TEnv, typename TParam>
        void SpawnSubtaskWithRunner(TEnv* env, TSubtaskCompletion* completion, TParam param) {
            static_assert((std::is_base_of<ITask, TTask>::value), "expect (std::is_base_of<ITask, TTask>::value)");
            TTaskRunnerBase* current = TTaskRunnerBase::CurrentTask();
            completion->SetRunning(current);
            NPrivate::SpawnTaskImpl(new TRunner(env, completion, new TTask(env, param)));
        }

    }

    // Instantiate and start a task with given parameter.
    template <typename TTask, typename TEnv, typename TParam>
    TIntrusivePtr<typename TTask::TTaskRunner> SpawnTask(TEnv* env, TParam param1, ISubtaskListener* subtaskListener = &TNopSubtaskListener::Instance) {
        return NPrivate::SpawnTaskWithRunner<
            TTask, typename TTask::ITask, typename TTask::TTaskRunner, TEnv, TParam>(env, param1, subtaskListener);
    }

    // Instantiate and start subtask of given task.
    template <typename TTask, typename TEnv, typename TParam>
    void SpawnSubtask(TEnv* env, TSubtaskCompletion* completion, TParam param) {
        return NPrivate::SpawnSubtaskWithRunner<TTask, typename TTask::ITask, typename TTask::TTaskRunner>(env, completion, param);
    }

}
