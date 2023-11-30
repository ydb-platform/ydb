#pragma once

#include <ydb/services/bg_tasks/abstract/common.h>
#include <ydb/services/bg_tasks/abstract/task.h>

#include <ydb/library/actors/core/event_local.h>

namespace NKikimr::NBackgroundTasks {

class TEvAddTask: public NActors::TEventLocal<TEvAddTask, EEvents::EvAddTask> {
private:
    YDB_READONLY_DEF(TTask, Task);
public:
    TEvAddTask(TTask&& task)
        : Task(std::move(task)) {
        Y_ABORT_UNLESS(!!Task.GetActivity());
    }
};

template <class TDerived, const ui32 evType>
class TEvCommonResult: public NActors::TEventLocal<TDerived, evType> {
private:
    YDB_READONLY_DEF(TString, TaskId);
    YDB_READONLY_FLAG(Success, false);
    YDB_ACCESSOR_DEF(TString, Report);
public:
    TEvCommonResult(const TString& taskId, const bool success, const TString& report = Default<TString>())
        : TaskId(taskId)
        , SuccessFlag(success)
        , Report(report)
    {
        Y_ABORT_UNLESS(!!TaskId);
    }

    TString GetDebugString() const {
        TStringBuilder sb;
        sb << "id=" << TaskId << ";";
        sb << "success=" << SuccessFlag << ";";
        if (!SuccessFlag) {
            sb << "report=" << Report << ";";
        }
        return sb;
    }
};

class TEvAddTaskResult: public TEvCommonResult<TEvAddTaskResult, EEvents::EvAddTaskResult> {
private:
    using TBase = TEvCommonResult<TEvAddTaskResult, EEvents::EvAddTaskResult>;
public:
    using TBase::TBase;
};

class TEvUpdateTaskEnabled: public NActors::TEventLocal<TEvUpdateTaskEnabled, EEvents::EvUpdateTaskEnabled> {
private:
    YDB_READONLY_DEF(TString, TaskId);
    YDB_ACCESSOR(bool, Enabled, true);
public:
    TEvUpdateTaskEnabled(const TString& taskId, const bool enabled)
        : TaskId(taskId)
        , Enabled(enabled)
    {
        Y_ABORT_UNLESS(!!TaskId);
    }
};

class TEvUpdateTaskEnabledResult: public TEvCommonResult<TEvUpdateTaskEnabledResult, EEvents::EvUpdateTaskEnabledResult> {
private:
    using TBase = TEvCommonResult<TEvUpdateTaskEnabledResult, EEvents::EvUpdateTaskEnabledResult>;
public:
    using TBase::TBase;
};

class TServiceOperator {
private:
    friend class TExecutor;
    std::atomic<bool> EnabledFlag{ false };
    static void Register();
public:
    static bool IsEnabled();
};

NActors::TActorId MakeServiceId(const ui32 nodeId);

}
