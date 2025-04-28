#pragma once

#include "abstract.h"
#include "config.h"
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/conclusion/result.h>
#include <ydb/core/base/events.h>
#include <ydb/core/kqp/runtime/scheduler/new/fwd.h>

namespace NKikimr::NConveyor {

struct TEvExecution {
    enum EEv {
        EvNewTask = EventSpaceBegin(TKikimrEvents::ES_CONVEYOR),
        EvRegisterProcess,
        EvUnregisterProcess,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_CONVEYOR), "expected EvEnd < EventSpaceEnd");

    class TEvNewTask: public NActors::TEventLocal<TEvNewTask, EvNewTask> {
    private:
        YDB_READONLY_DEF(ITask::TPtr, Task);
        YDB_READONLY(ui64, ProcessId, 0);
        YDB_READONLY(TMonotonic, ConstructInstant, TMonotonic::Now());
        YDB_READONLY_DEF(NKqp::NScheduler::TSchedulableTaskPtr, SchedulableTask);
    public:
        explicit TEvNewTask(ITask::TPtr task, NKqp::NScheduler::TSchedulableTaskPtr schedulableTask = {});
        explicit TEvNewTask(ITask::TPtr task, const ui64 processId, NKqp::NScheduler::TSchedulableTaskPtr schedulableTask);
    };

    class TEvRegisterProcess: public NActors::TEventLocal<TEvRegisterProcess, EvRegisterProcess> {
    private:
        YDB_READONLY(ui64, ProcessId, 0);
        YDB_READONLY_DEF(TCPULimitsConfig, CPULimits);
    public:
        explicit TEvRegisterProcess(const ui64 processId, const TCPULimitsConfig& cpuLimits)
            : ProcessId(processId)
            , CPULimits(cpuLimits) {
        }

    };

    class TEvUnregisterProcess: public NActors::TEventLocal<TEvUnregisterProcess, EvUnregisterProcess> {
    private:
        YDB_READONLY(ui64, ProcessId, 0);
    public:
        explicit TEvUnregisterProcess(const ui64 processId)
            : ProcessId(processId) {
        }

    };
};

}
