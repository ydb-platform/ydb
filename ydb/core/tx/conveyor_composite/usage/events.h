#pragma once
#include "common.h"
#include "config.h"

#include <ydb/core/base/events.h>

#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/conclusion/result.h>

namespace NKikimr::NConveyorComposite {

struct TEvExecution {
    enum EEv {
        EvNewTask = EventSpaceBegin(TKikimrEvents::ES_CONVEYOR_COMPOSITE),
        EvRegisterProcess,
        EvUnregisterProcess,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_CONVEYOR_COMPOSITE), "expected EvEnd < EventSpaceEnd");

    class TEvNewTask: public NActors::TEventLocal<TEvNewTask, EvNewTask> {
    private:
        YDB_ACCESSOR_DEF(ITask::TPtr, Task);
        YDB_READONLY(ESpecialTaskCategory, Category, ESpecialTaskCategory::Insert);
        YDB_READONLY(ui64, InternalProcessId, 0);
        YDB_READONLY(TMonotonic, ConstructInstant, TMonotonic::Now());

    public:
        TEvNewTask() = default;

        explicit TEvNewTask(ITask::TPtr task, const ESpecialTaskCategory category, const ui64 processId);
    };

    class TEvRegisterProcess: public NActors::TEventLocal<TEvRegisterProcess, EvRegisterProcess> {
    private:
        YDB_READONLY(ESpecialTaskCategory, Category, ESpecialTaskCategory::Insert);
        YDB_READONLY_DEF(TString, ScopeId);
        YDB_READONLY(ui64, InternalProcessId, 0);
        YDB_READONLY_DEF(TCPULimitsConfig, CPULimits);

    public:
        explicit TEvRegisterProcess(
            const TCPULimitsConfig& cpuLimits, const ESpecialTaskCategory category, const TString& scopeId, const ui64 internalProcessId)
            : Category(category)
            , ScopeId(scopeId)
            , InternalProcessId(internalProcessId)
            , CPULimits(cpuLimits) {
        }
    };

    class TEvUnregisterProcess: public NActors::TEventLocal<TEvUnregisterProcess, EvUnregisterProcess> {
    private:
        YDB_READONLY(ESpecialTaskCategory, Category, ESpecialTaskCategory::Insert);
        YDB_READONLY(ui64, InternalProcessId, 0);

    public:
        explicit TEvUnregisterProcess(const ESpecialTaskCategory category, const ui64 internalProcessId)
            : Category(category)
            , InternalProcessId(internalProcessId) {
        }
    };
};

}   // namespace NKikimr::NConveyorComposite
