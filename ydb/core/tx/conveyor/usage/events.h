#pragma once
#include "abstract.h"
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/conclusion/result.h>
#include <ydb/core/base/events.h>
#include <ydb/core/kqp/runtime/kqp_compute_scheduler_handle.h>

namespace NKikimr::NConveyor {

struct TEvExecution {
    enum EEv {
        EvNewTask = EventSpaceBegin(TKikimrEvents::ES_CONVEYOR),
        EvRegisterProcess,
        EvUnregisterProcess,
        EvGetResourcePoolHandle,
        EvGetResourcePoolHandleResponse,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_CONVEYOR), "expected EvEnd < EventSpaceEnd");

    class TEvNewTask: public NActors::TEventLocal<TEvNewTask, EvNewTask> {
    private:
        YDB_READONLY_DEF(ITask::TPtr, Task);
        YDB_READONLY(ui64, ProcessId, 0);
        YDB_READONLY(std::optional<TString>, ResourcePoolKey, std::nullopt);
        YDB_READONLY(TMonotonic, ConstructInstant, TMonotonic::Now());
    public:
        TEvNewTask() = default;

        explicit TEvNewTask(ITask::TPtr task, const std::optional<TString>& resourcePoolKey);
        explicit TEvNewTask(ITask::TPtr task, const ui64 processId, const std::optional<TString>& resourcePoolKey);
    };

    class TEvRegisterProcess: public NActors::TEventLocal<TEvRegisterProcess, EvRegisterProcess> {
    private:
        YDB_READONLY(ui64, ProcessId, 0);
    public:
        explicit TEvRegisterProcess(const ui64 processId)
            : ProcessId(processId) {
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

    class TEvGetResourcePoolHandle: public NActors::TEventLocal<TEvGetResourcePoolHandle, EvGetResourcePoolHandle> {
    private:
        YDB_READONLY(TString, ResourcePoolKey, "");
    public:
        explicit TEvGetResourcePoolHandle(const TString& resourcePoolKey)
            : ResourcePoolKey(resourcePoolKey) {
        }
    };

    class TEvGetResourcePoolHandleResponse: public NActors::TEventLocal<TEvGetResourcePoolHandleResponse, EvGetResourcePoolHandleResponse> {
    private:
        YDB_READONLY(TString, ResourcePoolKey, "");
        YDB_READONLY(std::optional<TString>, Error, std::nullopt);
        std::unique_ptr<NKqp::TSchedulerEntityHandle> Handle;
    public:
        TEvGetResourcePoolHandleResponse(const TString& resourcePoolKey, std::unique_ptr<NKqp::TSchedulerEntityHandle> handle)
            : ResourcePoolKey(resourcePoolKey)
            , Handle(std::move(handle)) {
        }

        explicit TEvGetResourcePoolHandleResponse(const TString& error)
            : Error(error) {
        }

        std::unique_ptr<NKqp::TSchedulerEntityHandle> ExtractHandle() {
            return std::move(Handle);
        }
    };
};

}
