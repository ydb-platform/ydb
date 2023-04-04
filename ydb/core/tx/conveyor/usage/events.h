#pragma once
#include "abstract.h"
#include <library/cpp/actors/core/event_local.h>
#include <library/cpp/actors/core/events.h>
#include <ydb/services/metadata/request/common.h>

namespace NKikimr::NConveyor {

struct TEvExecution {
    enum EEv {
        EvNewTask = EventSpaceBegin(TKikimrEvents::ES_CONVEYOR),
        EvTaskProcessedResult,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_CONVEYOR), "expected EvEnd < EventSpaceEnd");

    class TEvNewTask: public NActors::TEventLocal<TEvNewTask, EvNewTask> {
    private:
        YDB_READONLY_DEF(ITask::TPtr, Task);
    public:
        TEvNewTask() = default;

        explicit TEvNewTask(ITask::TPtr task)
            : Task(task) {
        }
    };

    class TEvTaskProcessedResult:
        public NActors::TEventLocal<TEvTaskProcessedResult, EvTaskProcessedResult>,
        public NMetadata::NRequest::TMaybeResult<ITask::TPtr> {
    private:
        using TBase = NMetadata::NRequest::TMaybeResult<ITask::TPtr>;
    public:
        using TBase::TBase;
    };
};

}
