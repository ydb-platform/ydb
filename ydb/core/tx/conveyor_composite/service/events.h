#pragma once
#include "counters.h"
#include "scope.h"

#include <ydb/core/tx/conveyor_composite/usage/common.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/monotonic.h>
#include <ydb/library/conclusion/result.h>

namespace NKikimr::NConveyorComposite {

class TWorkerTaskContext {
private:
    YDB_READONLY(TMonotonic, CreateInstant, TMonotonic::Now());
    YDB_READONLY_DEF(TDuration, PredictedDuration);
    YDB_READONLY(ESpecialTaskCategory, Category, ESpecialTaskCategory::Insert);
    YDB_READONLY_DEF(std::shared_ptr<TProcessScope>, Scope);
    YDB_READONLY(ui64, ProcessId, 0);

public:
    TWorkerTaskContext(
        const TDuration prediction, const ESpecialTaskCategory category, const std::shared_ptr<TProcessScope>& scope, const ui64 processId)
        : PredictedDuration(prediction)
        , Category(category)
        , Scope(scope)
        , ProcessId(processId) {
    }
};

class TWorkerTask;

class TWorkerTaskResult: public TWorkerTaskContext {
private:
    using TBase = TWorkerTaskContext;
    YDB_READONLY_DEF(TMonotonic, Start);
    YDB_READONLY_DEF(TMonotonic, Finish);

    TWorkerTaskResult(const TWorkerTaskContext& context, const TMonotonic start, const TMonotonic finish);
    friend class TWorkerTask;

public:
    TDuration GetDuration() const {
        return Finish - Start;
    }
};

class TWorkerTask: public TWorkerTaskContext {
private:
    using TBase = TWorkerTaskContext;
    YDB_READONLY_DEF(ITask::TPtr, Task);
    YDB_READONLY_DEF(std::shared_ptr<TTaskSignals>, TaskSignals);

public:
    TWorkerTaskResult GetResult(const TMonotonic start, const TMonotonic finish) const {
        return TWorkerTaskResult(*this, start, finish);
    }

    TWorkerTask(const ITask::TPtr& task, const TDuration prediction, const ESpecialTaskCategory category,
        const std::shared_ptr<TProcessScope>& scope, const std::shared_ptr<TTaskSignals>& taskSignals, const ui64 processId)
        : TBase(prediction, category, scope, processId)
        , Task(task)
        , TaskSignals(taskSignals) {
        Y_ABORT_UNLESS(task);
    }

    TWorkerTask(TWorkerTaskContext&& context, ITask::TPtr&& task, std::shared_ptr<TTaskSignals>&& taskSignals)
        : TBase(std::move(context))
        , Task(std::move(task))
        , TaskSignals(std::move(taskSignals)) {
        AFL_VERIFY(Task);
        AFL_VERIFY(TaskSignals);
    }
};

class TWorkerTaskPrepare: public TWorkerTaskContext {
private:
    using TBase = TWorkerTaskContext;
    YDB_READONLY_DEF(ITask::TPtr, Task);

public:
    TWorkerTask BuildTask(std::shared_ptr<TTaskSignals>&& signals) && {
        return TWorkerTask(std::move(*this), std::move(Task), std::move(signals));
    }

    TWorkerTaskPrepare(ITask::TPtr&& task, const TDuration prediction, const ESpecialTaskCategory category,
        const std::shared_ptr<TProcessScope>& scope, const ui64 processId)
        : TBase(prediction, category, scope, processId)
        , Task(std::move(task)) {
        AFL_VERIFY(Task);
    }

    bool operator<(const TWorkerTaskPrepare& wTask) const {
        return Task->GetPriority() < wTask.Task->GetPriority();
    }
};

struct TEvInternal {
    enum EEv {
        EvNewTask = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvTaskProcessedResult,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expected EvEnd < EventSpaceEnd");

    class TEvNewTask: public NActors::TEventLocal<TEvNewTask, EvNewTask> {
    private:
        std::vector<TWorkerTask> Tasks;
        YDB_READONLY(TMonotonic, ConstructInstant, TMonotonic::Now());

    public:
        TEvNewTask() = default;

        std::vector<TWorkerTask>&& ExtractTasks() {
            return std::move(Tasks);
        }

        explicit TEvNewTask(std::vector<TWorkerTask>&& tasks)
            : Tasks(std::move(tasks)) {
        }
    };

    class TEvTaskProcessedResult: public NActors::TEventLocal<TEvTaskProcessedResult, EvTaskProcessedResult> {
    private:
        using TBase = TConclusion<ITask::TPtr>;
        YDB_READONLY_DEF(TDuration, ForwardSendDuration);
        std::vector<TWorkerTaskResult> Results;
        YDB_READONLY(TMonotonic, ConstructInstant, TMonotonic::Now());
        YDB_READONLY(ui64, WorkerIdx, 0);
        YDB_READONLY(ui64, WorkersPoolId, 0);

    public:
        const std::vector<TWorkerTaskResult>& GetResults() const {
            return Results;
        }

        std::vector<TWorkerTaskResult>&& DetachResults() {
            return std::move(Results);
        }

        TEvTaskProcessedResult(
            std::vector<TWorkerTaskResult>&& results, const TDuration forwardSendDuration, const ui64 workerIdx, const ui64 workersPoolId);
    };
};

}   // namespace NKikimr::NConveyorComposite
