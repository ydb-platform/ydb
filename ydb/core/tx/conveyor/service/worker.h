#pragma once
#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/core/event_local.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/conveyor/usage/abstract.h>
#include <ydb/core/protos/services.pb.h>
#include <ydb/services/metadata/request/common.h>
#include <library/cpp/actors/core/log.h>
#include <library/cpp/actors/core/hfunc.h>

namespace NKikimr::NConveyor {

class TWorkerTask {
private:
    YDB_READONLY_DEF(ITask::TPtr, Task);
    YDB_READONLY_DEF(NActors::TActorId, OwnerId);
public:
    TWorkerTask(ITask::TPtr task, const NActors::TActorId& ownerId)
        : Task(task)
        , OwnerId(ownerId) {

    }
};

struct TEvInternal {
    enum EEv {
        EvNewTask = EventSpaceBegin(TEvents::ES_PRIVATE),
        EvTaskProcessedResult,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expected EvEnd < EventSpaceEnd");

    class TEvNewTask: public NActors::TEventLocal<TEvNewTask, EvNewTask> {
    private:
        TWorkerTask Task;
    public:
        TEvNewTask() = default;

        const TWorkerTask& GetTask() const {
            return Task;
        }

        explicit TEvNewTask(const TWorkerTask& task)
            : Task(task) {
        }
    };

    class TEvTaskProcessedResult:
        public NActors::TEventLocal<TEvTaskProcessedResult, EvTaskProcessedResult>,
        public NMetadata::NRequest::TMaybeResult<ITask::TPtr> {
    private:
        using TBase = NMetadata::NRequest::TMaybeResult<ITask::TPtr>;
        YDB_READONLY_DEF(NActors::TActorId, OwnerId);
    public:
        TEvTaskProcessedResult(const NActors::TActorId& ownerId, const TString& errorMessage)
            : TBase(errorMessage)
            , OwnerId(ownerId) {

        }
        TEvTaskProcessedResult(const NActors::TActorId& ownerId, ITask::TPtr result)
            : TBase(result)
            , OwnerId(ownerId) {

        }
    };
};

class TWorker: public NActors::TActorBootstrapped<TWorker> {
private:
    using TBase = NActors::TActorBootstrapped<TWorker>;
public:
    void HandleMain(TEvInternal::TEvNewTask::TPtr& ev);

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvInternal::TEvNewTask, HandleMain);
            default:
                ALS_ERROR(NKikimrServices::TX_CONVEYOR) << "unexpected event for task executor: " << ev->GetTypeRewrite();
                break;
        }
    }

    void Bootstrap() {
        Become(&TWorker::StateMain);
    }
};

}
