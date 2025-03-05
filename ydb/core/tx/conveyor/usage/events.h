#pragma once
#include "abstract.h"
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/conclusion/result.h>
#include <ydb/core/base/events.h>

namespace NKikimr::NConveyor {

struct TEvExecution {
    enum EEv {
        EvNewTask = EventSpaceBegin(TKikimrEvents::ES_CONVEYOR),
        EvRegisterProcess,
        EvUnregisterProcess,
        EvRegisterActor,
        EvRegisterActorResponse,
        EvActorFinished,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_CONVEYOR), "expected EvEnd < EventSpaceEnd");

    class TEvNewTask: public NActors::TEventLocal<TEvNewTask, EvNewTask> {
    private:
        YDB_READONLY_DEF(ITask::TPtr, Task);
        YDB_READONLY(ui64, ProcessId, 0);
    public:
        TEvNewTask() = default;

        explicit TEvNewTask(ITask::TPtr task);
        explicit TEvNewTask(ITask::TPtr task, const ui64 processId);
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

    class TEvRegisterActor: public NActors::TEventLocal<TEvRegisterActor, EvRegisterActor> {
    private:
        std::unique_ptr<NActors::IActor> Actor;
        YDB_READONLY_DEF(TActorId, Recipient);
        YDB_READONLY_DEF(TString, Type);
        YDB_READONLY_DEF(TString, DetailedInfo);
        YDB_READONLY(ui64, Cookie, 0);
    public:
        TEvRegisterActor() = default;

        std::unique_ptr<NActors::IActor> ExtractActor() {
            return std::move(Actor);
        }

        explicit TEvRegisterActor(const TActorId& recipient, std::unique_ptr<NActors::IActor>&& actor, const TString& type, const TString& detailedInfo, ui64 cookie);
    };

    class TEvRegisterActorResponse: public NActors::TEventLocal<TEvRegisterActorResponse, EvRegisterActorResponse> {
    private:
        YDB_READONLY_DEF(TActorId, ActorId);
        YDB_READONLY_DEF(TString, Type);
        YDB_READONLY_DEF(TString, DetailedInfo);
        YDB_READONLY(ui64, Cookie, 0);
        
    public:
        TEvRegisterActorResponse() = default;

        explicit TEvRegisterActorResponse(const TActorId& actorId, const TString& type, const TString& detailedInfo, ui64 cookie);
    };

    class TEvActorFinished: public NActors::TEventLocal<TEvActorFinished, EvActorFinished> {
    private:
        YDB_READONLY_DEF(TActorId, ActorId);
    public:
        TEvActorFinished() = default;

        explicit TEvActorFinished(TActorId actorId);
    };
};

}
