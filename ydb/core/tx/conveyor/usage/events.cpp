#include "events.h"
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NConveyor {

TEvExecution::TEvNewTask::TEvNewTask(ITask::TPtr task)
    : Task(task) {
    AFL_VERIFY(Task);
}

TEvExecution::TEvNewTask::TEvNewTask(ITask::TPtr task, const ui64 processId)
    : Task(task)
    , ProcessId(processId)
{
    AFL_VERIFY(Task);
}

TEvExecution::TEvRegisterActor::TEvRegisterActor(const TActorId& recipient, std::unique_ptr<NActors::IActor>&& actor, const TString& type, const TString& detailedInfo, ui64 cookie)
    : Actor(std::move(actor))
    , Recipient(recipient)
    , Type(type)
    , DetailedInfo(detailedInfo)
    , Cookie(cookie)
{}

TEvExecution::TEvRegisterActorResponse::TEvRegisterActorResponse(const TActorId& actorId, const TString& type, const TString& detailedInfo, ui64 cookie)
    : ActorId(actorId)
    , Type(type)
    , DetailedInfo(detailedInfo)
    , Cookie(cookie)
{}

}