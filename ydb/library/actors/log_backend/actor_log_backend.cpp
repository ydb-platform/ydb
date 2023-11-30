#include "actor_log_backend.h"

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/log.h>
#include <library/cpp/logger/record.h>

namespace {

NActors::NLog::EPriority GetActorLogPriority(ELogPriority priority) {
    switch (priority) {
    case TLOG_EMERG:
        return NActors::NLog::PRI_EMERG;
    case TLOG_ALERT:
        return NActors::NLog::PRI_ALERT;
    case TLOG_CRIT:
        return NActors::NLog::PRI_CRIT;
    case TLOG_ERR:
        return NActors::NLog::PRI_ERROR;
    case TLOG_WARNING:
        return NActors::NLog::PRI_WARN;
    case TLOG_NOTICE:
        return NActors::NLog::PRI_NOTICE;
    case TLOG_INFO:
        return NActors::NLog::PRI_INFO;
    case TLOG_DEBUG:
        return NActors::NLog::PRI_DEBUG;
    default:
        return NActors::NLog::PRI_TRACE;
    }
}

}

TActorLogBackend::TActorLogBackend(NActors::TActorSystem* actorSystem, int logComponent)
    : ActorSystem(actorSystem)
    , LogComponent(logComponent)
{
}

void TActorLogBackend::WriteData(const TLogRecord& rec) {
    LOG_LOG(*ActorSystem, GetActorLogPriority(rec.Priority), LogComponent, TString(rec.Data, rec.Len));
}
