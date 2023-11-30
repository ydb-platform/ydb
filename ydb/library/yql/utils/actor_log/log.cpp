#include "log.h"

#include <ydb/library/actors/core/log.h>

namespace NYql {
namespace NDq {

using namespace NActors;

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

NYql::NLog::ELevel GetYqlLogLevel(NActors::NLog::EPriority priority) {
    switch (priority) {
        case NActors::NLog::PRI_EMERG:
        case NActors::NLog::PRI_ALERT:
        case NActors::NLog::PRI_CRIT:
            return NYql::NLog::ELevel::FATAL;
        case NActors::NLog::PRI_ERROR:
            return NYql::NLog::ELevel::ERROR;
        case NActors::NLog::PRI_WARN:
            return NYql::NLog::ELevel::WARN;
        case NActors::NLog::PRI_NOTICE:
        case NActors::NLog::PRI_INFO:
            return NYql::NLog::ELevel::INFO;
        case NActors::NLog::PRI_DEBUG:
            return NYql::NLog::ELevel::DEBUG;
        case NActors::NLog::PRI_TRACE:
            return NYql::NLog::ELevel::TRACE;
        default:
            return NYql::NLog::ELevel::TRACE;
    }
}

} // namespace

void TActorYqlLogBackend::WriteData(const TLogRecord& rec) {
    std::visit([&](const auto* actorCtxOrSystem){
        Y_ABORT_UNLESS(actorCtxOrSystem);
        if (TraceId.empty()) {
            LOG_LOG(*actorCtxOrSystem, GetActorLogPriority(rec.Priority), Component,
                "SessionId: %s %.*s", SessionId.c_str(), (int)rec.Len, rec.Data);
        } else {
            LOG_LOG(*actorCtxOrSystem, GetActorLogPriority(rec.Priority), Component,
                "TraceId: %s, SessionId: %s %.*s", TraceId.c_str(), SessionId.c_str(), (int)rec.Len, rec.Data);
        }
    }, ActorCtxOrSystem);
}

void SetYqlLogLevels(const NActors::NLog::EPriority& priority) {
    auto logLevel = GetYqlLogLevel(priority);

    NYql::NLog::EComponentHelpers::ForEach([logLevel] (NYql::NLog::EComponent component) {
        NYql::NLog::YqlLogger().SetComponentLevel(component, logLevel);
    });
}

} // namespace NDq
} // namespace NYql
