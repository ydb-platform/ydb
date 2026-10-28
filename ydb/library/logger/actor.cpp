#include "actor.h"

namespace NKikimr {

    TDeferredActorLogBackend::TDeferredActorLogBackend(TSharedAtomicActorSystemPtr actorSystem, int logComponent)
        : ActorSystemPtr(std::move(actorSystem))
        , LogComponent(logComponent)
    {
    }

    NActors::NLog::EPriority TDeferredActorLogBackend::GetActorLogPriority(ELogPriority priority) const {
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

    void TDeferredActorLogBackend::WriteData(const TLogRecord& rec) {
        bool written = false;
        ActorSystemPtr->WithActorSystem([&](NActors::TActorSystem* actorSystem) {
            if (Y_LIKELY(actorSystem)) {
                LOG_LOG(*actorSystem, GetActorLogPriority(rec.Priority), LogComponent, TString(rec.Data, rec.Len));
                written = true;
            }
        });
        if (!written) {
            // Not inited, or already detached on shutdown. Temporary write to stderr.
            TStringBuilder out;
            out << TStringBuf(rec.Data, rec.Len) << Endl;
            Cerr << out;
        }
    }

} // NKikimr

