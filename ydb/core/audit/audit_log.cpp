#include "audit_log.h"
#include "audit_log_impl.h"

#include <library/cpp/logger/record.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>

namespace NKikimr::NAudit {

std::atomic<bool> AUDIT_LOG_ENABLED = false;

THolder<NActors::IActor> CreateAuditWriter(TMap<NKikimrConfig::TAuditConfig::EFormat, TVector<THolder<TLogBackend>>> logBackends)
{
    AUDIT_LOG_ENABLED.store(true);
    return MakeHolder<TAuditLogActor>(std::move(logBackends));
}

void SendAuditLog(const NActors::TActorSystem* sys, TVector<std::pair<TStringBuf, TString>>& parts)
{
    auto request = MakeHolder<TEvAuditLog::TEvWriteAuditLog>(Now(), parts);
    sys->Send(MakeAuditServiceID(), request.Release());
}

}    // namespace NKikimr::NAudit
