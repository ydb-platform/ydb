#include "audit_log.h"
#include "audit_log_impl.h"

#include <library/cpp/logger/record.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>

#include <util/string/builder.h>

namespace NKikimr::NAudit {

std::atomic<bool> AUDIT_LOG_ENABLED = false;

THolder<NActors::IActor> CreateAuditWriter(THolder<TLogBackend> auditFile, NKikimrConfig::TAuditConfig_EFormat format)
{
    AUDIT_LOG_ENABLED.store(true);
    switch (format) {
        case NKikimrConfig::TAuditConfig::JSON:
            return MakeHolder<TAuditJsonLogActor>(std::move(auditFile));
        case NKikimrConfig::TAuditConfig::TXT:
            return MakeHolder<TAuditTxtLogActor>(std::move(auditFile));
        default:
            return MakeHolder<TAuditJsonLogActor>(std::move(auditFile));
    }
}

void SendAuditLog(const NActors::TActorSystem* sys, TVector<std::pair<TStringBuf, TString>>& parts)
{
    auto request = MakeHolder<TEvAuditLog::TEvWriteAuditLog>(Now(), parts);
    sys->Send(MakeAuditServiceID(), request.Release());
}

}    // namespace NKikimr::NAudit
