#include "audit_log.h"
#include "audit_log_impl.h"

#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>

namespace NKikimr::NAudit {

using namespace NActors;

void TAuditLogActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    AUDIT_LOG_ENABLED.store(false);
    Die(ctx);
}

STFUNC(TAuditLogActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvAuditLog::TEvWriteAuditLog, HandleWriteAuditLog);
    default:
        HandleUnexpectedEvent(ev);
        break;
    }
}

void TAuditLogActor::WriteLog(const TString& log, const TVector<THolder<TLogBackend>>& logBackends) {
    for (auto& logBackend : logBackends) {
        try {
            logBackend->WriteData(
                TLogRecord(
                    ELogPriority::TLOG_INFO,
                    log.data(),
                    log.length()));
        } catch (const yexception& e) {
            LOG_W("WriteLog:"
                << " unable to write audit log (error: " << e.what() << ")");
        }
    }
}

TString TAuditLogActor::GetJsonLog(const TEvAuditLog::TEvWriteAuditLog::TPtr& ev) {
    const auto* msg = ev->Get();
    TStringStream ss;
    ss << msg->Time << ": ";
    NJson::TJsonMap m;
    for (auto& [k, v] : msg->Parts) {
        m[k] = v;
    }
    NJson::WriteJson(&ss, &m, false, false);
    ss << Endl;
    return ss.Str();
}

TString TAuditLogActor::GetTxtLog(const TEvAuditLog::TEvWriteAuditLog::TPtr& ev) {
    const auto* msg = ev->Get();
    TStringStream ss;
    ss << msg->Time << ": ";
    for (auto it = msg->Parts.begin(); it != msg->Parts.end(); it++) {
        if (it != msg->Parts.begin())
            ss << ", ";
        ss << it->first << "=" << it->second;
    }
    ss << Endl;
    return ss.Str();
}

void TAuditLogActor::HandleWriteAuditLog(const TEvAuditLog::TEvWriteAuditLog::TPtr& ev, const TActorContext& ctx) {
    Y_UNUSED(ctx);

    for (auto& logBackends : LogBackends) {
        switch (logBackends.first) {
            case NKikimrConfig::TAuditConfig::JSON:
                WriteLog(GetJsonLog(ev), logBackends.second);
                break;
            case NKikimrConfig::TAuditConfig::TXT:
                WriteLog(GetTxtLog(ev), logBackends.second);
                break;
            default:
                WriteLog(GetJsonLog(ev), logBackends.second);
                break;
        }
    }
}

void TAuditLogActor::HandleUnexpectedEvent(STFUNC_SIG)
{
    LOG_W("TAuditLogActor:"
          << " unhandled event type: " << ev->GetTypeRewrite()
          << " event: " << ev->GetTypeName());
}

}    // namespace NKikimr::NAudit
