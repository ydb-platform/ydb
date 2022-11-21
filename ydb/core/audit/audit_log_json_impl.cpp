#include "audit_log.h"
#include "audit_log_impl.h"

#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>

namespace NKikimr::NAudit {

using namespace NActors;

void TAuditJsonLogActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    AUDIT_LOG_ENABLED.store(false);
    Die(ctx);
}

STFUNC(TAuditJsonLogActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvAuditLog::TEvWriteAuditLog, HandleWriteAuditLog);
    default:
        HandleUnexpectedEvent(ev, ctx);
        break;
    }
}

void TAuditJsonLogActor::HandleWriteAuditLog(const TEvAuditLog::TEvWriteAuditLog::TPtr& ev, const TActorContext& ctx) {
    Y_UNUSED(ctx);
    const auto* msg = ev->Get();
    try {
        TStringStream ss;
        ss << msg->Time << ": ";
        NJson::TJsonMap m;
        for (auto& [k, v] : msg->Parts) {
            m[k] = v;
        }
        NJson::WriteJson(&ss, &m, false, false);
        ss << Endl;
        auto json = ss.Str();

        AuditFile->WriteData(
            TLogRecord(
                ELogPriority::TLOG_INFO,
                json.data(),
                json.length()));
    } catch (const TFileError& e) {
        LOG_W("TAuditJsonLogActor:"
              << " unable to write audit log (error: " << e.what() << ")");
    }
}

void TAuditJsonLogActor::HandleUnexpectedEvent(STFUNC_SIG)
{
    Y_UNUSED(ctx);

    LOG_W("TAuditJsonLogActor:"
          << " unhandled event type: " << ev->GetTypeRewrite()
          << " event: " << (ev->HasEvent() ? ev->GetBase()->ToString().data() : "serialized?"));
}

}    // namespace NKikimr::NAudit
