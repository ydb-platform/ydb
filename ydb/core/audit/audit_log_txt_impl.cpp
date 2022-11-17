#include "audit_log.h"
#include "audit_log_impl.h"

namespace NKikimr::NAudit {

using namespace NActors;

void TAuditTxtLogActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    AUDIT_LOG_ENABLED.store(false);
    Die(ctx);
}

STFUNC(TAuditTxtLogActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvAuditLog::TEvWriteAuditLog, HandleWriteAuditLog);
    default:
        HandleUnexpectedEvent(ev, ctx);
        break;
    }
}

void TAuditTxtLogActor::HandleWriteAuditLog(const TEvAuditLog::TEvWriteAuditLog::TPtr& ev, const TActorContext& ctx) {
    Y_UNUSED(ctx);
    const auto* msg = ev->Get();
    try {
        TStringStream ss;
        char buf[TimeBufSize];
        ss << FormatLocalTimestamp(msg->Time, buf) << ": ";

        for (auto it = msg->Parts.begin(); it != msg->Parts.end(); it++) {
            if (it != msg->Parts.begin())
                ss << ", ";
            ss << it->first << "=" << it->second;
        }
        ss << Endl;
        auto text = ss.Str();
        
        AuditFile->WriteData(
            TLogRecord(
                ELogPriority::TLOG_INFO,
                text.data(),
                text.length()));
    } catch (const TFileError& e) {
        LOG_W("TAuditTxtLogActor:"
              << " unable to write audit log (error: " << e.what() << ")");
    }
}

void TAuditTxtLogActor::HandleUnexpectedEvent(STFUNC_SIG)
{
    Y_UNUSED(ctx);

    LOG_W("TAuditTxtLogActor:"
          << " unhandled event type: " << ev->GetTypeRewrite()
          << " event: " << (ev->HasEvent() ? ev->GetBase()->ToString().data() : "serialized?"));
}

}    // namespace NKikimr::NAudit
