#include "audit_log.h"

#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/log.h>

#include <util/string/builder.h>

namespace NKikimr::NCms {

void AuditLog(const TString& component, const TString& message, const TActorContext& ctx) {
    LOG_NOTICE_S(ctx, NKikimrServices::CMS, "[AuditLog] [" << component << "] " << message);
}

void AuditLog(const TString& component, const IEventBase* request, const IEventBase* response, const TActorContext& ctx) {
    const TString message = TStringBuilder() << "Reply"
        << ": request# " << request->ToString()
        << ", response# " << response->ToString();

    AuditLog(component, message, ctx);
}

void AuditLog(const TString& component, NMon::TEvHttpInfo::TPtr& request, const TString& response, const TActorContext& ctx) {
    const auto& r = request->Get()->Request;

    const TString requestStr = TStringBuilder() << "{"
        << " method: " << r.GetMethod()
        << " path: " << r.GetPath()
        << " addr: " << r.GetRemoteAddr()
        << " body: " << r.GetPostContent()
        << " }";

    const TString message = TStringBuilder() << "Reply"
        << ": request# " << requestStr.Quote()
        << ", response# " << response.Quote();

    AuditLog(component, message, ctx);
}

} // namespace NKikimr::NCms
