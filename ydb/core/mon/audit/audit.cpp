#include "audit.h"
#include "url_tree.h"

#include <ydb/core/audit/audit_log.h>
#include <ydb/core/base/appdata.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/core/protos/config.pb.h>

#include <util/generic/string.h>

namespace NActors {

namespace {
    const TString MonitoringComponentName = "monitoring";
    const TString EmptyValue = "{none}";

    // Audit logging is based on HTTP methods like POST or PUT
    // but some handlers use GET for changes
    // FORCE_AUDIT lists URLs that always require audit logging
    // despite the HTTP methods associated with getting information
    static const TVector<TUrlPattern> FORCE_AUDIT = {
        {"/actors/failure_injection", "queue"},
        {"/actors/failure_injection", "probe"},
        {"/actors/failure_injection", "enable"},
        {"/actors/failure_injection", "terminate"},
        {"/actors/kqp_proxy", "force_shutdown"},
        {"/actors/logger", "p"},
        {"/actors/logger", "sp"},
        {"/actors/logger", "sr"},
        {"/actors/pdisks", "chunkLockByCount"},
        {"/actors/pdisks", "chunkLockByColor"},
        {"/actors/pdisks", "chunkUnlock"},
        {"/actors/pdisks", "restartPDisk"},
        {"/actors/pdisks", "stopPDisk"},
        {"/actors/pql2", "submit"},
        {"/actors/profiler", "action", "start"},
        {"/actors/profiler", "action", "stop-display"},
        {"/actors/profiler", "action", "stop-save"},
        {"/actors/profiler", "action", "stop-log"},
        {"/actors/vdisks", "type", "restart"},
        {"/actors/sqsgc/rescan"},
        {"/actors/sqsgc/clean"},
        {"/actors/sqsgc/clear_history"},
        {"/cms/api/console/removevolatileyamlconfig"},
        {"/cms/api/console/configureyamlconfig"},
        {"/cms/api/console/configurevolatileyamlconfig"},
        {"/cms/api/console/configure"},
        {"/cms/api/json/toggleconfigvalidator"},
        {"/memory/heap", "action", "log"},
        {"/nodetabmon", "action", "kill_tablet"},
        {"/trace"},
        {"/fq_diag/quotas", "submit"},
        {"/login"},
        {"/logout"},
        {"/tablets", "KillTabletID"},
        {"/tablets", "RestartTabletID"},
        {"/tablets/app"},
    };

    TUrlTree CreteAuditUrlPattern() {
        TUrlTree policy;
        for (const auto& pattern : FORCE_AUDIT) {
            policy.AddPattern(pattern);
        }
        return policy;
    }
}

bool NeedAudit(const TString& method, const TString& url, const TCgiParameters& params,
               TStringBuf responseStatus) {
    if (NKikimr::AppData()->AuditConfig.GetMonitoringAudit()) {
        return false;
    }

    // 3xx responses are not audited
    if (responseStatus.StartsWith("3")) {
        return false;
    }

    // OPTIONS are not audited
    if (method == "OPTIONS") {
        return false;
    }

    // only modifying methods are audited
    static const THashSet<TString> MODIFYING_METHODS = {"POST", "PUT", "DELETE"};
    if (MODIFYING_METHODS.contains(method)) {
        return true;
    }

    // force audit for specific URLs
    static const auto FORCE_AUDIT_URL_PATTERN = CreteAuditUrlPattern();
    if (FORCE_AUDIT_URL_PATTERN.Match(url, params)) {
        return true;
    }

    return false;
}

void AddAuditParts(const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, TAuditParts& parts) {
    parts.emplace_back("subject", userToken->GetUserSID());
    parts.emplace_back("sanitized_token", userToken->GetSanitizedToken());
}

void AddErrorStatusAuditParts(const TString& status, TAuditParts& parts) {
    parts.emplace_back("status", "ERROR");
    parts.emplace_back("reason", status);
}

TString GetStatus(const NHttp::THttpOutgoingResponsePtr response) {
    auto status = response.Get()->Status;
    if (status.StartsWith("2")) {
        return "SUCCESS";
    }
    return "ERROR";
}

void AuditRequest(const NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev,
                  const NHttp::THttpOutgoingResponsePtr response,
                  const TAuditParts& parts) {
    const auto&  request = ev->Get()->Request;
    const TString method(request->Method);
    const TString url(request->URL.Before('?'));
    const auto params = TCgiParameters(request->URL.After('?'));
    const auto status = GetStatus(response);

    if (!NeedAudit(method, url, params, response.Get()->Status)) {
        return;
    }

    const TString remote_address = request->Address ? request->Address->ToString() : TString();

    static const THashSet<TString> WHITE_LIST = {
        "killtabletid",
        "restarttabletid",
        "followerid",
        "ssid",
        "tabletid",
        "proxy",
        "session",
        "semaphore",
        "quoter_resource",
        "section",
        "page",
        "action",
        "kv",
        "txid",
        "sendreadset",
        "compaction",
        "extra_param"
    };
    TStringBuilder filteredParams;
    for (const auto& [key, value] : params) {
        if (WHITE_LIST.contains(to_lower(TString(key)))) {
            if (!filteredParams.empty()) {
                filteredParams << "&";
            }
            filteredParams << key << "=" << value;
        }
    }

    AUDIT_LOG(
        AUDIT_PART("component", MonitoringComponentName)
        AUDIT_PART("remote_address", (!remote_address.empty() ? remote_address : EmptyValue))
        for (const auto& [name, value] : parts) {
            AUDIT_PART(name, (!value.empty() ? value : EmptyValue))
        }
        AUDIT_PART("method", method)
        AUDIT_PART("url", url);
        AUDIT_PART("params", (!filteredParams.empty() ? filteredParams : EmptyValue))
        AUDIT_PART("status", status)
        AUDIT_PART("reason", response.Get()->Message)
    );
}

}
