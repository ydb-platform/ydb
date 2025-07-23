#include "audit.h"
#include "url_tree.h"

#include <ydb/core/audit/audit_log.h>
#include <ydb/core/base/appdata.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/core/protos/config.pb.h>

#include <util/generic/string.h>

namespace NActors {

namespace {
    const TString MONITORING_COMPONENT_NAME = "monitoring";
    const TString EMPTY_VALUE = "{none}";
    const TString SUCCESS_STATUS = "SUCCESS";
    const TString ERROR_STATUS = "ERROR";
    const TString TRUNCATED = "**TRUNCATED**";
    const TString X_FORWARDED_FOR_HEADER = "X-Forwarded-For";
    const size_t MAX_SIZE = 1000;

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

    TUrlTree CreateAuditUrlPattern() {
        TUrlTree policy;
        for (const auto& pattern : FORCE_AUDIT) {
            policy.AddPattern(pattern);
        }
        return policy;
    }

    bool Success(const NHttp::THttpOutgoingResponsePtr response) {
        auto status = response.Get()->Status;
        if (status.StartsWith("2")) {
            return true;
        }
        return false;
    }

    TString TruncateIfNeeded(TStringBuf part) {
        if (part.empty()) {
            return TString();
        } else if (part.size() <= MAX_SIZE - TRUNCATED.size()) {
            return ToString(part);
        } else if (MAX_SIZE <= TRUNCATED.size()) {
            return TRUNCATED;
        } else {
            TStringBuilder truncated;
            truncated.reserve(MAX_SIZE);
            truncated << TStringBuf(part.data(), MAX_SIZE - TRUNCATED.size()) << TRUNCATED;
            return truncated;
        }
    }
}

bool TAuditCtx::NeedAudit() const {
    return Need;
}

void TAuditCtx::AddAuditLogPart(TStringBuf name, const TString& value) {
    Parts.emplace_back(name, value);
}

bool TAuditCtx::CheckAuditConditions(const TString& method, const TString& url, const TCgiParameters& params) {
    if (!NKikimr::AppData()->AuditConfig.GetMonitoringAudit()) {
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
    static const auto FORCE_AUDIT_URL_PATTERN = CreateAuditUrlPattern();
    if (FORCE_AUDIT_URL_PATTERN.Match(url, params)) {
        return true;
    }

    return false;
}

void TAuditCtx::InitAudit(const NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev) {
    const auto& request = ev->Get()->Request;
    const TString method(request->Method);
    const TString url(request->URL.Before('?'));
    const auto params = request->URL.After('?');
    const auto cgiParams = TCgiParameters(params);
    if (!(Need = CheckAuditConditions(method, url, cgiParams))) {
        return;
    }

    NHttp::THeaders headers(request->Headers);
    auto remote_address = ToString(headers.Get(X_FORWARDED_FOR_HEADER));

    AddAuditLogPart("component", MONITORING_COMPONENT_NAME);
    AddAuditLogPart("remote_address", remote_address);
    AddAuditLogPart("operation", (TStringBuilder() << method << " " << url));
    AddAuditLogPart("params", TruncateIfNeeded(params));
    AddAuditLogPart("body", TruncateIfNeeded(request->Body));
}

void TAuditCtx::AddAuditLogParts(const TIntrusiveConstPtr<NACLib::TUserToken>& userToken) {
    AddAuditLogPart("subject", userToken->GetUserSID());
    AddAuditLogPart("sanitized_token", userToken->GetSanitizedToken());
}

void TAuditCtx::AddAuditLogParts(const NHttp::THttpOutgoingResponsePtr& response) {
    const auto success = Success(response);
    AddAuditLogPart("status", (success ? SUCCESS_STATUS : ERROR_STATUS));
    AddAuditLogPart("reason", TString(response.Get()->Message));
}

void TAuditCtx::FinishAudit() {
    if (!NeedAudit()) {
        return;
    }

    AUDIT_LOG(
        for (const auto& [name, value] : Parts) {
            AUDIT_PART(name, (!value.empty() ? value : EMPTY_VALUE))
        }
    );
}

}
