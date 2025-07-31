#include "audit.h"
#include "audit_force.h"

#include <ydb/core/audit/audit_log.h>
#include <ydb/core/audit/audit_config/audit_config.h>
#include <ydb/core/base/appdata.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/core/protos/config.pb.h>

#include <util/generic/string.h>

namespace NMonitoring::NAudit {

namespace {
    const TString MONITORING_COMPONENT_NAME = "monitoring";
    const TString DEFAULT_OPERATION = "HTTP REQUEST";
    const TString EMPTY_VALUE = "{none}";
    const TString X_FORWARDED_FOR_HEADER = "X-Forwarded-For";
    const TStringBuf TRUNCATED_SUFFIX = "**TRUNCATED_BY_YDB**";

    // audit event has limit of 4 MB, but we limit body size to 2 MB
    const size_t MAX_AUDIT_BODY_SIZE = 2 * 1024 * 1024 - TRUNCATED_SUFFIX.size();

    enum ERequestStatus {
        Success,
        Process,
        Error,
    };

    ERequestStatus GetStatus(const NHttp::THttpOutgoingResponsePtr response) {
        auto status = response.Get()->Status;
        if (status.StartsWith("2")) {
            return ERequestStatus::Success;
        } else if (status.StartsWith("3")) {
            return ERequestStatus::Process;
        }

        return ERequestStatus::Error;
    }

    TString ToString(const ERequestStatus value) {
        switch (value) {
            case ERequestStatus::Success: return "SUCCESS";
            case ERequestStatus::Process: return "IN-PROCESS";
            case ERequestStatus::Error: return "ERROR";
        }
        return EMPTY_VALUE;
    }
}

void TAuditCtx::AddAuditLogPart(TStringBuf name, const TString& value) {
    Parts.emplace_back(name, value);
}

bool TAuditCtx::AuditableRequest(const TString& method, const TString& url, const TCgiParameters& cgiParams) {
    // only modifying methods are audited
    static const THashSet<TString> MODIFYING_METHODS = {"POST", "PUT", "DELETE"};
    if (MODIFYING_METHODS.contains(method)) {
        return true;
    }

    // OPTIONS are not audited
    if (method == "OPTIONS") {
        return false;
    }

    // force audit for specific URLs
    static auto FORCE_AUDIT_MATCHER = CreateAuditableActionsMatcher();
    if (FORCE_AUDIT_MATCHER.Match(url, cgiParams)) {
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
    if (!(AuditEnabled = AuditableRequest(method, url, cgiParams))) {
        return;
    }

    NHttp::THeaders headers(request->Headers);
    auto remoteAddress = ToString(headers.Get(X_FORWARDED_FOR_HEADER).Before(',')); // Get the first address in the list

    AddAuditLogPart("component", MONITORING_COMPONENT_NAME);
    AddAuditLogPart("remote_address", remoteAddress);
    AddAuditLogPart("operation", DEFAULT_OPERATION);
    AddAuditLogPart("method", method);
    AddAuditLogPart("url", url);
    if (!params.Empty()) {
        AddAuditLogPart("params", ToString(params));
    }
    if (!request->Body.Empty()) {
        TStringBuilder auditBody;
        if (request->Body.size() > MAX_AUDIT_BODY_SIZE) {
            auditBody << request->Body.substr(0, MAX_AUDIT_BODY_SIZE) << TRUNCATED_SUFFIX;
        } else {
            auditBody << request->Body;
        }

        AddAuditLogPart("body", auditBody);
    }
}

void TAuditCtx::AddAuditLogParts(const TIntrusiveConstPtr<NACLib::TUserToken>& userToken) {
    if (!AuditEnabled) {
        return;
    }
    SubjectType = userToken ? userToken->GetSubjectType() : NACLibProto::SUBJECT_TYPE_ANONYMOUS;
    AddAuditLogPart("subject", userToken->GetUserSID());
    AddAuditLogPart("sanitized_token", userToken->GetSanitizedToken());
}

void TAuditCtx::FinishAudit(const NHttp::THttpOutgoingResponsePtr& response) {
    AuditEnabled &= NKikimr::AppData()->AuditConfig.EnableLogging(NKikimrConfig::TAuditConfig::TLogClassConfig::ClusterAdmin, SubjectType);

    if (!AuditEnabled) {
        return;
    }

    auto status = GetStatus(response);

    AUDIT_LOG(
        for (const auto& [name, value] : Parts) {
            AUDIT_PART(name, (!value.empty() ? value : EMPTY_VALUE))
        }
        AUDIT_PART("status", ToString(status));
        if (status != ERequestStatus::Success) {
            AUDIT_PART("reason", response.Get()->Message);
        }
    );
}

}
