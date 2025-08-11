#include "audit.h"
#include "auditable_actions.cpp"

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
    const TString REASON_EXECUTE = "Execute";

    // audit event has limit of 4 MB, but we limit body size to 2 MB
    const size_t MAX_AUDIT_BODY_SIZE = 2_MB - TRUNCATED_SUFFIX.size();

    inline TUrlMatcher CreateAuditableActionsMatcher() {
        TUrlMatcher policy;
        for (const auto& pattern : AUDITABLE_ACTIONS) {
            policy.AddPattern(pattern);
        }
        return policy;
    }

    NACLibProto::ESubjectType GetSubjectType(const TIntrusiveConstPtr<NACLib::TUserToken>& userToken) {
        return userToken ? userToken->GetSubjectType() : NACLibProto::SUBJECT_TYPE_ANONYMOUS;
    }

    bool HttpAuditEnabled(NACLibProto::ESubjectType subjectType) {
        return NKikimr::AppData()->AuditConfig.EnableLogging(
            NKikimrConfig::TAuditConfig::TLogClassConfig::ClusterAdmin, subjectType);
    }

    HTTP_METHOD ParseMethod(TStringBuf method) {
        if (method == "GET") {
            return HTTP_METHOD_GET;
        }
        if (method == "OPTIONS") {
            return HTTP_METHOD_OPTIONS;
        }
        if (method == "POST") {
            return HTTP_METHOD_POST;
        }
        if (method == "HEAD") {
            return HTTP_METHOD_HEAD;
        }
        if (method == "PUT") {
            return HTTP_METHOD_PUT;
        }
        if (method == "DELETE") {
            return HTTP_METHOD_DELETE;
        }
        return HTTP_METHOD_UNDEFINED;
    }

    TString ToString(HTTP_METHOD method) {
        switch (method) {
            case HTTP_METHOD_GET: return "GET";
            case HTTP_METHOD_OPTIONS: return "OPTIONS";
            case HTTP_METHOD_POST: return "POST";
            case HTTP_METHOD_HEAD: return "HEAD";
            case HTTP_METHOD_PUT: return "PUT";
            case HTTP_METHOD_DELETE: return "DELETE";
            default: return "UNDEFINED";
        }
    }
}

TAuditCtx::ERequestStatus TAuditCtx::GetStatus(const NHttp::THttpOutgoingResponsePtr response) {
    auto status = response.Get()->Status;
    if (status.StartsWith("2")) {
        return TAuditCtx::ERequestStatus::Success;
    } else if (status.StartsWith("3")) {
        return TAuditCtx::ERequestStatus::Process;
    }

    return TAuditCtx::ERequestStatus::Error;
}

TString TAuditCtx::ToString(const TAuditCtx::ERequestStatus value) {
    switch (value) {
        case TAuditCtx::ERequestStatus::Success: return "SUCCESS";
        case TAuditCtx::ERequestStatus::Process: return "IN-PROCESS";
        case TAuditCtx::ERequestStatus::Error: return "ERROR";
    }
    return EMPTY_VALUE;
}

TString TAuditCtx::GetReason(const NHttp::THttpOutgoingResponsePtr& response) {
    TStringBuilder reason;
    reason << response.Get()->Status << " " << response.Get()->Message;
    return reason;
}

void TAuditCtx::AddAuditLogPart(TStringBuf name, const TString& value) {
    Parts.emplace_back(name, value);
}

bool TAuditCtx::AuditableRequest(const HTTP_METHOD method, const TString& url, const TCgiParameters& cgiParams) {
    // only modifying methods are audited
    static const THashSet<HTTP_METHOD> MODIFYING_METHODS = {HTTP_METHOD_POST, HTTP_METHOD_PUT, HTTP_METHOD_DELETE};
    if (MODIFYING_METHODS.contains(method)) {
        return true;
    }

    // OPTIONS are not audited
    if (method == HTTP_METHOD_OPTIONS) {
        return false;
    }

    // auditable actions are defined with URL patterns
    static auto AUDITABLE_ACTIONS_MATCHER = CreateAuditableActionsMatcher();
    if (AUDITABLE_ACTIONS_MATCHER.Match(url, cgiParams)) {
        return true;
    }

    return false;
}

void TAuditCtx::LogAudit(ERequestStatus status, const TString& reason) {
    AUDIT_LOG(
        for (const auto& [name, value] : Parts) {
            AUDIT_PART(name, (!value.empty() ? value : EMPTY_VALUE));
        }

        AUDIT_PART("status", ToString(status));
        if (status != ERequestStatus::Success) {
            AUDIT_PART("reason", (!reason.empty() ? reason : EMPTY_VALUE));
        }
    );
}

void TAuditCtx::Init(const NActors::NMon::TEvHttpInfo::TPtr& ev) {
    if (State != EState::Init) {
        return;
    }
    const auto& request = ev->Get()->Request;
    const auto method = request.GetMethod();
    const TString url(request.GetUri().Before('?'));
    const auto params = request.GetUri().After('?');
    const auto cgiParams = TCgiParameters(params);

    auto remoteAddress = ::ToString(request.GetHeader(X_FORWARDED_FOR_HEADER).Before(',')); // Get the first address in the list
    if (!AuditableRequest(method, url, cgiParams)) {
        State = EState::Completed;
        return;
    }

    AddAuditLogPart("component", MONITORING_COMPONENT_NAME);
    AddAuditLogPart("remote_address", remoteAddress);
    AddAuditLogPart("operation", DEFAULT_OPERATION);
    AddAuditLogPart("method", NMonitoring::NAudit::ToString(method));
    AddAuditLogPart("url", url);
    if (!params.Empty()) {
        AddAuditLogPart("params", ::ToString(params));
    }
    if (!request.GetPostContent().Empty()) {
        TStringBuilder auditBody;
        TStringBuf body = request.GetPostContent();
        if (body.size() > MAX_AUDIT_BODY_SIZE) {
            auditBody << body.SubString(0, MAX_AUDIT_BODY_SIZE) << TRUNCATED_SUFFIX;
        } else {
            auditBody << body;
        }

        AddAuditLogPart("body", auditBody);
    }

    State = EState::Executing;
}

void TAuditCtx::Init(const NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev) {
    if (State != EState::Init) {
        return;
    }
    const auto& request = ev->Get()->Request;
    const auto method = ParseMethod(request->Method);
    const TString url(request->URL.Before('?'));
    const auto params = request->URL.After('?');
    const auto cgiParams = TCgiParameters(params);

    NHttp::THeaders headers(request->Headers);
    auto remoteAddress = ::ToString(headers.Get(X_FORWARDED_FOR_HEADER).Before(',')); // Get the first address in the list
    if (!AuditableRequest(method, url, cgiParams)) {
        State = EState::Completed;
        return;
    }

    AddAuditLogPart("component", MONITORING_COMPONENT_NAME);
    AddAuditLogPart("remote_address", remoteAddress);
    AddAuditLogPart("operation", DEFAULT_OPERATION);
    AddAuditLogPart("method", NMonitoring::NAudit::ToString(method));
    AddAuditLogPart("url", url);
    if (!params.Empty()) {
        AddAuditLogPart("params", ::ToString(params));
    }
    if (!request->Body.Empty()) {
        TStringBuilder auditBody;
        TStringBuf body = request->Body;
        if (body.size() > MAX_AUDIT_BODY_SIZE) {
            auditBody << body.SubString(0, MAX_AUDIT_BODY_SIZE) << TRUNCATED_SUFFIX;
        } else {
            auditBody << body;
        }

        AddAuditLogPart("body", auditBody);
    }

    State = EState::Executing;
}

void TAuditCtx::SetUserToken(const TIntrusiveConstPtr<NACLib::TUserToken>& userToken) {
    AddAuditLogPart("subject", (userToken->GetUserSID() ? userToken->GetUserSID() : EMPTY_VALUE));
    AddAuditLogPart("sanitized_token", (userToken->GetSanitizedToken() ? userToken->GetSanitizedToken() : EMPTY_VALUE));
    SubjectType = userToken->GetSubjectType();
}

void TAuditCtx::SetUserToken(const NACLibProto::TUserToken& userToken) {
    AddAuditLogPart("subject", (userToken.GetUserSID() ? userToken.GetUserSID() : EMPTY_VALUE));
    AddAuditLogPart("sanitized_token", (userToken.GetSanitizedToken() ? userToken.GetSanitizedToken() : EMPTY_VALUE));
    SubjectType = userToken.GetSubjectType();
}

void TAuditCtx::LogOnExecute() {
    if (State != EState::Executing) {
        return;
    }
    if (!HttpAuditEnabled(SubjectType)) {
        return;
    }
    LogAudit(ERequestStatus::Process, REASON_EXECUTE);
}

void TAuditCtx::LogOnResult(const NHttp::THttpOutgoingResponsePtr& response) {
    if (State != EState::Executing) {
        return;
    }

    auto status = GetStatus(response);
    if (status == ERequestStatus::Process) {
        return; // do not log in-process requests
    }

    auto reason = GetReason(response);
    LogAudit(status, reason);
    State = EState::Completed;
}

bool HttpAuditEnabled(const TIntrusiveConstPtr<NACLib::TUserToken>& userToken) {
    auto subjectType = GetSubjectType(userToken);
    return HttpAuditEnabled(subjectType);
}

// bool HttpAuditEnabled(TString serializedToken) {
//     NACLibProto::TUserToken userToken;
//     if (userToken.ParseFromString(serializedToken)) {
//         return HttpAuditEnabled(userToken.GetSubjectType());
//     }
//     return false;
// }

}
