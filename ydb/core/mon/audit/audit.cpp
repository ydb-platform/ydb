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
    if (State != EState::Init) {
        return;
    }
    const auto& request = ev->Get()->Request;
    const TString method(request->Method);
    const TString url(request->URL.Before('?'));
    const auto params = request->URL.After('?');
    const auto cgiParams = TCgiParameters(params);
    Auditable = AuditableRequest(method, url, cgiParams);

    NHttp::THeaders headers(request->Headers);
    auto remoteAddress = ::ToString(headers.Get(X_FORWARDED_FOR_HEADER).Before(',')); // Get the first address in the list

    AddAuditLogPart("component", MONITORING_COMPONENT_NAME);
    AddAuditLogPart("remote_address", remoteAddress);
    AddAuditLogPart("operation", DEFAULT_OPERATION);
    AddAuditLogPart("method", method);
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
    State = EState::PendingExecution;
}

void TAuditCtx::AddAuditLogParts(const TIntrusiveConstPtr<NACLib::TUserToken>& userToken) {
    SubjectType = userToken ? userToken->GetSubjectType() : NACLibProto::SUBJECT_TYPE_ANONYMOUS;
    if (userToken) {
        Subject = userToken->GetUserSID();
        SanitizedToken = userToken->GetSanitizedToken();
    }
}

void TAuditCtx::LogAudit(ERequestStatus status, const TString& reason) {
    AUDIT_LOG(
        AddAuditLogPart("subject", (Subject ? Subject : EMPTY_VALUE));
        AddAuditLogPart("sanitized_token", (SanitizedToken ? SanitizedToken : EMPTY_VALUE));

        for (const auto& [name, value] : Parts) {
            AUDIT_PART(name, (!value.empty() ? value : EMPTY_VALUE));
        }

        AUDIT_PART("status", ToString(status));
        if (status != ERequestStatus::Success) {
            AUDIT_PART("reason", (!reason.empty() ? reason : EMPTY_VALUE));
        }
    );
}

void TAuditCtx::LogOnExecute(bool directed) {
    if (State != EState::PendingExecution) {
        return;
    }

    Auditable |= directed;
    Auditable &= HttpAuditEnabled(SubjectType);
    if (!Auditable) {
        return;
    }

    LogAudit(ERequestStatus::Process, REASON_EXECUTE);
    State = EState::Executing;
}

void TAuditCtx::LogOnResult(const NHttp::THttpOutgoingResponsePtr& response) {
    if (State != EState::Executing) {
        return;
    }

    auto status = GetStatus(response);
    auto reason = GetReason(response);
    LogAudit(status, reason);
    State = EState::Completed;
}

bool HttpAuditEnabled(const TIntrusiveConstPtr<NACLib::TUserToken>& userToken) {
    auto subjectType = GetSubjectType(userToken);
    return HttpAuditEnabled(subjectType);
}

bool HttpAuditEnabled(TString serializedToken) {
    NACLibProto::TUserToken userToken;
    if (userToken.ParseFromString(serializedToken)) {
        return HttpAuditEnabled(userToken.GetSubjectType());
    }
    return false;
}

}
