#include "audit.h"
#include "audit_denylist.cpp"

#include <ydb/core/audit/audit_log.h>
#include <ydb/core/audit/audit_config/audit_config.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/util/address_classifier.h>
#include <ydb/library/aclib/aclib.h>

#include <util/generic/is_in.h>
#include <util/generic/hash_set.h>
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

    TString GetReason(const NHttp::THttpOutgoingResponsePtr& response) {
        TStringBuilder reason;
        reason << response.Get()->Status << " " << response.Get()->Message;
        return reason;
    }

    inline TUrlMatcher CreateDenylistMatcher() {
        TUrlMatcher policy;
        for (const auto& pattern : AUDIT_DENYLIST) {
            policy.AddPattern(pattern);
        }
        return policy;
    }
}

TString ExtractRemoteAddress(const NHttp::THttpIncomingRequest* request) {
    if (!request) {
        return {};
    }
    NHttp::THeaders headers(request->Headers);

    TString remoteAddress = ToString(headers.Get(X_FORWARDED_FOR_HEADER).Before(',')); // Get the first address in the list
    if (remoteAddress.empty()) {
        remoteAddress = NKikimr::NAddressClassifier::ExtractAddress(request->Address->ToString());
    }
    return remoteAddress;
}

TString ExtractRemoteAddress(const IMonHttpRequest* request) {
    if (!request) {
        return {};
    }
    const auto& headers = request->GetHeaders();
    const auto* forwardedHeader = headers.FindHeader(X_FORWARDED_FOR_HEADER);
    TString remoteAddress;
    if (forwardedHeader) {
        remoteAddress = ToString(TStringBuf(forwardedHeader->Value()).Before(',')); // Get the first address in the list
    }
    if (remoteAddress.empty()) {
        remoteAddress = NKikimr::NAddressClassifier::ExtractAddress(request->GetRemoteAddr());
    }
    return remoteAddress;
}

bool TAuditCtx::AuditEnabled(NKikimrConfig::TAuditConfig::TLogClassConfig::ELogPhase logPhase, NACLibProto::ESubjectType subjectType)
{
    if (NKikimr::HasAppData()) {
        return NKikimr::AppData()->AuditConfig.EnableLogging(NKikimrConfig::TAuditConfig::TLogClassConfig::ClusterAdmin,
                                                             logPhase, subjectType);
    }
    return false;
}


void TAuditCtx::AddAuditLogPart(TStringBuf name, const TString& value) {
    Parts.emplace_back(name, value);
}

bool TAuditCtx::AuditableRequest(const NHttp::THttpIncomingRequestPtr& request) const {
    // modifying methods are always audited
    const TString method(request->Method);
    static const THashSet<TString> MODIFYING_METHODS = {"POST", "PUT", "DELETE"};
    if (MODIFYING_METHODS.contains(method)) {
        return true;
    }

    // OPTIONS are not audited
    if (method == "OPTIONS") {
        return false;
    }

    // skip audit for URLs from denylist
    static auto DENYLIST_MATCHER = CreateDenylistMatcher();
    if (DENYLIST_MATCHER.Match(request->URL)) {
        return false;
    }

    return true;
}

void TAuditCtx::InitAudit(const NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev) {
    const auto& request = ev->Get()->Request;
    const TString method(request->Method);
    const TString url(request->URL.Before('?'));
    const auto params = request->URL.After('?');
    const auto cgiParams = TCgiParameters(params);

    if (!(Auditable = AuditableRequest(ev->Get()->Request))) {
        return;
    }

    TString remoteAddress = ExtractRemoteAddress(request.Get());

    AddAuditLogPart("component", MONITORING_COMPONENT_NAME);
    AddAuditLogPart("remote_address", remoteAddress ? remoteAddress : EMPTY_VALUE);
    AddAuditLogPart("operation", DEFAULT_OPERATION);
    AddAuditLogPart("method", method);
    AddAuditLogPart("url", url);
    if (!params.Empty()) {
        AddAuditLogPart("params", ToString(params));
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
}

void TAuditCtx::AddAuditLogParts(const TAuditParts& parts) {
    if (!Auditable) {
        return;
    }
    // TODO: refactor so that all the parts are logged
    static const THashSet<TString> ALLOWED_PARTS = {
        "subject",
        "sanitized_token",
        // "start_time",
        // "end_time",
        // "database",
        // "remote_address",
        "cloud_id",
        "folder_id",
        "resource_id",
    };
    for (const auto& [k, v] : parts) {
        if (IsIn(ALLOWED_PARTS, k)) {
            Parts.emplace_back(k, v);
        }
    }
}

void TAuditCtx::SetSubjectType(NACLibProto::ESubjectType subjectType) {
    SubjectType = subjectType;
}

void TAuditCtx::LogAudit(ERequestStatus status, const TString& reason, NKikimrConfig::TAuditConfig::TLogClassConfig::ELogPhase logPhase) {
    if (!Auditable || !AuditEnabled(logPhase, SubjectType)) {
        return;
    }

    AUDIT_LOG(
        for (const auto& [name, value] : Parts) {
            AUDIT_PART(name, (!value.empty() ? value : EMPTY_VALUE));
        }

        AUDIT_PART("status", ToString(status));
        AUDIT_PART("reason", reason, !reason.empty());
    );
}

void TAuditCtx::LogOnReceived() {
    LogAudit(ERequestStatus::Process, REASON_EXECUTE, NKikimrConfig::TAuditConfig::TLogClassConfig::Received);
}

void TAuditCtx::LogOnCompleted(const NHttp::THttpOutgoingResponsePtr& response) {
    auto status = GetStatus(response);
    auto reason = GetReason(response);
    LogAudit(status, reason, NKikimrConfig::TAuditConfig::TLogClassConfig::Completed);
}

}
