#include "audit.h"
#include "audit_denylist.h"

#include <ydb/core/audit/audit_log.h>
#include <ydb/core/audit/audit_config/audit_config.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/util/address_classifier.h>
#include <ydb/library/aclib/aclib.h>

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

bool TAuditCtx::AuditEnabled(NKikimrConfig::TAuditConfig::TLogClassConfig::ELogPhase logPhase, NACLibProto::ESubjectType subjectType)
{
    if (NKikimr::HasAppData()) {
        return NKikimr::AppData()->AuditConfig.EnableLogging(NKikimrConfig::TAuditConfig::TLogClassConfig::ClusterAdmin,
                                                             logPhase, subjectType);
    }
    return false;
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

void TAuditCtx::InitAudit(const NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev, bool needAudit) {
    if (!(Auditable = needAudit)) {
        return;
    }

    const auto& request = ev->Get()->Request;
    if (!(Auditable = AuditableRequest(request))) {
        return;
    }

    RemoteAddress = ExtractRemoteAddress(request.Get());
    Method = TString(request->Method);
    Url = TString(request->URL.Before('?'));
    Params = ToString(request->URL.After('?'));
    if (!request->Body.Empty()) {
        TStringBuilder auditBody;
        TStringBuf body = request->Body;
        if (body.size() > MAX_AUDIT_BODY_SIZE) {
            auditBody << body.SubString(0, MAX_AUDIT_BODY_SIZE) << TRUNCATED_SUFFIX;
        } else {
            auditBody << body;
        }

        Body = auditBody;
    }
}

void TAuditCtx::AddAuditLogParts(const TAuditParts& parts) {
    if (!Auditable) {
        return;
    }
    for (const auto& [name, value] : parts) {
        // Preserve explicitly empty values; absent optional fields remain empty.
        const TString& normalizedValue = value.empty() ? EMPTY_VALUE : value;
        if (name == "subject") {
            Subject = normalizedValue;
        } else if (name == "sanitized_token") {
            SanitizedToken = normalizedValue;
        } else if (name == "cloud_id") {
            CloudId = normalizedValue;
        } else if (name == "folder_id") {
            FolderId = normalizedValue;
        } else if (name == "resource_id") {
            ResourceId = normalizedValue;
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
        AUDIT_PART("component", MONITORING_COMPONENT_NAME);
        AUDIT_PART("remote_address", (!RemoteAddress.empty() ? RemoteAddress : EMPTY_VALUE));
        AUDIT_PART("operation", DEFAULT_OPERATION);
        AUDIT_PART("method", (!Method.empty() ? Method : EMPTY_VALUE));
        AUDIT_PART("url", (!Url.empty() ? Url : EMPTY_VALUE));
        AUDIT_PART("params", Params);
        AUDIT_PART("body", Body);
        AUDIT_PART("cloud_id", CloudId);
        AUDIT_PART("folder_id", FolderId);
        AUDIT_PART("resource_id", ResourceId);
        AUDIT_PART("subject", (!Subject.empty() ? Subject : EMPTY_VALUE));
        AUDIT_PART("sanitized_token", (!SanitizedToken.empty() ? SanitizedToken : EMPTY_VALUE));
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
