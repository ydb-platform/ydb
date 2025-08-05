#include "audit.h"

#include <ydb/core/audit/audit_log.h>
#include <ydb/core/base/appdata.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/core/protos/config.pb.h>

#include <util/generic/string.h>

namespace NActors::NAudit {

namespace {
    const TString MONITORING_COMPONENT_NAME = "monitoring";
    const TString DEFAULT_OPERATION = "HTTP REQUEST";
    const TString EMPTY_VALUE = "{none}";
    const TString X_FORWARDED_FOR_HEADER = "X-Forwarded-For";

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

bool TAuditCtx::AuditEnabled() const {
    return true; // TODO: Implement audit enabled check
}

bool TAuditCtx::CheckAuditable(const TString& method, const EAuditableAction action) const {
    // specific methods are always audited
    static const THashSet<TString> MODIFYING_METHODS = {"POST", "PUT", "DELETE"};
    if (MODIFYING_METHODS.contains(method)) {
        return true;
    }

    // OPTIONS are never audited
    if (method == "OPTIONS") {
        return false;
    }

    // specific auditable action are audited
    if (action != EAuditableAction::Unknown) {
        Cerr << "iiii Auditable " << Endl;
        return true;
    }

    return false;
}

void TAuditCtx::InitAudit(const NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev, const EAuditableAction action) {
    Cerr << "iiii Init Audit " << Endl;
    if (!AuditEnabled()) {
        return;
    }
    Cerr << "iiii AuditEnabled " << Endl;
    const auto& request = ev->Get()->Request;
    const TString method(request->Method);
    const TString url(request->URL.Before('?'));
    const auto params = request->URL.After('?');
    const auto cgiParams = TCgiParameters(params);
    Auditable |= CheckAuditable(method, action);

    NHttp::THeaders headers(request->Headers);
    auto remote_address = ToString(headers.Get(X_FORWARDED_FOR_HEADER).Before(',')); // Get the first address in the list

    AddAuditLogPart("component", MONITORING_COMPONENT_NAME);
    AddAuditLogPart("remote_address", remote_address);
    AddAuditLogPart("operation", ToString(action));
    AddAuditLogPart("method", method);
    AddAuditLogPart("url", url);
    if (!params.Empty()) {
        AddAuditLogPart("params", ToString(params));
    }
    if (!request->Body.Empty()) {
        AddAuditLogPart("body", ToString(request->Body));
    }
}

void TAuditCtx::AddAuditLogParts(const TIntrusiveConstPtr<NACLib::TUserToken>& userToken) {
    if (!AuditEnabled()) {
        return;
    }
    AddAuditLogPart("subject", userToken->GetUserSID());
    AddAuditLogPart("sanitized_token", userToken->GetSanitizedToken());
}

void TAuditCtx::FinishAudit(const NHttp::THttpOutgoingResponsePtr& response) {
    if (!AuditEnabled() || !Auditable) {
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
