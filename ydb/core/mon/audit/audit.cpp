#include "audit.h"
<<<<<<< HEAD
=======
#include "audit_force.h"
#include "url_tree.h"
>>>>>>> ea31ec8811c (audit log http)

#include <ydb/core/audit/audit_log.h>
#include <ydb/core/base/appdata.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/core/protos/config.pb.h>

#include <util/generic/string.h>

namespace NActors {

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

<<<<<<< HEAD
=======
bool TAuditCtx::NeedAudit() const {
    return Need;
}

>>>>>>> ea31ec8811c (audit log http)
void TAuditCtx::AddAuditLogPart(TStringBuf name, const TString& value) {
    Parts.emplace_back(name, value);
}

<<<<<<< HEAD
bool TAuditCtx::CheckAuditConditions(const TString& method) {
    return false; // change when audit config is ready
=======
bool TAuditCtx::CheckAuditConditions(const TString& method, const TString& url, const TCgiParameters& params) {
    // if (!NKikimr::AppData()->AuditConfig.GetMonitoringAudit()) {
    //     return false;
    // }

    // OPTIONS are not audited
    if (method == "OPTIONS") {
        return false;
    }
>>>>>>> ea31ec8811c (audit log http)

    // only modifying methods are audited
    static const THashSet<TString> MODIFYING_METHODS = {"POST", "PUT", "DELETE"};
    if (MODIFYING_METHODS.contains(method)) {
        return true;
    }

<<<<<<< HEAD
    // OPTIONS are not audited
    if (method == "OPTIONS") {
        return false;
=======
    // force audit for specific URLs
    static const auto FORCE_AUDIT_URL_PATTERN = CreateAuditUrlPattern();
    if (FORCE_AUDIT_URL_PATTERN.Match(url, params)) {
        return true;
>>>>>>> ea31ec8811c (audit log http)
    }

    return false;
}

void TAuditCtx::InitAudit(const NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev) {
    const auto& request = ev->Get()->Request;
    const TString method(request->Method);
    const TString url(request->URL.Before('?'));
    const auto params = request->URL.After('?');
    const auto cgiParams = TCgiParameters(params);
<<<<<<< HEAD
    if (!(Need = CheckAuditConditions(method))) {
=======
    if (!(Need = CheckAuditConditions(method, url, cgiParams))) {
>>>>>>> ea31ec8811c (audit log http)
        return;
    }

    NHttp::THeaders headers(request->Headers);
    auto remote_address = ToString(headers.Get(X_FORWARDED_FOR_HEADER).Before(',')); // Get the first address in the list

    AddAuditLogPart("component", MONITORING_COMPONENT_NAME);
    AddAuditLogPart("remote_address", remote_address);
    AddAuditLogPart("operation", DEFAULT_OPERATION);
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
<<<<<<< HEAD
    if (!Need) {
=======
    if (!NeedAudit()) {
>>>>>>> ea31ec8811c (audit log http)
        return;
    }
    AddAuditLogPart("subject", userToken->GetUserSID());
    AddAuditLogPart("sanitized_token", userToken->GetSanitizedToken());
}

void TAuditCtx::FinishAudit(const NHttp::THttpOutgoingResponsePtr& response) {
<<<<<<< HEAD
    if (!Need) {
=======
    if (!NeedAudit()) {
>>>>>>> ea31ec8811c (audit log http)
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
