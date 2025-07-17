#include "mon.h"
#include "mon_audit.h"

#include <ydb/core/audit/audit_log.h>
#include <ydb/core/base/appdata.h>
#include <ydb/library/aclib/aclib.h>

#include <util/generic/string.h>

namespace NActors {

const TString MonitoringComponentName = "monitoring";
const TString EmptyValue = "{none}";

bool IsAuditEnabled(const NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev, TMon::EAuditPolicy auditPolicy) {
    const auto* mon = NKikimr::AppData()->Mon;
    if (!mon || !mon->GetConfig().AuditRequests) {
        return false;
    }

    const auto& request = ev->Get()->Request;
    if (request->Method == "OPTIONS") {
        return false;
    }

    switch (auditPolicy) {
        case TMon::EAuditPolicy::None:
            return false;
        case TMon::EAuditPolicy::PostOnly:
            return request->Method == "POST";
        case TMon::EAuditPolicy::Always:
            return true;
    }
}

void AddUserTokenAuditParts(const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, TAuditParts& parts) {
    parts.emplace_back("subject", userToken->GetUserSID());
    parts.emplace_back("sanitized_token", userToken->GetSanitizedToken());
}

void AuditRequest(const NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev,
                  const TMon::EAuditPolicy auditPolicy,
                  const TAuditParts& additionalParts) {
    if (!IsAuditEnabled(ev, auditPolicy)) {
        return;
    }

    const auto&  request = ev->Get()->Request;

    const TString url(request->URL.Before('?'));
    const auto params = TCgiParameters(request->URL.After('?'));
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

        for (const auto& [name, value] : additionalParts) {
            AUDIT_PART(name, (!value.empty() ? value : EmptyValue))
        }
        AUDIT_PART("url", url);
        AUDIT_PART("params", (!filteredParams.empty() ? filteredParams : EmptyValue))
    );
}

}
