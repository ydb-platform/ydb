#include "mon.h"
#include "mon_audit.h"

#include <ydb/core/audit/audit_log.h>
#include <ydb/core/base/appdata.h>
#include <ydb/library/aclib/aclib.h>

#include <util/generic/string.h>

namespace NActors {

namespace {
    const TString MonitoringComponentName = "monitoring";
    const TString EmptyValue = "{none}";

    enum class TAuditMode : ui8 {
        Unknown = 0,
        Off = 1,
        Auditable = 2,
    };

    //
    // None
    // Auditable (auditable)
    // (nebius) (auditable + uknown)
    //

    //  - Never
    //  - Essencial
    //  -

    static const THashMap<TString, TAuditMode> AUDIT_MODE_MAP = {
        {"/viewer", TAuditMode::Off},
        {"/viewer/query", TAuditMode::Auditable},
        {"/viewer/acl", TAuditMode::Auditable},
        {"/pdisk", TAuditMode::Off},
        {"/pdisk/restart", TAuditMode::Auditable},
        {"/pdisk/status", TAuditMode::Auditable},
        {"/vdisk", TAuditMode::Off},
        {"/vdisk/evict", TAuditMode::Auditable},
        {"/storage", TAuditMode::Off},
        {"/operation", TAuditMode::Off},
        {"/operation/cancel", TAuditMode::Auditable},
        {"/operation/forget", TAuditMode::Auditable},
        {"/scheme", TAuditMode::Off},
        {"/actors", TAuditMode::Off},
        {"/actors/failure_injection", TAuditMode::Auditable},
        {"/actors/icb", TAuditMode::Auditable},
        {"/actors/kqp_proxy", TAuditMode::Auditable},
        {"/actors/load_test", TAuditMode::Auditable},
        {"/actors/logger", TAuditMode::Auditable},
        {"/actors/pdisks", TAuditMode::Auditable},
        {"/actors/pql2", TAuditMode::Auditable},
        {"/actors/profiler", TAuditMode::Auditable},
        {"/actors/vdisks", TAuditMode::Auditable},
        {"/actors/sqsgc", TAuditMode::Auditable},
        {"/cms", TAuditMode::Off},
        {"/cms/api/console/removevolatileyamlconfig", TAuditMode::Auditable},
        {"/cms/api/console/configureyamlconfig", TAuditMode::Auditable},
        {"/cms/api/console/configurevolatileyamlconfig", TAuditMode::Auditable},
        {"/cms/api/console/configure", TAuditMode::Auditable},
        {"/cms/api/json/toggleconfigvalidator", TAuditMode::Auditable},
        {"/counter", TAuditMode::Off},
        {"/followercounters", TAuditMode::Off},
        {"/labeledcounters", TAuditMode::Off},
        {"/memory", TAuditMode::Off},
        {"/memory/heap", TAuditMode::Auditable},
        {"/nodetabmon", TAuditMode::Auditable},
        {"/tablet", TAuditMode::Auditable},
        {"/trace", TAuditMode::Auditable},
        {"/get_blob", TAuditMode::Off},
        {"/blob_range", TAuditMode::Off},
        {"/vdisk_stream", TAuditMode::Off},
        {"/grpc", TAuditMode::Off},
        {"/status", TAuditMode::Off},
        {"/fq_diag", TAuditMode::Off},
        {"/fq_diag/quotas", TAuditMode::Auditable},
        {"/tablets", TAuditMode::Auditable},
        {"/tablets/counters", TAuditMode::Off},
        {"/tablets/db", TAuditMode::Off},
        {"/tablets/executorInternals", TAuditMode::Off},
    };

    struct TAuditTreeNode {
        THashMap<TStringBuf, TAutoPtr<TAuditTreeNode>> Children;
        std::optional<TAuditMode> Policy;
    };

    class TAuditTree {
    public:
        void AddPath(const TString& path, TAuditMode mode) {
            TStringBuf remaining = path;
            TStringBuf part;
            TAuditTreeNode* node = &Root;

            while (remaining.NextTok('/', part)) {
                auto [it, inserted] = node->Children.emplace(part, nullptr);
                if (inserted || !it->second) {
                    it->second = new TAuditTreeNode();
                }
                node = it->second.Get();
            }
            node->Policy = mode;
        }

        TAuditMode Match(TStringBuf url) const {
            TStringBuf remaining = url;
            TStringBuf part;
            const TAuditTreeNode* node = &Root;
            std::optional<TAuditMode> lastMatch;

            while (remaining.NextTok('/', part)) {
                auto it = node->Children.find(part);
                if (it == node->Children.end()) {
                    break;
                }
                node = it->second.Get();
                if (node->Policy) {
                    lastMatch = node->Policy;
                }
            }

            return lastMatch.value_or(TAuditMode::Auditable);
        }

    private:
        TAuditTreeNode Root;
    };

    TAuditMode GetAuditPolicyFor(TStringBuf url) {
        static const TAuditTree tree = [] {
            TAuditTree t;
            for (const auto& [path, mode] : AUDIT_MODE_MAP) {
                t.AddPath(path, mode);
            }
            return t;
        }();

        return tree.Match(url);
    }
}

bool IsAuditEnabled(const NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev) {
    const auto* mon = NKikimr::AppData()->Mon;
    if (!mon || !mon->GetConfig().AuditRequests) {
        Cerr << "iiii audit disabled" << Endl;
        return false;
    }

    const auto& request = ev->Get()->Request;
    if (request->Method == "OPTIONS") {
        Cerr << "iiii options" << Endl;
        return false;
    }

    Cerr << "iiiiii check policy" << Endl;
    auto auditPolicy = GetAuditPolicyFor(ev->Get()->Request->URL);
    return auditPolicy == TAuditMode::Auditable;
}

void AddUserTokenAuditParts(const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, TAuditParts& parts) {
    parts.emplace_back("subject", userToken->GetUserSID());
    parts.emplace_back("sanitized_token", userToken->GetSanitizedToken());
}

void AddErrorStatusAuditParts(const TString& status, TAuditParts& parts) {
    parts.emplace_back("status", "ERROR");
    parts.emplace_back("reason", status);
}

void AddErrorStatusAuditParts(const TString& status, TAuditParts& parts) {
    parts.emplace_back("status", "ERROR");
    parts.emplace_back("reason", status);
}

void AuditRequest(const NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev,
                  const TAuditParts& additionalParts) {
    if (!IsAuditEnabled(ev)) {
        Cerr << "iiii audit disabled for this request" << Endl;
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
        AUDIT_PART("method", request->Method)
        AUDIT_PART("url", url);
        AUDIT_PART("params", (!filteredParams.empty() ? filteredParams : EmptyValue))
    );
}

}
