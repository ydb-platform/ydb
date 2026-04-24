#include "viewer_database_query_gate.h"

#include <algorithm>
#include <ydb/core/base/auth.h>

namespace NKikimr::NViewer {

namespace {

// Keep aligned with ydb/tests/functional/security/test_mon_endpoints_auth.DATABASE_ENDPOINTS_LIST
const TStringBuf VIEWER_JSON_PATHS_REQUIRING_EXPLICIT_DATABASE_QUERY[] = {
    TStringBuf("/viewer/browse"),
    TStringBuf("/viewer/compute"),
    TStringBuf("/viewer/counters"),
    TStringBuf("/viewer/graph"),
    TStringBuf("/viewer/hiveinfo"),
    TStringBuf("/viewer/hivestats"),
    TStringBuf("/viewer/labeledcounters"),
    TStringBuf("/viewer/metainfo"),
    TStringBuf("/viewer/netinfo"),
    TStringBuf("/viewer/pqconsumerinfo"),
    TStringBuf("/viewer/storage"),
    TStringBuf("/viewer/storage_usage"),
    TStringBuf("/viewer/tenants"),
    TStringBuf("/viewer/topicinfo"),
};

bool IsDatabaseOnlyMonitoringPrincipal(
    const NKikimrConfig::TDomainsConfig::TSecurityConfig& security,
    const TString& userTokenSerialized)
{
    if (userTokenSerialized.empty()) {
        return false;
    }
    return IsTokenAllowed(userTokenSerialized, security.GetDatabaseAllowedSIDs())
        && !IsTokenAllowed(userTokenSerialized, security.GetViewerAllowedSIDs())
        && !IsTokenAllowed(userTokenSerialized, security.GetMonitoringAllowedSIDs())
        && !IsTokenAllowed(userTokenSerialized, security.GetAdministrationAllowedSIDs());
}

} // namespace

bool ViewerHttpPathRequiresExplicitDatabaseQuery(TStringBuf pathWithoutQuery) {
    const auto* begin = std::begin(VIEWER_JSON_PATHS_REQUIRING_EXPLICIT_DATABASE_QUERY);
    const auto* end = std::end(VIEWER_JSON_PATHS_REQUIRING_EXPLICIT_DATABASE_QUERY);
    return std::binary_search(begin, end, pathWithoutQuery);
}

bool MonHttpRequestHasExplicitDatabaseSelector(const NMonitoring::IMonHttpRequest& request) {
    const auto& params = request.GetParams();
    return bool(params.Get("database")) || bool(params.Get("tenant"));
}

bool HttpRequestHasExplicitDatabaseSelector(const NHttp::THttpRequest& request) {
    const NHttp::TUrlParameters params(request.URL);
    return !TString(params["database"]).empty() || !TString(params["tenant"]).empty();
}

EViewerJsonDatabaseQueryGateResult EvaluateViewerJsonDatabaseQueryGate(
    const NKikimrConfig::TDomainsConfig& domainsConfig,
    TStringBuf pathWithoutQuery,
    const TString& userTokenSerialized,
    bool hasExplicitDatabaseInQuery)
{
    if (!ViewerHttpPathRequiresExplicitDatabaseQuery(pathWithoutQuery)) {
        return EViewerJsonDatabaseQueryGateResult::Allow;
    }
    if (hasExplicitDatabaseInQuery) {
        return EViewerJsonDatabaseQueryGateResult::Allow;
    }
    const auto& security = domainsConfig.GetSecurityConfig();
    if (IsDatabaseOnlyMonitoringPrincipal(security, userTokenSerialized)) {
        return EViewerJsonDatabaseQueryGateResult::ForbiddenMissingDatabaseQueryForDatabaseSid;
    }
    return EViewerJsonDatabaseQueryGateResult::Allow;
}

} // namespace NKikimr::NViewer
