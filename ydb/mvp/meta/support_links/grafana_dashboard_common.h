#pragma once

#include "source_common.h"
#include "types.h"

#include <library/cpp/cgiparam/cgiparam.h>

namespace NMVP::NSupportLinks {

inline constexpr TStringBuf GRAFANA_WORKSPACE_KEY = "k8s_namespace";
inline constexpr TStringBuf GRAFANA_DATASOURCE_KEY = "datasource";

inline void ApplyGrafanaDashboardClusterBindings(
    TCgiParameters& queryParameters,
    const THashMap<TString, TString>& clusterInfo)
{
    const auto workspaceIt = clusterInfo.find(GRAFANA_WORKSPACE_KEY);
    if (workspaceIt != clusterInfo.end() && !workspaceIt->second.empty()) {
        queryParameters.InsertUnescaped("var-workspace", workspaceIt->second);
    }

    const auto datasourceIt = clusterInfo.find(GRAFANA_DATASOURCE_KEY);
    if (datasourceIt != clusterInfo.end() && !datasourceIt->second.empty()) {
        queryParameters.InsertUnescaped("var-ds", datasourceIt->second);
    }
}

inline std::pair<TString, TCgiParameters> BuildGrafanaDashboardUrlParts(
    TStringBuf grafanaEndpoint,
    TStringBuf url)
{
    TString resolvedUrl = IsAbsoluteUrl(url)
        ? TString(url)
        : JoinUrl(grafanaEndpoint, url);
    TString path = TString(TStringBuf(resolvedUrl).Before('?'));
    const TStringBuf queryString = TStringBuf(resolvedUrl).After('?');

    TCgiParameters queryParameters;
    if (!queryString.empty()) {
        queryParameters.Scan(queryString);
    }

    return {std::move(path), std::move(queryParameters)};
}

inline TCgiParameters BuildRequestQueryParameters(const NHttp::TUrlParameters& urlParameters)
{
    TCgiParameters queryParameters;
    for (const auto& parameter : urlParameters.Parameters) {
        queryParameters.InsertUnescaped(parameter.first, urlParameters[parameter.first]);
    }
    return queryParameters;
}

inline void ApplyGrafanaDashboardBindingPolicy(
    TCgiParameters& queryParameters,
    const THashMap<TString, TString>& clusterInfo,
    const TCgiParameters& requestQueryParameters)
{
    ApplyGrafanaDashboardClusterBindings(queryParameters, clusterInfo);

    for (const auto& [name, value] : requestQueryParameters) {
        queryParameters.InsertUnescaped(TStringBuilder() << "var-" << name, value);
    }
}

inline TString BuildGrafanaDashboardUrl(
    TStringBuf grafanaEndpoint,
    TStringBuf url,
    const THashMap<TString, TString>& clusterInfo,
    const TCgiParameters& requestQueryParameters)
{
    auto [path, queryParameters] = BuildGrafanaDashboardUrlParts(grafanaEndpoint, url);
    ApplyGrafanaDashboardBindingPolicy(queryParameters, clusterInfo, requestQueryParameters);

    return queryParameters.empty()
        ? path
        : TStringBuilder() << path << '?' << queryParameters.Print();
}

inline TString BuildGrafanaDashboardUrl(
    TStringBuf grafanaEndpoint,
    TStringBuf url,
    const THashMap<TString, TString>& clusterInfo,
    const NHttp::TUrlParameters& requestUrlParameters)
{
    return BuildGrafanaDashboardUrl(
        grafanaEndpoint,
        url,
        clusterInfo,
        BuildRequestQueryParameters(requestUrlParameters));
}

} // namespace NMVP::NSupportLinks
