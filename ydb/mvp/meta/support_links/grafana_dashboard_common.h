#pragma once

#include "source_common.h"
#include "types.h"

#include <library/cpp/cgiparam/cgiparam.h>

namespace NMVP::NSupportLinks {

inline void ApplyGrafanaDashboardBindingPolicy(
    TCgiParameters& queryParameters,
    const THashMap<TString, TString>& clusterInfo,
    const NHttp::TUrlParameters& requestUrlParameters)
{
    static constexpr TStringBuf WorkspaceKey = "k8s_namespace";
    static constexpr TStringBuf DatasourceKey = "datasource";

    const auto workspaceIt = clusterInfo.find(WorkspaceKey);
    if (workspaceIt != clusterInfo.end() && !workspaceIt->second.empty()) {
        queryParameters.InsertUnescaped("var-workspace", workspaceIt->second);
    }

    const auto datasourceIt = clusterInfo.find(DatasourceKey);
    if (datasourceIt != clusterInfo.end() && !datasourceIt->second.empty()) {
        queryParameters.InsertUnescaped("var-ds", datasourceIt->second);
    }

    for (const auto& parameter : requestUrlParameters.Parameters) {
        queryParameters.InsertUnescaped(TStringBuilder() << "var-" << parameter.first, requestUrlParameters[parameter.first]);
    }
}

inline TString BuildGrafanaDashboardUrl(
    TStringBuf grafanaEndpoint,
    TStringBuf url,
    const THashMap<TString, TString>& clusterInfo,
    const NHttp::TUrlParameters& requestUrlParameters)
{
    TString resolvedUrl = IsAbsoluteUrl(url)
        ? TString(url)
        : JoinUrl(grafanaEndpoint, url);
    const TStringBuf path = TStringBuf(resolvedUrl).Before('?');
    const TStringBuf queryString = TStringBuf(resolvedUrl).After('?');

    TCgiParameters queryParameters;
    if (!queryString.empty()) {
        queryParameters.Scan(queryString);
    }
    ApplyGrafanaDashboardBindingPolicy(queryParameters, clusterInfo, requestUrlParameters);

    return queryParameters.empty()
        ? TString(path)
        : TStringBuilder() << path << '?' << queryParameters.Print();
}

} // namespace NMVP::NSupportLinks
