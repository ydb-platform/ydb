#pragma once

#include "source_common.h"
#include "types.h"

namespace NMVP::NSupportLinks {

inline TString BuildGrafanaDashboardUrl(
    TStringBuf grafanaEndpoint,
    TStringBuf url,
    const THashMap<TString, TString>& clusterInfo,
    const NHttp::TUrlParameters& requestUrlParameters)
{
    static constexpr TStringBuf WorkspaceKey = "k8s_namespace";
    static constexpr TStringBuf DatasourceKey = "datasource";

    TString resolvedUrl = IsAbsoluteUrl(url)
        ? TString(url)
        : JoinUrl(grafanaEndpoint, url);
    const auto workspaceIt = clusterInfo.find(WorkspaceKey);
    if (workspaceIt != clusterInfo.end() && !workspaceIt->second.empty()) {
        resolvedUrl = AppendQueryParam(resolvedUrl, "var-workspace", workspaceIt->second);
    }

    const auto datasourceIt = clusterInfo.find(DatasourceKey);
    if (datasourceIt != clusterInfo.end() && !datasourceIt->second.empty()) {
        resolvedUrl = AppendQueryParam(resolvedUrl, "var-ds", datasourceIt->second);
    }

    for (const auto& [name, _] : requestUrlParameters.Parameters) {
        resolvedUrl = AppendQueryParam(resolvedUrl, TStringBuilder() << "var-" << name, requestUrlParameters[name]);
    }
    return resolvedUrl;
}

} // namespace NMVP::NSupportLinks
