#pragma once

#include "source_common.h"
#include "types.h"

namespace NMVP::NSupportLinks {

inline TString ResolveGrafanaUrl(TStringBuf grafanaEndpoint, const TString& configuredUrl) {
    if (IsAbsoluteUrl(configuredUrl)) {
        return configuredUrl;
    }
    return JoinUrl(grafanaEndpoint, configuredUrl);
}

inline TString ResolveGrafanaDashboardUrl(
    TStringBuf grafanaEndpoint,
    const TLinkResolveContext& context,
    TVector<TSupportError>& errors)
{
    static constexpr TStringBuf WorkspaceColumn = "k8s_namespace";
    static constexpr TStringBuf DatasourceColumn = "datasource";

    TString url = ResolveGrafanaUrl(grafanaEndpoint, context.LinkConfig.GetUrl());
    const auto workspaceIt = context.ClusterColumns.find(WorkspaceColumn);
    if (workspaceIt == context.ClusterColumns.end() || workspaceIt->second.empty()) {
        errors.emplace_back(TSupportError{
            .Source = TString(SOURCE_META),
            .Message = TStringBuilder() << "Cluster metadata column '" << WorkspaceColumn << "' is missing or empty"
        });
    } else {
        url = AppendQueryParam(url, "var-workspace", workspaceIt->second);
    }

    const auto datasourceIt = context.ClusterColumns.find(DatasourceColumn);
    if (datasourceIt == context.ClusterColumns.end() || datasourceIt->second.empty()) {
        errors.emplace_back(TSupportError{
            .Source = TString(SOURCE_META),
            .Message = TStringBuilder() << "Cluster metadata column '" << DatasourceColumn << "' is missing or empty"
        });
    } else {
        url = AppendQueryParam(url, "var-ds", datasourceIt->second);
    }

    for (const auto& [name, _] : context.UrlParameters.Parameters) {
        url = AppendQueryParam(url, TStringBuilder() << "var-" << name, context.UrlParameters[name]);
    }
    return url;
}

} // namespace NMVP::NSupportLinks
