#pragma once

#include "source_common.h"

namespace NMVP::NSupportLinks {

inline TString ResolveGrafanaUrl(const TGrafanaSupportConfig& grafanaConfig, const TString& configuredUrl) {
    if (IsAbsoluteUrl(configuredUrl)) {
        return configuredUrl;
    }
    return JoinUrl(grafanaConfig.Endpoint, configuredUrl);
}

inline TString ResolveGrafanaDashboardUrl(
    const TGrafanaSupportConfig& grafanaConfig,
    const TLinkResolveContext& context,
    TVector<TSupportError>& errors)
{
    static constexpr TStringBuf WorkspaceColumn = "workspace";
    static constexpr TStringBuf DatasourceColumn = "grafana_ds";

    TString url = ResolveGrafanaUrl(grafanaConfig, context.LinkConfig.GetUrl());
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

    for (const auto& [name, value] : context.QueryParams) {
        url = AppendQueryParam(url, TStringBuilder() << "var-" << name, value);
    }
    return url;
}

} // namespace NMVP::NSupportLinks
