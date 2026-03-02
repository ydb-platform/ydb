#pragma once

#include "common.h"
#include "resolver_base.h"

namespace NMVP::NSupportLinks {

template <class TDerived>
class TGrafanaResolverBase : public TResolverBase<TDerived> {
protected:
    using TResolverBase<TDerived>::Context;

    explicit TGrafanaResolverBase(TLinkResolveContext context)
        : TResolverBase<TDerived>(std::move(context))
    {}

    const TGrafanaSupportConfig& GetGrafanaConfig() const {
        return InstanceMVP->GrafanaSupportConfig;
    }

    bool ValidateRequiredClusterColumns(TVector<TSupportError>& errors) const {
        const auto& grafanaConfig = GetGrafanaConfig();
        bool ok = true;

        const auto workspaceIt = Context.ClusterColumns.find(grafanaConfig.WorkspaceColumn);
        if (workspaceIt == Context.ClusterColumns.end() || workspaceIt->second.empty()) {
            errors.emplace_back(TSupportError{
                .Source = TString(SOURCE_META),
                .Message = TStringBuilder() << "Cluster metadata column '" << grafanaConfig.WorkspaceColumn << "' is missing or empty"
            });
            ok = false;
        }

        const auto datasourceIt = Context.ClusterColumns.find(grafanaConfig.DatasourceColumn);
        if (datasourceIt == Context.ClusterColumns.end() || datasourceIt->second.empty()) {
            errors.emplace_back(TSupportError{
                .Source = TString(SOURCE_META),
                .Message = TStringBuilder() << "Cluster metadata column '" << grafanaConfig.DatasourceColumn << "' is missing or empty"
            });
            ok = false;
        }

        return ok;
    }

    TString ResolveGrafanaUrl(const TString& configuredUrl) const {
        if (IsAbsoluteUrl(configuredUrl)) {
            return configuredUrl;
        }
        return JoinUrl(GetGrafanaConfig().Endpoint, configuredUrl);
    }

    TString ResolveGrafanaDashboardUrl(const TString& configuredUrl, TVector<TSupportError>& errors) const {
        TString url = ResolveGrafanaUrl(configuredUrl);
        const auto& grafanaConfig = GetGrafanaConfig();
        const auto workspaceIt = Context.ClusterColumns.find(grafanaConfig.WorkspaceColumn);
        if (workspaceIt == Context.ClusterColumns.end() || workspaceIt->second.empty()) {
            errors.emplace_back(TSupportError{
                .Source = TString(SOURCE_META),
                .Message = TStringBuilder() << "Cluster metadata column '" << grafanaConfig.WorkspaceColumn << "' is missing or empty"
            });
        } else {
            url = AppendQueryParam(url, "var-workspace", workspaceIt->second);
        }

        const auto datasourceIt = Context.ClusterColumns.find(grafanaConfig.DatasourceColumn);
        if (datasourceIt == Context.ClusterColumns.end() || datasourceIt->second.empty()) {
            errors.emplace_back(TSupportError{
                .Source = TString(SOURCE_META),
                .Message = TStringBuilder() << "Cluster metadata column '" << grafanaConfig.DatasourceColumn << "' is missing or empty"
            });
        } else {
            url = AppendQueryParam(url, "var-ds", datasourceIt->second);
        }

        for (const auto& [name, value] : Context.QueryParams) {
            url = AppendQueryParam(url, TStringBuilder() << "var-" << name, value);
        }
        return url;
    }
};

} // namespace NMVP::NSupportLinks
