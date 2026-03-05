#pragma once

#include "common.h"
#include "events.h"

#include <ydb/library/actors/http/http.h>

#include <util/string/cast.h>

namespace NMVP::NSupportLinks {

template <class TDerived>
class TResolverBase {
protected:
    TVector<TResolvedLink> Links;
    TVector<TSupportError> Errors;
    TLinkResolveContext Context;

    explicit TResolverBase(TLinkResolveContext context)
        : Context(std::move(context))
    {}

    void AddHttpError(const NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr& event) {
        TSupportError error;
        error.Source = Context.SourceName;
        if (event->Get()->Response) {
            ui32 status = 0;
            if (TryFromString<ui32>(event->Get()->Response->Status, status)) {
                error.Status = status;
            }
            error.Reason = TString(event->Get()->Response->Message);
        }
        if (!event->Get()->Error.empty()) {
            error.Message = event->Get()->Error;
        } else if (event->Get()->Response) {
            error.Message = TStringBuilder() << event->Get()->Response->Status << " " << event->Get()->Response->Message;
        } else {
            error.Message = "Unknown Grafana request error";
        }
        Errors.emplace_back(std::move(error));
    }

    void SendResultAndDie() {
        static_cast<TDerived*>(this)->SendSourceResponse();
        static_cast<TDerived*>(this)->DieResolver();
    }
};

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

template <class TDerived>
class TGrafanaResolverBase : public TResolverBase<TDerived> {
protected:
    using TResolverBase<TDerived>::Context;

    explicit TGrafanaResolverBase(TLinkResolveContext context, const TMetaSettings& metaSettings)
        : TResolverBase<TDerived>(std::move(context))
        , MetaSettings(metaSettings)
    {}

    const TGrafanaSupportConfig& GetGrafanaConfig() const {
        return MetaSettings.GrafanaConfig;
    }

    bool ValidateRequiredClusterColumns(TVector<TSupportError>& errors) const {
        static constexpr TStringBuf WorkspaceColumn = "workspace";
        static constexpr TStringBuf DatasourceColumn = "grafana_ds";
        bool ok = true;

        const auto workspaceIt = Context.ClusterColumns.find(WorkspaceColumn);
        if (workspaceIt == Context.ClusterColumns.end() || workspaceIt->second.empty()) {
            errors.emplace_back(TSupportError{
                .Source = TString(SOURCE_META),
                .Message = TStringBuilder() << "Cluster metadata column '" << WorkspaceColumn << "' is missing or empty"
            });
            ok = false;
        }

        const auto datasourceIt = Context.ClusterColumns.find(DatasourceColumn);
        if (datasourceIt == Context.ClusterColumns.end() || datasourceIt->second.empty()) {
            errors.emplace_back(TSupportError{
                .Source = TString(SOURCE_META),
                .Message = TStringBuilder() << "Cluster metadata column '" << DatasourceColumn << "' is missing or empty"
            });
            ok = false;
        }

        return ok;
    }

    TString ResolveGrafanaUrl(const TString& configuredUrl) const {
        return NSupportLinks::ResolveGrafanaUrl(GetGrafanaConfig(), configuredUrl);
    }

    TString ResolveGrafanaDashboardUrl(const TString& configuredUrl, TVector<TSupportError>& errors) const {
        TLinkResolveContext context = Context;
        context.LinkConfig.SetUrl(configuredUrl);
        return NSupportLinks::ResolveGrafanaDashboardUrl(GetGrafanaConfig(), context, errors);
    }

private:
    const TMetaSettings& MetaSettings;
};

} // namespace NMVP::NSupportLinks
