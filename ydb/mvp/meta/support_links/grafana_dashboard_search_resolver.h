#pragma once

#include "common.h"
#include "events.h"
#include "resolver_base.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/http/http.h>

#include <library/cpp/json/json_reader.h>

#include <util/string/cast.h>
#include <util/generic/yexception.h>
#include <util/string/strip.h>

namespace NMVP::NSupportLinks {

class TGrafanaDashboardSearchResolver : public NActors::TActorBootstrapped<TGrafanaDashboardSearchResolver> {
public:
    TGrafanaDashboardSearchResolver(TLinkResolveContext context, const TMetaSettings& metaSettings)
        : Context(std::move(context))
        , MetaSettings(metaSettings)
    {}

    void Bootstrap() {
        TString token = ReadToken();
        if (token.empty()) {
            SendResultAndDie();
            return;
        }

        TString searchUrl = ResolveSearchUrl();
        NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestGet(searchUrl);
        httpRequest->Set("Authorization", TStringBuilder() << "Bearer " << token);

        auto request = MakeHolder<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(httpRequest);
        request->Timeout = TDuration::Seconds(30);
        Send(Context.HttpProxyId, request.Release());

        Become(&TGrafanaDashboardSearchResolver::StateWork, TDuration::Seconds(30), new NActors::TEvents::TEvWakeup());
    }

    TString ReadToken() {
        const auto& grafanaConfig = GetGrafanaConfig();
        if (grafanaConfig.SecretName.empty()) {
            Errors.emplace_back(TSupportError{
                .Source = Context.SourceName,
                .Message = "grafana.secret_name is required"
            });
            return {};
        }

        for (const auto& secret : TMVP::TokensConfig.GetSecretInfo()) {
            if (secret.GetName() != grafanaConfig.SecretName) {
                continue;
            }

            TString token = StripString(secret.GetSecret());
            if (token.empty()) {
                Errors.emplace_back(TSupportError{
                    .Source = Context.SourceName,
                    .Message = TStringBuilder()
                        << "Secret '" << grafanaConfig.SecretName << "' has empty value in auth token config secret_info"
                });
            }
            return token;
        }

        Errors.emplace_back(TSupportError{
            .Source = Context.SourceName,
            .Message = TStringBuilder()
                << "Secret '" << grafanaConfig.SecretName << "' was not found in auth token config secret_info"
        });
        return {};
    }

    TString ResolveSearchUrl() const {
        TString configuredUrl = Context.LinkConfig.GetUrl().empty()
            ? TString("/api/search")
            : Context.LinkConfig.GetUrl();
        TString searchUrl = NSupportLinks::ResolveGrafanaUrl(GetGrafanaConfig(), configuredUrl);
        if (!Context.LinkConfig.GetTag().empty()) {
            searchUrl = AppendQueryParam(searchUrl, "tag", Context.LinkConfig.GetTag());
        }
        if (!Context.LinkConfig.GetFolder().empty()) {
            searchUrl = AppendQueryParam(searchUrl, "folderUIDs", Context.LinkConfig.GetFolder());
        }
        return searchUrl;
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event) {
        if (!event->Get()->Error.empty() || !event->Get()->Response || event->Get()->Response->Status != "200") {
            AddHttpError(event);
            SendResultAndDie();
            return;
        }

        ParseDashboards(event->Get()->Response->Body);
        SendResultAndDie();
    }

    void ParseDashboards(TStringBuf body) {
        NJson::TJsonValue dashboardsJson;
        NJson::TJsonReaderConfig jsonReaderConfig;
        if (!NJson::ReadJsonTree(body, &jsonReaderConfig, &dashboardsJson) || dashboardsJson.GetType() != NJson::JSON_ARRAY) {
            Errors.emplace_back(TSupportError{
                .Source = Context.SourceName,
                .Message = "Invalid JSON from Grafana Search API"
            });
            return;
        }

        if (!ValidateRequiredClusterColumns(Errors)) {
            return;
        }

        for (const auto& item : dashboardsJson.GetArray()) {
            if (item.GetType() != NJson::JSON_MAP) {
                continue;
            }

            TString title;
            TString dashboardPath;
            if (item.Has("title") && item["title"].GetType() == NJson::JSON_STRING) {
                title = item["title"].GetStringRobust();
            }
            if (item.Has("url") && item["url"].GetType() == NJson::JSON_STRING) {
                dashboardPath = item["url"].GetStringRobust();
            } else if (item.Has("uri") && item["uri"].GetType() == NJson::JSON_STRING) {
                dashboardPath = item["uri"].GetStringRobust();
            }

            if (dashboardPath.empty()) {
                continue;
            }

            TString url = ResolveGrafanaDashboardUrl(dashboardPath);
            if (!url.empty()) {
                Links.emplace_back(TResolvedLink{
                    .Title = std::move(title),
                    .Url = std::move(url)
                });
            }
        }
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
            cFunc(NActors::TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }

    void SendSourceResponse() {
        Send(Context.Parent, new TEvPrivate::TEvSourceResponse(Context.Place, std::move(this->Links), std::move(this->Errors)));
    }

    void DieResolver() {
        PassAway();
    }

private:
    TVector<TResolvedLink> Links;
    TVector<TSupportError> Errors;
    TLinkResolveContext Context;
    const TMetaSettings& MetaSettings;

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
        SendSourceResponse();
        DieResolver();
    }

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

    TString ResolveGrafanaDashboardUrl(const TString& configuredUrl) {
        TLinkResolveContext context = Context;
        context.LinkConfig.SetUrl(configuredUrl);
        return NSupportLinks::ResolveGrafanaDashboardUrl(GetGrafanaConfig(), context, Errors);
    }
};

inline NActors::IActor* BuildGrafanaDashboardSearchResolver(TLinkResolveContext context, const TMetaSettings& metaSettings) {
    return new TGrafanaDashboardSearchResolver(std::move(context), metaSettings);
}

inline void ValidateGrafanaDashboardSearchResolverConfig(const TResolverValidationContext& context)
{
    const TString effectiveUrl = context.LinkConfig.GetUrl().empty()
        ? TString("/api/search")
        : context.LinkConfig.GetUrl();
    if (!IsAbsoluteUrl(effectiveUrl) && context.GrafanaConfig.Endpoint.empty())
    {
        ythrow yexception() << "grafana.endpoint is required for relative url";
    }
    if (context.GrafanaConfig.SecretName.empty()) {
        ythrow yexception()
            << "grafana.secret_name is required for source="
            << context.LinkConfig.GetSource();
    }
}

} // namespace NMVP::NSupportLinks
