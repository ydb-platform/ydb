#pragma once

#include "common.h"
#include "events.h"
#include "resolver_base.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/http/http.h>

#include <library/cpp/json/json_reader.h>

#include <util/generic/yexception.h>
#include <util/string/strip.h>

namespace NMVP::NSupportLinks {

class TGrafanaDashboardSearchResolver
    : public NActors::TActorBootstrapped<TGrafanaDashboardSearchResolver>
    , public TGrafanaResolverBase<TGrafanaDashboardSearchResolver> {
public:
    explicit TGrafanaDashboardSearchResolver(TLinkResolveContext context)
        : TGrafanaResolverBase<TGrafanaDashboardSearchResolver>(std::move(context))
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
        TString configuredUrl = Context.LinkConfig.Url.empty()
            ? TString("/api/search")
            : Context.LinkConfig.Url;
        TString searchUrl = TGrafanaResolverBase<TGrafanaDashboardSearchResolver>::ResolveGrafanaUrl(configuredUrl);
        if (!Context.LinkConfig.Tag.empty()) {
            searchUrl = AppendQueryParam(searchUrl, "tag", Context.LinkConfig.Tag);
        }
        if (!Context.LinkConfig.Folder.empty()) {
            searchUrl = AppendQueryParam(searchUrl, "folderUIDs", Context.LinkConfig.Folder);
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

            TString url = ResolveGrafanaDashboardUrl(dashboardPath, Errors);
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
            cFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void HandleTimeout() {
        Errors.emplace_back(TSupportError{
            .Source = Context.SourceName,
            .Message = "Timeout while calling Grafana Search API"
        });
        SendResultAndDie();
    }

    void SendSourceResponse() {
        Send(Context.Parent, new TEvPrivate::TEvSourceResponse(Context.Place, std::move(this->Links), std::move(this->Errors)));
    }

    void DieResolver() {
        PassAway();
    }
};

inline NActors::IActor* BuildGrafanaDashboardSearchResolver(TLinkResolveContext context) {
    return new TGrafanaDashboardSearchResolver(std::move(context));
}

inline void ValidateGrafanaDashboardSearchResolverConfig(const TResolverValidationContext& context)
{
    const TString effectiveUrl = context.LinkConfig.Url.empty()
        ? TString("/api/search")
        : context.LinkConfig.Url;
    if (!IsAbsoluteUrl(effectiveUrl) && context.GrafanaConfig.Endpoint.empty())
    {
        ythrow yexception() << context.Where << ": grafana.endpoint is required for relative url";
    }
    if (context.GrafanaConfig.SecretName.empty()) {
        ythrow yexception()
            << context.Where
            << ": grafana.secret_name is required for source="
            << context.LinkConfig.Source;
    }
}

} // namespace NMVP::NSupportLinks
