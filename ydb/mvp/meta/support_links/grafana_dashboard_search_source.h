#pragma once

#include "events.h"
#include "grafana_dashboard_common.h"
#include "source_common.h"

#include <ydb/mvp/core/appdata.h>
#include <ydb/mvp/meta/mvp.h>
#include <ydb/mvp/meta/support_links/source.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/http/http.h>

#include <library/cpp/json/json_reader.h>

#include <util/generic/yexception.h>
#include <util/string/cast.h>
#include <util/string/strip.h>

namespace NMVP::NSupportLinks {

class TGrafanaDashboardSearchActor : public NActors::TActorBootstrapped<TGrafanaDashboardSearchActor> {
public:
    TGrafanaDashboardSearchActor(
        TSupportLinkEntryConfig config,
        TString grafanaEndpoint,
        const THashMap<TString, TString>& clusterInfo,
        TCgiParameters requestQueryParameters,
        NActors::TActorId owner,
        NActors::TActorId httpProxyId,
        size_t place)
        : Config(std::move(config))
        , GrafanaEndpoint(std::move(grafanaEndpoint))
        , ClusterInfo(clusterInfo)
        , RequestQueryParameters(std::move(requestQueryParameters))
        , Owner(owner)
        , HttpProxyId(httpProxyId)
        , Place(place)
    {}

    void Bootstrap() {
        const TString authHeaderValue = GetAuthorizationHeaderValue();
        NHttp::THttpOutgoingRequestPtr request = NHttp::THttpOutgoingRequest::CreateRequestGet(BuildSearchUrl());
        request->Set("Authorization", authHeaderValue);

        auto event = MakeHolder<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(request);
        Send(HttpProxyId, event.Release());
        Become(&TGrafanaDashboardSearchActor::StateWork);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
            cFunc(NActors::TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }

private:
    TSupportLinkEntryConfig Config;
    TString GrafanaEndpoint;
    THashMap<TString, TString> ClusterInfo;
    TCgiParameters RequestQueryParameters;
    NActors::TActorId Owner;
    NActors::TActorId HttpProxyId;
    size_t Place = 0;
    TVector<TResolvedLink> Links;
    TVector<TSupportError> Errors;

    TString GetAuthorizationHeaderValue() {
        auto* appData = MVPAppData();
        if (!appData || !appData->Tokenator) {
            return {};
        }
        return StripString(appData->Tokenator->GetToken(TMVP::MetaDatabaseTokenName));
    }

    TString BuildSearchUrl() const {
        TString url = Config.GetUrl().empty() ? TString("/api/search") : Config.GetUrl();
        url = IsAbsoluteUrl(url) ? url : JoinUrl(GrafanaEndpoint, url);
        for (size_t i = 0; i < Config.TagSize(); ++i) {
            if (!Config.GetTag(i).empty()) {
                url = AppendQueryParam(url, "tag", Config.GetTag(i));
            }
        }
        for (size_t i = 0; i < Config.FolderSize(); ++i) {
            if (!Config.GetFolder(i).empty()) {
                url = AppendQueryParam(url, "folderUIDs", Config.GetFolder(i));
            }
        }
        return url;
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event) {
        if (!event->Get()->Error.empty() || !event->Get()->Response || event->Get()->Response->Status != "200") {
            TSupportError error;
            error.Source = Config.GetSource();
            if (!event->Get()->Error.empty()) {
                error.Message = event->Get()->Error;
            } else if (event->Get()->Response) {
                ui32 status = 0;
                if (TryFromString<ui32>(event->Get()->Response->Status, status)) {
                    error.Status = status;
                }
                error.Reason = TString(event->Get()->Response->Message);
                error.Message = TStringBuilder() << event->Get()->Response->Status << " " << event->Get()->Response->Message;
            } else {
                error.Message = "Unknown Grafana request error";
            }
            Errors.push_back(std::move(error));
            ReplyAndDie();
            return;
        }

        ParseSearchResponse(event->Get()->Response->Body);
        ReplyAndDie();
    }

    void ParseSearchResponse(TStringBuf body) {
        NJson::TJsonValue dashboardsJson;
        NJson::TJsonReaderConfig jsonReaderConfig;
        if (!NJson::ReadJsonTree(body, &jsonReaderConfig, &dashboardsJson) || dashboardsJson.GetType() != NJson::JSON_ARRAY) {
            Errors.emplace_back(TSupportError{
                .Source = Config.GetSource(),
                .Message = "Invalid JSON from Grafana Search API",
            });
            return;
        }

        for (const auto& item : dashboardsJson.GetArray()) {
            if (item.GetType() != NJson::JSON_MAP) {
                continue;
            }
            if (!IsDashboardItem(item)) {
                continue;
            }

            TString title;
            TString dashboardUrl;
            if (item.Has("title") && item["title"].GetType() == NJson::JSON_STRING) {
                title = item["title"].GetString();
            }
            if (item.Has("url") && item["url"].GetType() == NJson::JSON_STRING) {
                dashboardUrl = item["url"].GetString();
            } else if (item.Has("uri") && item["uri"].GetType() == NJson::JSON_STRING) {
                dashboardUrl = item["uri"].GetString();
            }

            if (dashboardUrl.empty()) {
                continue;
            }

            const TString resolvedUrl = BuildGrafanaDashboardUrl(
                GrafanaEndpoint,
                dashboardUrl,
                ClusterInfo,
                RequestQueryParameters);
            if (!resolvedUrl.empty()) {
                Links.emplace_back(TResolvedLink{
                    .Title = std::move(title),
                    .Url = resolvedUrl,
                });
            }
        }
    }

    static bool IsDashboardItem(const NJson::TJsonValue& item) {
        if (!item.Has("type") || item["type"].GetType() != NJson::JSON_STRING) {
            return false;
        }

        const TString type = item["type"].GetString();
        return type == "dash-db" || type == "dashboard";
    }

    void ReplyAndDie() {
        Send(Owner, new TEvPrivate::TEvSourceResponse(Place, std::move(Links), std::move(Errors)));
        PassAway();
    }
};

} // namespace NMVP::NSupportLinks

namespace NMVP {

class TGrafanaDashboardSearchSource : public ILinkSource {
public:
    TGrafanaDashboardSearchSource(TSupportLinkEntryConfig config, TString grafanaEndpoint)
        : Config(std::move(config))
        , GrafanaEndpoint(std::move(grafanaEndpoint))
    {}

    TResolveOutput Resolve(const TLinkResolveInput& input, const TResolveContext& context) const override {
        TResolveOutput result{
            .Name = Config.GetSource(),
        };
        result.Actor = NActors::TActivationContext::Register(
            new NSupportLinks::TGrafanaDashboardSearchActor(
                Config,
                GrafanaEndpoint,
                input.ClusterInfo,
                NSupportLinks::BuildRequestQueryParameters(input.UrlParameters),
                context.Owner,
                context.HttpProxyId,
                context.Place),
            context.Owner);
        return result;
    }

private:
    TSupportLinkEntryConfig Config;
    TString GrafanaEndpoint;
};

inline void ValidateGrafanaDashboardSearchSourceConfig(const TSupportLinkEntryConfig& config, const TMetaSettings& metaSettings) {
    if (metaSettings.SupportLinks.GrafanaEndpoint.empty()) {
        ythrow yexception() << "grafana.endpoint is required for source=" << config.GetSource();
    }
    if (TMVP::MetaDatabaseTokenName.empty()) {
        ythrow yexception() << "meta.meta_database_token_name is required for source=" << config.GetSource();
    }
}

inline std::shared_ptr<ILinkSource> MakeGrafanaDashboardSearchSource(TSupportLinkEntryConfig config, const TMetaSettings& metaSettings) {
    ValidateGrafanaDashboardSearchSourceConfig(config, metaSettings);
    return std::make_shared<TGrafanaDashboardSearchSource>(
        std::move(config),
        metaSettings.SupportLinks.GrafanaEndpoint
    );
}

} // namespace NMVP
