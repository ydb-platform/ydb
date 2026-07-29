#include "grafana_dashboard_search_source.h"

#include "events.h"
#include "grafana_dashboard_common.h"
#include "source_common.h"

#include <ydb/mvp/core/appdata.h>
#include <ydb/mvp/meta/mvp.h>

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
namespace {

class TGrafanaDashboardSearchActor : public NActors::TActorBootstrapped<TGrafanaDashboardSearchActor> {
public:
    TGrafanaDashboardSearchActor(
        TSupportLinkEntryConfig config,
        TString grafanaEndpoint,
        TResolvedParamBindings paramBindings,
        const ILinkSource::TLinkResolveInput& input,
        const ILinkSource::TResolveContext& context)
        : Config(std::move(config))
        , GrafanaEndpoint(std::move(grafanaEndpoint))
        , ParamBindings(std::move(paramBindings))
        , ClusterInfo(input.ClusterInfo)
        , RequestQueryParameters(BuildForwardedParameters(input.Identity, input.AdditionalRequestParams))
        , Context(context)
    {}

    void Bootstrap() {
        const TString authHeaderValue = GetAuthorizationHeaderValue();
        NHttp::THttpOutgoingRequestPtr request = NHttp::THttpOutgoingRequest::CreateRequestGet(BuildSearchUrl());
        request->Set("Authorization", authHeaderValue);

        auto event = MakeHolder<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(request);
        Send(Context.HttpProxyId, event.Release());
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
    TResolvedParamBindings ParamBindings;
    THashMap<TString, TString> ClusterInfo;
    TCgiParameters RequestQueryParameters;
    ILinkSource::TResolveContext Context;
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
                RequestQueryParameters,
                ParamBindings);
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
        Send(Context.Owner, new TEvPrivate::TEvSourceResponse(Context.Place, std::move(Links), std::move(Errors)));
        PassAway();
    }
};

class TGrafanaDashboardSearchSource : public ILinkSource {
public:
    TGrafanaDashboardSearchSource(TSupportLinkEntryConfig config, TString grafanaEndpoint, TResolvedParamBindings paramBindings)
        : Config(std::move(config))
        , GrafanaEndpoint(std::move(grafanaEndpoint))
        , ParamBindings(std::move(paramBindings))
    {}

    TResolveOutput Resolve(const ILinkSource::TLinkResolveInput& input, const ILinkSource::TResolveContext& context) const override {
        TResolveOutput result{
            .Name = Config.GetSource(),
        };
        result.Actor = NActors::TActivationContext::Register(
            new TGrafanaDashboardSearchActor(
                Config,
                GrafanaEndpoint,
                ParamBindings,
                input,
                context),
            context.Owner);
        return result;
    }

private:
    TSupportLinkEntryConfig Config;
    TString GrafanaEndpoint;
    TResolvedParamBindings ParamBindings;
};

} // namespace

void ValidateGrafanaDashboardSearchSourceConfig(const TSupportLinkEntryConfig& config, const TMetaSettings& metaSettings) {
    if (metaSettings.SupportLinks.GrafanaEndpoint.empty()) {
        ythrow yexception() << "grafana.endpoint is required for source=" << config.GetSource();
    }
    if (TMVP::MetaDatabaseTokenName.empty()) {
        ythrow yexception() << "meta.meta_database_token_name is required for source=" << config.GetSource();
    }
    ValidateParamsAreUnique(ResolveParamBindings(config, BuildDefaultGrafanaDashboardParamBindings()), config);
}

std::shared_ptr<ILinkSource> MakeGrafanaDashboardSearchSource(TSupportLinkEntryConfig config, const TMetaSettings& metaSettings) {
    ValidateGrafanaDashboardSearchSourceConfig(config, metaSettings);
    auto paramBindings = ResolveParamBindings(config, BuildDefaultGrafanaDashboardParamBindings());
    return std::make_shared<TGrafanaDashboardSearchSource>(
        std::move(config),
        metaSettings.SupportLinks.GrafanaEndpoint,
        std::move(paramBindings));
}

} // namespace NMVP::NSupportLinks
