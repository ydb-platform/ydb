#pragma once

#include "events.h"
#include "grafana_dashboard_common.h"
#include "grafana_dashboard_probe.h"
#include "source_common.h"

#include <ydb/mvp/core/appdata.h>
#include <ydb/mvp/core/mvp_log.h>
#include <ydb/mvp/meta/mvp.h>
#include <ydb/mvp/meta/support_links/source.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/http/http.h>

#include <library/cpp/json/json_reader.h>

#include <util/generic/vector.h>
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
        AuthorizationHeaderValue = GetAuthorizationHeaderValue();
        SendGrafanaGetRequest(BuildSearchUrl(), "search dashboards");
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
    TString AuthorizationHeaderValue;
    TVector<TGrafanaDashboardCandidate> DashboardCandidates;
    size_t CurrentDashboardIndex = 0;
    TVector<TGrafanaProbeGroup> CurrentProbeGroups;
    size_t CurrentProbeGroupIndex = 0;

    enum class ERequestKind {
        Search,
        DashboardModel,
        Probe,
    };

    ERequestKind RequestKind = ERequestKind::Search;

    TString GetAuthorizationHeaderValue() const {
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

    void SendGrafanaGetRequest(TString url, TStringBuf purpose) {
        BLOG_D("Support links Grafana request: purpose=" << purpose << " method=GET url=" << url);

        NHttp::THttpOutgoingRequestPtr request = NHttp::THttpOutgoingRequest::CreateRequestGet(url);
        if (!AuthorizationHeaderValue.empty()) {
            request->Set("Authorization", AuthorizationHeaderValue);
        }

        auto event = MakeHolder<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(request);
        Send(HttpProxyId, event.Release());
    }

    static TString DescribeHttpFailure(const NHttp::TEvHttpProxy::TEvHttpIncomingResponse& response) {
        if (!response.Error.empty()) {
            return response.Error;
        }
        if (response.Response) {
            return TStringBuilder() << response.Response->Status << ' ' << response.Response->Message;
        }
        return "Unknown Grafana request error";
    }

    static bool IsSuccessfulResponse(const NHttp::TEvHttpProxy::TEvHttpIncomingResponse& response) {
        return response.Error.empty() && response.Response && response.Response->Status == "200";
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event) {
        switch (RequestKind) {
            case ERequestKind::Search:
                HandleSearchResponse(event);
                return;
            case ERequestKind::DashboardModel:
                HandleDashboardModelResponse(event);
                return;
            case ERequestKind::Probe:
                HandleProbeResponse(event);
                return;
        }
    }

    void HandleSearchResponse(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event) {
        if (!IsSuccessfulResponse(*event->Get())) {
            TSupportError error;
            error.Source = Config.GetSource();
            error.Message = DescribeHttpFailure(*event->Get());
            if (event->Get()->Response) {
                ui32 status = 0;
                if (TryFromString<ui32>(event->Get()->Response->Status, status)) {
                    error.Status = status;
                }
                error.Reason = TString(event->Get()->Response->Message);
            }
            Errors.push_back(std::move(error));
            ReplyAndDie();
            return;
        }

        ParseSearchResponse(event->Get()->Response->Body);
        if (!Errors.empty() || DashboardCandidates.empty()) {
            ReplyAndDie();
            return;
        }

        CurrentDashboardIndex = 0;
        RequestDashboardModel();
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

            TGrafanaDashboardCandidate candidate;
            if (TryBuildGrafanaDashboardCandidate(
                    item,
                    GrafanaEndpoint,
                    ClusterInfo,
                    RequestQueryParameters,
                    candidate))
            {
                DashboardCandidates.push_back(std::move(candidate));
            } else {
                BLOG_D("Support links Grafana skip dashboard without resolvable probe metadata");
            }
        }
    }

    void RequestDashboardModel() {
        if (CurrentDashboardIndex >= DashboardCandidates.size()) {
            ReplyAndDie();
            return;
        }

        RequestKind = ERequestKind::DashboardModel;
        const auto& candidate = DashboardCandidates[CurrentDashboardIndex];
        SendGrafanaGetRequest(candidate.DashboardApiUrl, "fetch dashboard model");
    }

    void HandleDashboardModelResponse(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event) {
        const auto& candidate = DashboardCandidates[CurrentDashboardIndex];
        if (!IsSuccessfulResponse(*event->Get())) {
            BLOG_W("Support links Grafana dashboard model fetch failed: title=" << candidate.Title
                << " url=" << candidate.DashboardApiUrl
                << " error=" << DescribeHttpFailure(*event->Get()));
            AdvanceToNextDashboard();
            return;
        }

        NJson::TJsonValue responseJson;
        NJson::TJsonReaderConfig jsonReaderConfig;
        if (!NJson::ReadJsonTree(event->Get()->Response->Body, &jsonReaderConfig, &responseJson) || responseJson.GetType() != NJson::JSON_MAP) {
            BLOG_W("Support links Grafana dashboard model has invalid JSON: title=" << candidate.Title
                << " url=" << candidate.DashboardApiUrl);
            AdvanceToNextDashboard();
            return;
        }

        const NJson::TJsonValue* dashboard = &responseJson;
        if (responseJson.Has("dashboard") && responseJson["dashboard"].GetType() == NJson::JSON_MAP) {
            dashboard = &responseJson["dashboard"];
        }

        CurrentProbeGroups = BuildGrafanaDashboardProbeGroups(*dashboard, candidate.QueryParameters);
        CurrentProbeGroupIndex = 0;
        if (CurrentProbeGroups.empty()) {
            BLOG_D("Support links Grafana dashboard has no probeable buckets: title=" << candidate.Title
                << " url=" << candidate.ResolvedUrl);
            AdvanceToNextDashboard();
            return;
        }

        RequestProbe();
    }

    void RequestProbe() {
        if (CurrentProbeGroupIndex >= CurrentProbeGroups.size()) {
            AdvanceToNextDashboard();
            return;
        }

        const auto& probeGroup = CurrentProbeGroups[CurrentProbeGroupIndex];
        TString url = JoinUrl(
            GrafanaEndpoint,
            TStringBuilder() << "/api/datasources/proxy/uid/" << probeGroup.DatasourceUid << "/api/v1/query");
        url = AppendQueryParam(url, "query", probeGroup.Query);

        RequestKind = ERequestKind::Probe;
        SendGrafanaGetRequest(url, "probe dashboard signal");
    }

    void HandleProbeResponse(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event) {
        const auto& candidate = DashboardCandidates[CurrentDashboardIndex];
        const auto& probeGroup = CurrentProbeGroups[CurrentProbeGroupIndex];
        if (!IsSuccessfulResponse(*event->Get())) {
            BLOG_W("Support links Grafana probe request failed: title=" << candidate.Title
                << " datasource=" << probeGroup.DatasourceUid
                << " error=" << DescribeHttpFailure(*event->Get()));
            AdvanceToNextProbeGroup();
            return;
        }

        if (HasGrafanaDashboardProbeSignal(event->Get()->Response->Body)) {
            Links.emplace_back(TResolvedLink{
                .Title = candidate.Title,
                .Url = candidate.ResolvedUrl,
            });
            AdvanceToNextDashboard();
            return;
        }

        AdvanceToNextProbeGroup();
    }

    void AdvanceToNextProbeGroup() {
        ++CurrentProbeGroupIndex;
        RequestProbe();
    }

    void AdvanceToNextDashboard() {
        CurrentProbeGroups.clear();
        CurrentProbeGroupIndex = 0;
        ++CurrentDashboardIndex;
        RequestDashboardModel();
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
