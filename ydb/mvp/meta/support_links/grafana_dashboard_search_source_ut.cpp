#include <ydb/mvp/meta/mvp.h>
#include <ydb/mvp/meta/support_links/events.h>
#include <ydb/mvp/meta/support_links/grafana_dashboard_search_source.h>

#include <ydb/mvp/core/mvp_test_runtime.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/http/http_proxy.h>

#include <library/cpp/cgiparam/cgiparam.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>

#include <utility>

namespace {

class TTestActorRuntime : public TMvpTestRuntime {
public:
    TTestActorRuntime()
        : TMvpTestRuntime(1, false)
    {
        Initialize();
    }
};

class TMetaDatabaseTokenNameGuard {
public:
    explicit TMetaDatabaseTokenNameGuard(TString value)
        : OldValue(std::exchange(NMVP::TMVP::MetaDatabaseTokenName, std::move(value)))
    {}

    ~TMetaDatabaseTokenNameGuard() {
        NMVP::TMVP::MetaDatabaseTokenName = OldValue;
    }

private:
    TString OldValue;
};

NMVP::TMetaSettings MakeMetaSettings(TStringBuf grafanaEndpoint = "https://grafana.example.net") {
    NMVP::TMetaSettings settings;
    settings.SupportLinks.GrafanaEndpoint = TString(grafanaEndpoint);
    return settings;
}

NMVP::TSupportLinkEntryConfig MakeConfig(
    TStringBuf url = TStringBuf(),
    const TVector<TString>& tag = {},
    const TVector<TString>& folders = {})
{
    NMVP::TSupportLinkEntryConfig config;
    config.SetSource("grafana/dashboard/search");
    if (!url.empty()) {
        config.SetUrl(TString(url));
    }
    for (const TString& value : tag) {
        if (!value.empty()) {
            config.AddTag(value);
        }
    }
    for (const TString& folder : folders) {
        if (!folder.empty()) {
            config.AddFolder(folder);
        }
    }
    return config;
}

NHttp::TUrlParametersBuilder MakeUrlParameters(TStringBuf query) {
    NHttp::TUrlParametersBuilder builder;
    for (TStringBuf param = query.NextTok('&'); !param.empty(); param = query.NextTok('&')) {
        TStringBuf name = param.NextTok('=');
        builder.Set(name, param);
    }
    return builder;
}

class TResolveRunnerActor : public NActors::TActorBootstrapped<TResolveRunnerActor> {
public:
    TResolveRunnerActor(
        std::shared_ptr<NMVP::ILinkSource> source,
        THashMap<TString, TString> clusterInfo,
        NHttp::TUrlParametersBuilder urlParameters,
        NActors::TActorId replyTo,
        NActors::TActorId httpProxyId)
        : Source(std::move(source))
        , ClusterInfo(std::move(clusterInfo))
        , UrlParameters(std::move(urlParameters))
        , ReplyTo(replyTo)
        , HttpProxyId(httpProxyId)
    {}

    void Bootstrap() {
        auto result = Source->Resolve(
            NMVP::ILinkSource::TLinkResolveInput{
                .ClusterInfo = ClusterInfo,
                .UrlParameters = UrlParameters,
            },
            NMVP::ILinkSource::TResolveContext{
                .Place = 0,
                .Owner = SelfId(),
                .HttpProxyId = HttpProxyId,
            });
        if (!result.Actor) {
            Send(ReplyTo, new NMVP::NSupportLinks::TEvPrivate::TEvSourceResponse(0, std::move(result.Links), std::move(result.Errors)));
            PassAway();
            return;
        }
        Become(&TResolveRunnerActor::StateWork);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NMVP::NSupportLinks::TEvPrivate::TEvSourceResponse, Handle);
        }
    }

    void Handle(NMVP::NSupportLinks::TEvPrivate::TEvSourceResponse::TPtr event) {
        Send(ReplyTo, new NMVP::NSupportLinks::TEvPrivate::TEvSourceResponse(
            event->Get()->Place,
            std::move(event->Get()->Links),
            std::move(event->Get()->Errors)));
        PassAway();
    }

private:
    std::shared_ptr<NMVP::ILinkSource> Source;
    THashMap<TString, TString> ClusterInfo;
    NHttp::TUrlParametersBuilder UrlParameters;
    NActors::TActorId ReplyTo;
    NActors::TActorId HttpProxyId;
};

class TSearchApiRequestCheckActor : public NActors::TActor<TSearchApiRequestCheckActor> {
public:
    using TBase = NActors::TActor<TSearchApiRequestCheckActor>;

    TSearchApiRequestCheckActor()
        : TBase(&TSearchApiRequestCheckActor::StateWork)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpOutgoingRequest::TPtr event) {
        const auto request = event->Get()->Request;
        const TString url(request->URL);
        const bool targetsSearchApi = url.Contains("/api/search");
        TCgiParameters query;
        const size_t queryPos = url.find('?');
        if (queryPos != TString::npos) {
            query.Scan(TStringBuf(url).SubStr(queryPos + 1));
        }

        THashSet<TString> requestTags;
        for (const TString& value : query.Range("tag")) {
            requestTags.insert(value);
        }

        THashSet<TString> requestFolders;
        for (const TString& value : query.Range("folderUIDs")) {
            requestFolders.insert(value);
        }

        struct TDashboardCandidate {
            TString Title;
            TString Url;
            THashSet<TString> Tags;
            TString FolderUid;
        };

        const TVector<TDashboardCandidate> dashboards = {
            {
                .Title = "YDB Cluster Overview",
                .Url = "/d/matched/dashboard",
                .Tags = {"ydb-common", "ydb-storage"},
                .FolderUid = "ops-folder",
            },
            {
                .Title = "YDB Database Overview",
                .Url = "/d/missing-tag/dashboard",
                .Tags = {"ydb-common"},
                .FolderUid = "ops-folder",
            },
            {
                .Title = "YDB Storage Overview",
                .Url = "/d/wrong-folder/dashboard",
                .Tags = {"ydb-common", "ydb-storage"},
                .FolderUid = "other-folder",
            },
        };

        NJson::TJsonValue responseJson(NJson::JSON_ARRAY);
        if (targetsSearchApi) {
            for (const auto& dashboard : dashboards) {
                bool matchesTags = true;
                for (const TString& tag : requestTags) {
                    if (!dashboard.Tags.contains(tag)) {
                        matchesTags = false;
                        break;
                    }
                }

                const bool matchesFolders = requestFolders.empty() || requestFolders.contains(dashboard.FolderUid);
                if (!matchesTags || !matchesFolders) {
                    continue;
                }

                NJson::TJsonValue item(NJson::JSON_MAP);
                item["type"] = "dash-db";
                item["title"] = dashboard.Title;
                item["url"] = dashboard.Url;
                responseJson.AppendValue(std::move(item));
            }
        }

        TString body = NJson::WriteJson(responseJson, false);
        const TString statusLine = targetsSearchApi
            ? "HTTP/1.1 200 OK"
            : "HTTP/1.1 400 Bad Request";

        NHttp::THttpIncomingResponsePtr response = new NHttp::THttpIncomingResponse(request);
        EatWholeString(
            response,
            TStringBuilder()
                << statusLine << "\r\n"
                << "Connection: close\r\n"
                << "Content-Type: application/json; charset=utf-8\r\n"
                << "Content-Length: " << body.size() << "\r\n"
                << "\r\n"
                << body);
        Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(request, response));
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpOutgoingRequest, Handle);
        }
    }
};

class TGrafanaSearchReplyActor : public NActors::TActor<TGrafanaSearchReplyActor> {
public:
    using TBase = NActors::TActor<TGrafanaSearchReplyActor>;

    explicit TGrafanaSearchReplyActor(TString body)
        : TBase(&TGrafanaSearchReplyActor::StateWork)
        , Body(std::move(body))
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpOutgoingRequest::TPtr event) {
        const auto request = event->Get()->Request;
        NHttp::THttpIncomingResponsePtr response = new NHttp::THttpIncomingResponse(request);
        EatWholeString(
            response,
            TStringBuilder()
                << "HTTP/1.1 200 OK\r\n"
                << "Connection: close\r\n"
                << "Content-Type: application/json; charset=utf-8\r\n"
                << "Content-Length: " << Body.size() << "\r\n"
                << "\r\n"
                << Body);
        Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(request, response));
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpOutgoingRequest, Handle);
        }
    }

private:
    TString Body;
};

class TForbiddenReplyActor : public NActors::TActor<TForbiddenReplyActor> {
public:
    using TBase = NActors::TActor<TForbiddenReplyActor>;

    TForbiddenReplyActor()
        : TBase(&TForbiddenReplyActor::StateWork)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpOutgoingRequest::TPtr event) {
        const auto request = event->Get()->Request;
        const TString body = R"({"message":"forbidden"})";
        NHttp::THttpIncomingResponsePtr response = new NHttp::THttpIncomingResponse(request);
        EatWholeString(
            response,
            TStringBuilder()
                << "HTTP/1.1 403 Forbidden\r\n"
                << "Connection: close\r\n"
                << "Content-Type: application/json; charset=utf-8\r\n"
                << "Content-Length: " << body.size() << "\r\n"
                << "\r\n"
                << body);
        Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(request, response));
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpOutgoingRequest, Handle);
        }
    }
};

NMVP::NSupportLinks::TEvPrivate::TEvSourceResponse* ResolveSource(
    TTestActorRuntime& runtime,
    std::shared_ptr<NMVP::ILinkSource> source,
    THashMap<TString, TString> clusterInfo,
    NHttp::TUrlParametersBuilder urlParameters,
    NActors::TActorId httpProxyId,
    TAutoPtr<NActors::IEventHandle>& handle)
{
    const auto replyTo = runtime.AllocateEdgeActor();
    runtime.Register(new TResolveRunnerActor(
        std::move(source),
        std::move(clusterInfo),
        std::move(urlParameters),
        replyTo,
        httpProxyId));
    return runtime.GrabEdgeEvent<NMVP::NSupportLinks::TEvPrivate::TEvSourceResponse>(handle);
}

void AssertUrlQuery(TStringBuf actualUrl, TStringBuf expectedUrl) {
    UNIT_ASSERT_VALUES_EQUAL(actualUrl.Before('?'), expectedUrl.Before('?'));

    TCgiParameters actualQuery;
    TCgiParameters expectedQuery;
    actualQuery.Scan(actualUrl.After('?'));
    expectedQuery.Scan(expectedUrl.After('?'));
    UNIT_ASSERT_VALUES_EQUAL(actualQuery.Print(), expectedQuery.Print());
}

} // namespace

Y_UNIT_TEST_SUITE(SupportLinksGrafanaDashboardSearchSource) {
    Y_UNIT_TEST(ValidationRejectsRelativeUrlWithoutGrafanaEndpoint) {
        TMetaDatabaseTokenNameGuard tokenNameGuard("meta-token");
        auto settings = MakeMetaSettings("");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            NMVP::MakeGrafanaDashboardSearchSource(MakeConfig("/api/search"), settings),
            yexception,
            "grafana.endpoint is required for source=grafana/dashboard/search");
    }

    Y_UNIT_TEST(ValidationRejectsAbsoluteUrlWithoutGrafanaEndpoint) {
        TMetaDatabaseTokenNameGuard tokenNameGuard("meta-token");
        auto settings = MakeMetaSettings("");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            NMVP::MakeGrafanaDashboardSearchSource(MakeConfig("https://grafana.example.net/api/search"), settings),
            yexception,
            "grafana.endpoint is required for source=grafana/dashboard/search");
    }

    Y_UNIT_TEST(ValidationRejectsMissingMetaDatabaseTokenName) {
        TMetaDatabaseTokenNameGuard tokenNameGuard("");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            NMVP::MakeGrafanaDashboardSearchSource(MakeConfig("/api/search"), MakeMetaSettings()),
            yexception,
            "meta.meta_database_token_name is required for source=grafana/dashboard/search");
    }

    Y_UNIT_TEST(ResolveUsesAllTagAndFolderFiltersInSearchRequest) {
        TMetaDatabaseTokenNameGuard tokenNameGuard("meta-token");
        TTestActorRuntime runtime;

        auto source = NMVP::MakeGrafanaDashboardSearchSource(
            MakeConfig("/api/search", TVector<TString>{"ydb-common", "ydb-storage"}, TVector<TString>{"team-folder", "ops-folder"}),
            MakeMetaSettings());
        auto httpProxyId = runtime.Register(new TSearchApiRequestCheckActor());

        TAutoPtr<NActors::IEventHandle> handle;
        auto* response = ResolveSource(runtime, source, {}, MakeUrlParameters(""), httpProxyId, handle);
        UNIT_ASSERT_VALUES_EQUAL(response->Errors.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(response->Links.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Links[0].Title, "YDB Cluster Overview");
        UNIT_ASSERT_VALUES_EQUAL(response->Links[0].Url, "https://grafana.example.net/d/matched/dashboard");
    }

    Y_UNIT_TEST(ResolveBuildsDashboardLinksFromSearchResponse) {
        TMetaDatabaseTokenNameGuard tokenNameGuard("meta-token");
        TTestActorRuntime runtime;

        const TString body = R"json([
            {"type":"dash-db","title":"CPU","url":"/d/ydb_cpu/cpu"},
            {"type":"dashboard","title":"DB overview","uri":"db/db-overview"},
            {"type":"dash-folder","title":"Folder","url":"/dashboards/f/team-folder"},
            {"title":"Skip me"}
        ])json";
        auto httpProxyId = runtime.Register(new TGrafanaSearchReplyActor(body));
        auto source = NMVP::MakeGrafanaDashboardSearchSource(
            MakeConfig("/api/search", TVector<TString>{"ydb-common"}),
            MakeMetaSettings());

        THashMap<TString, TString> clusterInfo;
        clusterInfo["k8s_namespace"] = "ydb-workspace";
        clusterInfo["datasource"] = "3f8a1e2c-6b7d-4c91-9a52-1d7f0e8b4a63";

        TAutoPtr<NActors::IEventHandle> handle;
        auto* response = ResolveSource(
            runtime,
            source,
            clusterInfo,
            MakeUrlParameters("cluster=ydb-global&database=%2Froot%2Ftest"),
            httpProxyId,
            handle);

        UNIT_ASSERT_VALUES_EQUAL(response->Errors.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(response->Links.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(response->Links[0].Title, "CPU");
        AssertUrlQuery(
            response->Links[0].Url,
            "https://grafana.example.net/d/ydb_cpu/cpu?var-workspace=ydb-workspace&var-ds=3f8a1e2c-6b7d-4c91-9a52-1d7f0e8b4a63&var-cluster=ydb-global&var-database=/root/test");
        UNIT_ASSERT_VALUES_EQUAL(response->Links[1].Title, "DB overview");
        AssertUrlQuery(
            response->Links[1].Url,
            "https://grafana.example.net/db/db-overview?var-workspace=ydb-workspace&var-ds=3f8a1e2c-6b7d-4c91-9a52-1d7f0e8b4a63&var-cluster=ydb-global&var-database=/root/test");
    }

    Y_UNIT_TEST(ResolveReturnsHttpError) {
        TMetaDatabaseTokenNameGuard tokenNameGuard("meta-token");
        TTestActorRuntime runtime;

        auto httpProxyId = runtime.Register(new TForbiddenReplyActor());
        auto source = NMVP::MakeGrafanaDashboardSearchSource(MakeConfig(), MakeMetaSettings());

        TAutoPtr<NActors::IEventHandle> handle;
        auto* response = ResolveSource(runtime, source, {}, MakeUrlParameters(""), httpProxyId, handle);
        UNIT_ASSERT_VALUES_EQUAL(response->Links.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(response->Errors.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Errors[0].Source, "grafana/dashboard/search");
        UNIT_ASSERT_VALUES_EQUAL(*response->Errors[0].Status, 403);
        UNIT_ASSERT_VALUES_EQUAL(response->Errors[0].Reason, "Forbidden");
    }
}
