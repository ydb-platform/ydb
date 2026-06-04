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
#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>
#include <util/stream/file.h>

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

TString MakeDashboardModelBody(TStringBuf query, TStringBuf datasource = "${ds}") {
    NJson::TJsonValue root(NJson::JSON_MAP);
    NJson::TJsonValue dashboard(NJson::JSON_MAP);
    NJson::TJsonValue templating(NJson::JSON_MAP);
    NJson::TJsonValue list(NJson::JSON_ARRAY);
    NJson::TJsonValue item(NJson::JSON_MAP);
    NJson::TJsonValue datasourceJson(NJson::JSON_MAP);

    datasourceJson["uid"] = TString(datasource);
    item["name"] = "probe";
    item["datasource"] = std::move(datasourceJson);
    item["query"] = TString(query);
    list.AppendValue(std::move(item));
    templating["list"] = std::move(list);
    dashboard["templating"] = std::move(templating);
    root["dashboard"] = std::move(dashboard);

    return NJson::WriteJson(root, false);
}

TString MakeProbeResponseBody(TStringBuf value) {
    NJson::TJsonValue root(NJson::JSON_MAP);
    NJson::TJsonValue data(NJson::JSON_MAP);
    NJson::TJsonValue result(NJson::JSON_ARRAY);
    NJson::TJsonValue item(NJson::JSON_MAP);
    NJson::TJsonValue sample(NJson::JSON_ARRAY);
    NJson::TJsonValue metric(NJson::JSON_MAP);

    sample.AppendValue(1717500000);
    sample.AppendValue(TString(value));
    item["metric"] = std::move(metric);
    item["value"] = std::move(sample);
    result.AppendValue(std::move(item));
    data["resultType"] = "vector";
    data["result"] = std::move(result);
    root["status"] = "success";
    root["data"] = std::move(data);

    return NJson::WriteJson(root, false);
}

NJson::TJsonValue ReadFixtureJson(TStringBuf relativePath) {
    const TString path = ArcadiaFromCurrentLocation(__SOURCE_FILE__, relativePath);
    const TString body = TUnbufferedFileInput(path).ReadAll();

    NJson::TJsonValue value;
    NJson::TJsonReaderConfig jsonReaderConfig;
    UNIT_ASSERT_C(NJson::ReadJsonTree(body, &jsonReaderConfig, &value), "failed to parse json fixture");
    return value;
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
            TString Uid;
            THashSet<TString> Tags;
            TString FolderUid;
        };

        const TVector<TDashboardCandidate> dashboards = {
            {
                .Title = "YDB Cluster Overview",
                .Url = "/d/matched/dashboard",
                .Uid = "matched",
                .Tags = {"ydb-common", "ydb-storage"},
                .FolderUid = "ops-folder",
            },
            {
                .Title = "YDB Database Overview",
                .Url = "/d/missing-tag/dashboard",
                .Uid = "missing-tag",
                .Tags = {"ydb-common"},
                .FolderUid = "ops-folder",
            },
            {
                .Title = "YDB Storage Overview",
                .Url = "/d/wrong-folder/dashboard",
                .Uid = "wrong-folder",
                .Tags = {"ydb-common", "ydb-storage"},
                .FolderUid = "other-folder",
            },
        };

        if (targetsSearchApi) {
            NJson::TJsonValue responseJson(NJson::JSON_ARRAY);
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
                item["uid"] = dashboard.Uid;
                responseJson.AppendValue(std::move(item));
            }

            ReplyJson(event, "HTTP/1.1 200 OK", NJson::WriteJson(responseJson, false));
            return;
        }

        if (url.Contains("/api/dashboards/uid/matched")) {
            ReplyJson(
                event,
                "HTTP/1.1 200 OK",
                MakeDashboardModelBody("label_values(ElapsedMicrosec{__workspace__=\"$workspace\",__bucket__=\"utils\",database=\"$database\"},database)"));
            return;
        }

        if (url.Contains("/api/datasources/proxy/uid/")) {
            UNIT_ASSERT(url.Contains("__bucket__%3D%22utils%22"));
            UNIT_ASSERT(url.Contains("database%3D%22/root/test%22"));
            ReplyJson(event, "HTTP/1.1 200 OK", MakeProbeResponseBody("1"));
            return;
        }

        ReplyJson(event, "HTTP/1.1 400 Bad Request", R"({"message":"bad request"})");
    }

    void ReplyJson(
        NHttp::TEvHttpProxy::TEvHttpOutgoingRequest::TPtr event,
        TStringBuf statusLine,
        TStringBuf body)
    {
        const auto request = event->Get()->Request;
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

class TGrafanaApiReplyActor : public NActors::TActor<TGrafanaApiReplyActor> {
public:
    using TBase = NActors::TActor<TGrafanaApiReplyActor>;

    struct TRule {
        TString UrlContains;
        TString StatusLine;
        TString Body;
    };

    explicit TGrafanaApiReplyActor(TVector<TRule> rules)
        : TBase(&TGrafanaApiReplyActor::StateWork)
        , Rules(std::move(rules))
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpOutgoingRequest::TPtr event) {
        const auto request = event->Get()->Request;
        const TString url(request->URL);
        for (const auto& rule : Rules) {
            if (!url.Contains(rule.UrlContains)) {
                continue;
            }
            Reply(event, rule.StatusLine, rule.Body);
            return;
        }

        Reply(event, "HTTP/1.1 404 Not Found", R"({"message":"not found"})");
    }

    void Reply(
        NHttp::TEvHttpProxy::TEvHttpOutgoingRequest::TPtr event,
        TStringBuf statusLine,
        TStringBuf body)
    {
        const auto request = event->Get()->Request;
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

private:
    TVector<TRule> Rules;
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

        THashMap<TString, TString> clusterInfo;
        clusterInfo["k8s_namespace"] = "ydb-workspace";
        clusterInfo["datasource"] = "3f8a1e2c-6b7d-4c91-9a52-1d7f0e8b4a63";

        TAutoPtr<NActors::IEventHandle> handle;
        auto* response = ResolveSource(runtime, source, clusterInfo, MakeUrlParameters("database=%2Froot%2Ftest"), httpProxyId, handle);
        UNIT_ASSERT_VALUES_EQUAL(response->Errors.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(response->Links.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Links[0].Title, "YDB Cluster Overview");
        AssertUrlQuery(
            response->Links[0].Url,
            "https://grafana.example.net/d/matched/dashboard?var-workspace=ydb-workspace&var-ds=3f8a1e2c-6b7d-4c91-9a52-1d7f0e8b4a63&var-database=/root/test");
    }

    Y_UNIT_TEST(ResolveBuildsDashboardLinksFromSearchResponse) {
        TMetaDatabaseTokenNameGuard tokenNameGuard("meta-token");
        TTestActorRuntime runtime;

        const TString body = R"json([
            {"type":"dash-db","title":"CPU","url":"/d/ydb_cpu/cpu","uid":"ydb_cpu"},
            {"type":"dashboard","title":"DB overview","uri":"db/db-overview"},
            {"type":"dash-folder","title":"Folder","url":"/dashboards/f/team-folder"},
            {"title":"Skip me"}
        ])json";
        TVector<TGrafanaApiReplyActor::TRule> rules = {
            {.UrlContains = "/api/search", .StatusLine = "HTTP/1.1 200 OK", .Body = body},
            {.UrlContains = "/api/dashboards/uid/ydb_cpu", .StatusLine = "HTTP/1.1 200 OK", .Body = MakeDashboardModelBody("label_values(ElapsedMicrosec{__workspace__=\"$workspace\",__bucket__=\"utils\",database=\"$database\"},database)")},
            {.UrlContains = "/api/dashboards/db/db-overview", .StatusLine = "HTTP/1.1 200 OK", .Body = MakeDashboardModelBody("label_values(requestBytes{__workspace__=\"$workspace\",__bucket__=\"dsproxynode\",database=\"$database\"},storagePool)")},
            {.UrlContains = "/api/datasources/proxy/uid/3f8a1e2c-6b7d-4c91-9a52-1d7f0e8b4a63/api/v1/query", .StatusLine = "HTTP/1.1 200 OK", .Body = MakeProbeResponseBody("1")},
        };
        auto httpProxyId = runtime.Register(new TGrafanaApiReplyActor(std::move(rules)));
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

    Y_UNIT_TEST(ResolveFiltersDashboardsWithoutSignal) {
        TMetaDatabaseTokenNameGuard tokenNameGuard("meta-token");
        TTestActorRuntime runtime;

        const TString body = R"json([
            {"type":"dash-db","title":"CPU","url":"/d/ydb_cpu/cpu","uid":"ydb_cpu"},
            {"type":"dash-db","title":"Storage","url":"/d/storage/storage","uid":"storage"}
        ])json";
        TVector<TGrafanaApiReplyActor::TRule> rules = {
            {.UrlContains = "/api/search", .StatusLine = "HTTP/1.1 200 OK", .Body = body},
            {.UrlContains = "/api/dashboards/uid/ydb_cpu", .StatusLine = "HTTP/1.1 200 OK", .Body = MakeDashboardModelBody("label_values(ElapsedMicrosec{__workspace__=\"$workspace\",__bucket__=\"utils\",database=\"$database\"},database)")},
            {.UrlContains = "/api/dashboards/uid/storage", .StatusLine = "HTTP/1.1 200 OK", .Body = MakeDashboardModelBody("label_values(requestBytes{__workspace__=\"$workspace\",__bucket__=\"dsproxynode\",database=\"$database\"},storagePool)")},
            {.UrlContains = "__bucket__%3D%22utils%22", .StatusLine = "HTTP/1.1 200 OK", .Body = MakeProbeResponseBody("1")},
            {.UrlContains = "__bucket__%3D%22dsproxynode%22", .StatusLine = "HTTP/1.1 200 OK", .Body = MakeProbeResponseBody("0")},
        };
        auto httpProxyId = runtime.Register(new TGrafanaApiReplyActor(std::move(rules)));
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
            MakeUrlParameters("database=%2Froot%2Ftest"),
            httpProxyId,
            handle);

        UNIT_ASSERT_VALUES_EQUAL(response->Errors.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(response->Links.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Links[0].Title, "CPU");
    }

    Y_UNIT_TEST(BuildsProbeQueryFromFixture) {
        const NJson::TJsonValue fixture = ReadFixtureJson("ut/grafana_dashboard_probe_fixture.json");
        UNIT_ASSERT(fixture.Has("dashboard"));

        TCgiParameters queryParameters;
        queryParameters.InsertUnescaped("var-ds", "fc872d93-d988-491c-83cd-19249cb962b0");
        queryParameters.InsertUnescaped("var-workspace", "ydb-disks-ext");
        queryParameters.InsertUnescaped("var-database", "/testing-disks-ext/NBS");
        queryParameters.InsertUnescaped("var-storagePool", "pool-1");

        const auto probeGroups = NMVP::NSupportLinks::BuildGrafanaDashboardProbeGroups(
            fixture["dashboard"],
            queryParameters);

        UNIT_ASSERT_VALUES_EQUAL(probeGroups.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(probeGroups[0].DatasourceUid, "fc872d93-d988-491c-83cd-19249cb962b0");
        UNIT_ASSERT_VALUES_EQUAL(probeGroups[0].Buckets.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(probeGroups[0].Buckets[0], "dsproxynode");
        UNIT_ASSERT_VALUES_EQUAL(probeGroups[0].Buckets[1], "pdisks");
        UNIT_ASSERT_VALUES_EQUAL(probeGroups[0].Buckets[2], "utils");

        const TString expectedQuery =
            "(count(count_over_time(requestBytes{__workspace__=\"ydb-disks-ext\",__bucket__=\"dsproxynode\",database=\"/testing-disks-ext/NBS\",storagePool=\"pool-1\"}[1m])) or on() vector(0)) + "
            "(count(count_over_time(CompletionThreadCPU{__workspace__=\"ydb-disks-ext\",__bucket__=\"pdisks\"}[1m])) or on() vector(0)) + "
            "(count(count_over_time(ElapsedMicrosec{container=\"ydb-dynamic\",__workspace__=\"ydb-disks-ext\",__bucket__=\"utils\",database=\"/testing-disks-ext/NBS\"}[1m])) or on() vector(0))";
        UNIT_ASSERT_VALUES_EQUAL(probeGroups[0].Query, expectedQuery);
    }

    Y_UNIT_TEST(IgnoresUnusedHeaderVariablesWhenBuildingProbeQuery) {
        const NJson::TJsonValue fixture = ReadFixtureJson("ut/grafana_dashboard_probe_fixture.json");
        UNIT_ASSERT(fixture.Has("dashboard"));

        TCgiParameters baseQueryParameters;
        baseQueryParameters.InsertUnescaped("var-ds", "fc872d93-d988-491c-83cd-19249cb962b0");
        baseQueryParameters.InsertUnescaped("var-workspace", "ydb-disks-ext");
        baseQueryParameters.InsertUnescaped("var-database", "/testing-disks-ext/NBS");
        baseQueryParameters.InsertUnescaped("var-storagePool", "pool-1");

        TCgiParameters extendedQueryParameters = baseQueryParameters;
        extendedQueryParameters.InsertUnescaped("var-handleclass", "GetFast");
        extendedQueryParameters.InsertUnescaped("var-aggregation", "not_selected");
        extendedQueryParameters.InsertUnescaped("var-query1", "");
        extendedQueryParameters.InsertUnescaped("var-env", "[PROD] MAN");
        extendedQueryParameters.InsertUnescaped("var-unused", "42");

        const auto baseProbeGroups = NMVP::NSupportLinks::BuildGrafanaDashboardProbeGroups(
            fixture["dashboard"],
            baseQueryParameters);
        const auto extendedProbeGroups = NMVP::NSupportLinks::BuildGrafanaDashboardProbeGroups(
            fixture["dashboard"],
            extendedQueryParameters);

        UNIT_ASSERT_VALUES_EQUAL(baseProbeGroups.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(extendedProbeGroups.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(extendedProbeGroups[0].DatasourceUid, baseProbeGroups[0].DatasourceUid);
        UNIT_ASSERT_VALUES_EQUAL(extendedProbeGroups[0].Buckets, baseProbeGroups[0].Buckets);
        UNIT_ASSERT_VALUES_EQUAL(extendedProbeGroups[0].Query, baseProbeGroups[0].Query);
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
