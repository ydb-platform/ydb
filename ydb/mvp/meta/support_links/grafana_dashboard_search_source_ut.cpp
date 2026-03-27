#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/mvp/core/appdata.h>
#include <ydb/mvp/core/mvp_test_runtime.h>
#include <ydb/mvp/core/mvp_tokens.h>
#include <ydb/mvp/meta/mvp.h>
#include <ydb/mvp/meta/support_links/events.h>
#include <ydb/mvp/meta/support_links/grafana_dashboard_search_source.h>

namespace {

class TTestActorRuntime : public TMvpTestRuntime {
public:
    TTestActorRuntime()
        : TMvpTestRuntime(1, false)
    {
        Initialize();
    }
};

NMVP::TMetaSettings MakeMetaSettings() {
    NMVP::TMetaSettings settings;
    settings.GrafanaEndpoint = "https://grafana.nebius.dev";
    return settings;
}

void ConfigureTokenator(TTestActorRuntime& runtime, const TString& tokenName, const TString& tokenValue) {
    NMvp::TTokensConfig tokensConfig;
    auto* staffToken = tokensConfig.MutableStaffApiUserTokenInfo();
    staffToken->SetName(tokenName);
    staffToken->SetToken(tokenValue);
    auto* tokenator = NMVP::TMvpTokenator::CreateTokenator(tokensConfig, NActors::TActorId());
    runtime.GetActorSystem(0)->AppData<NMVP::TMVPAppData>()->Tokenator = tokenator;
}

NHttp::TUrlParameters MakeUrlParameters(TStringBuf url) {
    return NHttp::TUrlParameters(url);
}

class TSearchApiRequestCheckActor : public NActors::TActor<TSearchApiRequestCheckActor> {
public:
    using TBase = NActors::TActor<TSearchApiRequestCheckActor>;

    TSearchApiRequestCheckActor()
        : TBase(&TSearchApiRequestCheckActor::StateWork)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpOutgoingRequest::TPtr event) {
        const auto request = event->Get()->Request;
        const TString url(request->URL);
        const bool hasTag = url.Contains("tag=ydb-common");
        const bool hasFolder = url.Contains("folderUIDs=team-folder");
        const bool hasExpectedAuth = request->Headers.find("Authorization: OAuth test-token") != TString::npos;
        const TString body = "[]";
        const TString statusLine = (url.StartsWith("/api/search?") && hasTag && hasFolder && hasExpectedAuth)
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
                << body
        );
        Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(request, response));
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpOutgoingRequest, Handle);
        }
    }
};

class TSearchApiForbiddenActor : public NActors::TActor<TSearchApiForbiddenActor> {
public:
    using TBase = NActors::TActor<TSearchApiForbiddenActor>;

    TSearchApiForbiddenActor()
        : TBase(&TSearchApiForbiddenActor::StateWork)
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
                << body
        );
        Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(request, response));
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpOutgoingRequest, Handle);
        }
    }
};

class TGrafanaMockServiceActor : public NActors::TActor<TGrafanaMockServiceActor> {
public:
    using TBase = NActors::TActor<TGrafanaMockServiceActor>;

    TGrafanaMockServiceActor()
        : TBase(&TGrafanaMockServiceActor::StateWork)
    {}

    static TStringBuf DashboardsYdbCommonJson() {
        return R"json([
    {
        "id": 176,
        "uid": "ydb_cpu",
        "orgId": 1,
        "title": "CPU",
        "uri": "db/cpu",
        "url": "/d/ydb_cpu/cpu",
        "slug": "",
        "type": "dash-db",
        "tags": [
            "ydb-common"
        ],
        "isStarred": false
    },
    {
        "id": 312,
        "uid": "ydb_dboverview",
        "orgId": 1,
        "title": "DB overview",
        "uri": "db/db-overview",
        "url": "/d/ydb_dboverview/db-overview",
        "slug": "",
        "type": "dash-db",
        "tags": [
            "ydb-common"
        ],
        "isStarred": false
    },
    {
        "id": 5648,
        "uid": "bevkmtk3xlclca",
        "orgId": 1,
        "title": "Interconnect",
        "uri": "db/interconnect",
        "url": "/d/bevkmtk3xlclca/interconnect",
        "slug": "",
        "type": "dash-db",
        "tags": [
            "ydb-common"
        ],
        "isStarred": false
    }
])json";
    }

    static TString ExtractQueryParam(TStringBuf url, TStringBuf name) {
        const size_t queryPos = url.find('?');
        if (queryPos == TStringBuf::npos) {
            return {};
        }

        TStringBuf query = url.SubStr(queryPos + 1);
        size_t pos = 0;
        while (pos <= query.size()) {
            size_t ampPos = query.find('&', pos);
            TStringBuf part = ampPos == TStringBuf::npos ? query.SubStr(pos) : query.SubStr(pos, ampPos - pos);
            const size_t eqPos = part.find('=');
            if (eqPos != TStringBuf::npos) {
                if (part.SubStr(0, eqPos) == name) {
                    return TString(part.SubStr(eqPos + 1));
                }
            }
            if (ampPos == TStringBuf::npos) {
                break;
            }
            pos = ampPos + 1;
        }
        return {};
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpOutgoingRequest::TPtr event) {
        const auto request = event->Get()->Request;
        const TString tag = ExtractQueryParam(request->URL, "tag");
        const TString responseBody = (tag == "ydb-common") ? TString(DashboardsYdbCommonJson()) : "[]";

        NHttp::THttpIncomingResponsePtr response = new NHttp::THttpIncomingResponse(request);
        EatWholeString(
            response,
            TStringBuilder()
                << "HTTP/1.1 200 OK\r\n"
                << "Connection: close\r\n"
                << "Content-Type: application/json; charset=utf-8\r\n"
                << "Content-Length: " << responseBody.size() << "\r\n"
                << "\r\n"
                << responseBody
        );

        Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(request, response));
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpOutgoingRequest, Handle);
        }
    }
};

} // namespace

Y_UNIT_TEST_SUITE(SupportLinksGrafanaDashboardSearchSource) {
    Y_UNIT_TEST(ReturnsErrorWhenMetaDatabaseTokenNameIsNotConfigured) {
        TTestActorRuntime runtime;
        TAutoPtr<NActors::IEventHandle> handle;
        NMVP::TMVP::MetaDatabaseTokenName.clear();

        auto parent = runtime.AllocateEdgeActor();
        auto anyHttpProxy = runtime.AllocateEdgeActor();

        NMVP::NSupportLinks::TLinkResolveContext context;
        context.Parent = parent;
        context.HttpProxyId = anyHttpProxy;
        context.SourceName = "grafana/dashboard/search";
        context.LinkConfig.SetSource("grafana/dashboard/search");
        const auto settings = MakeMetaSettings();

        runtime.Register(NMVP::NSupportLinks::BuildGrafanaDashboardSearchResolver(std::move(context), settings));

        auto* response = runtime.GrabEdgeEvent<NMVP::NSupportLinks::TEvPrivate::TEvSourceResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(response->Links.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(response->Errors.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Errors[0].Source, "grafana/dashboard/search");
        UNIT_ASSERT_VALUES_EQUAL(
            response->Errors[0].Message,
            "meta.meta_database_token_name is required for source=grafana/dashboard/search"
        );
    }

    Y_UNIT_TEST(LoadsDashboardsFromMockServiceByTag) {
        TTestActorRuntime runtime;
        TAutoPtr<NActors::IEventHandle> handle;
        NMVP::TMVP::MetaDatabaseTokenName = "grafana-token";
        ConfigureTokenator(runtime, NMVP::TMVP::MetaDatabaseTokenName, "test-token");

        auto parent = runtime.AllocateEdgeActor();
        auto grafanaMockProxy = runtime.Register(new TGrafanaMockServiceActor());

        NMVP::NSupportLinks::TLinkResolveContext context;
        context.Parent = parent;
        context.HttpProxyId = grafanaMockProxy;
        context.SourceName = "grafana/dashboard/search";
        context.LinkConfig.SetSource("grafana/dashboard/search");
        context.LinkConfig.SetUrl("/api/search?limit=100&type=dash-db");
        context.LinkConfig.SetTag("ydb-common");
        context.ClusterColumns["k8s_namespace"] = "ydb-workspace";
        context.ClusterColumns["datasource"] = "3f8a1e2c-6b7d-4c91-9a52-1d7f0e8b4a63";
        context.UrlParameters = MakeUrlParameters("/?cluster=ydb-global&database=%2Froot%2Ftest");
        const auto settings = MakeMetaSettings();

        runtime.Register(NMVP::NSupportLinks::BuildGrafanaDashboardSearchResolver(std::move(context), settings));

        auto* response = runtime.GrabEdgeEvent<NMVP::NSupportLinks::TEvPrivate::TEvSourceResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(response->Links.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(response->Links[0].Title, "CPU");
        UNIT_ASSERT_VALUES_EQUAL(
            response->Links[0].Url,
            "https://grafana.nebius.dev/d/ydb_cpu/cpu?var-workspace=ydb-workspace&var-ds=3f8a1e2c-6b7d-4c91-9a52-1d7f0e8b4a63&var-cluster=ydb-global&var-database=/root/test"
        );
        UNIT_ASSERT_VALUES_EQUAL(response->Links[2].Title, "Interconnect");
        UNIT_ASSERT_VALUES_EQUAL(response->Errors.size(), 0);
    }

    Y_UNIT_TEST(ReturnsSingleMissingColumnErrorForMultipleDashboards) {
        TTestActorRuntime runtime;
        TAutoPtr<NActors::IEventHandle> handle;
        NMVP::TMVP::MetaDatabaseTokenName = "grafana-token";
        ConfigureTokenator(runtime, NMVP::TMVP::MetaDatabaseTokenName, "test-token");

        auto parent = runtime.AllocateEdgeActor();
        auto grafanaMockProxy = runtime.Register(new TGrafanaMockServiceActor());

        NMVP::NSupportLinks::TLinkResolveContext context;
        context.Parent = parent;
        context.HttpProxyId = grafanaMockProxy;
        context.SourceName = "grafana/dashboard/search";
        context.LinkConfig.SetSource("grafana/dashboard/search");
        context.LinkConfig.SetUrl("/api/search?limit=100&type=dash-db");
        context.LinkConfig.SetTag("ydb-common");
        context.ClusterColumns["datasource"] = "3f8a1e2c-6b7d-4c91-9a52-1d7f0e8b4a63";
        context.UrlParameters = MakeUrlParameters("/?cluster=ydb-global");
        const auto settings = MakeMetaSettings();

        runtime.Register(NMVP::NSupportLinks::BuildGrafanaDashboardSearchResolver(std::move(context), settings));

        auto* response = runtime.GrabEdgeEvent<NMVP::NSupportLinks::TEvPrivate::TEvSourceResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(response->Links.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(response->Errors.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Errors[0].Source, "meta");
        UNIT_ASSERT(response->Errors[0].Message.Contains("k8s_namespace"));
        UNIT_ASSERT(response->Errors[0].Message.Contains("is missing or empty"));
    }

    Y_UNIT_TEST(UsesDefaultSearchPathAndForwardsTagAndFolder) {
        TTestActorRuntime runtime;
        TAutoPtr<NActors::IEventHandle> handle;
        NMVP::TMVP::MetaDatabaseTokenName = "grafana-token";
        ConfigureTokenator(runtime, NMVP::TMVP::MetaDatabaseTokenName, "test-token");

        auto parent = runtime.AllocateEdgeActor();
        auto checkProxy = runtime.Register(new TSearchApiRequestCheckActor());

        NMVP::NSupportLinks::TLinkResolveContext context;
        context.Parent = parent;
        context.HttpProxyId = checkProxy;
        context.SourceName = "grafana/dashboard/search";
        context.LinkConfig.SetSource("grafana/dashboard/search");
        context.LinkConfig.SetTag("ydb-common");
        context.LinkConfig.SetFolder("team-folder");
        context.ClusterColumns["k8s_namespace"] = "ydb-workspace";
        context.ClusterColumns["datasource"] = "3f8a1e2c-6b7d-4c91-9a52-1d7f0e8b4a63";
        const auto settings = MakeMetaSettings();

        runtime.Register(NMVP::NSupportLinks::BuildGrafanaDashboardSearchResolver(std::move(context), settings));

        auto* response = runtime.GrabEdgeEvent<NMVP::NSupportLinks::TEvPrivate::TEvSourceResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(response->Links.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(response->Errors.size(), 0);
    }

    Y_UNIT_TEST(ReturnsStructuredErrorWhenGrafanaSearchReturnsForbidden) {
        TTestActorRuntime runtime;
        TAutoPtr<NActors::IEventHandle> handle;
        NMVP::TMVP::MetaDatabaseTokenName = "grafana-token";
        ConfigureTokenator(runtime, NMVP::TMVP::MetaDatabaseTokenName, "test-token");

        auto parent = runtime.AllocateEdgeActor();
        auto forbiddenProxy = runtime.Register(new TSearchApiForbiddenActor());

        NMVP::NSupportLinks::TLinkResolveContext context;
        context.Parent = parent;
        context.HttpProxyId = forbiddenProxy;
        context.SourceName = "grafana/dashboard/search";
        context.LinkConfig.SetSource("grafana/dashboard/search");
        const auto settings = MakeMetaSettings();

        runtime.Register(NMVP::NSupportLinks::BuildGrafanaDashboardSearchResolver(std::move(context), settings));

        auto* response = runtime.GrabEdgeEvent<NMVP::NSupportLinks::TEvPrivate::TEvSourceResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(response->Links.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(response->Errors.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Errors[0].Source, "grafana/dashboard/search");
        UNIT_ASSERT_VALUES_EQUAL(*response->Errors[0].Status, 403);
        UNIT_ASSERT_VALUES_EQUAL(response->Errors[0].Reason, "Forbidden");
        UNIT_ASSERT_VALUES_EQUAL(response->Errors[0].Message, "403 Forbidden");
    }
}
