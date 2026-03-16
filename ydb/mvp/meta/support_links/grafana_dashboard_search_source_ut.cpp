#include <library/cpp/testing/unittest/registar.h>
#include <memory>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/mvp/core/mvp_test_runtime.h>
#include <ydb/mvp/meta/mvp.h>
#include <ydb/mvp/meta/support_links/events.h>
#include <ydb/mvp/meta/support_links/grafana_dashboard_search_source.h>

Y_UNIT_TEST_SUITE(SupportLinksGrafanaDashboardSearchSource) {
    class TMvpGuard {
    public:
        TMvpGuard() {
            const char* argv[] = {"source_grafana_dashboard_search_source_ut"};
            Mvp = std::make_unique<NMVP::TMVP>(1, argv);
        }

    private:
        std::unique_ptr<NMVP::TMVP> Mvp;
    };

    class TTestActorRuntime : public NActors::TTestActorRuntimeBase {
    public:
        TTestActorRuntime() {
            Initialize();
        }
    };

    void ConfigureGrafanaSecret(const TString& secretName, const TString& secretValue) {
        NMVP::TMVP::TokensConfig.ClearSecretInfo();
        auto* secret = NMVP::TMVP::TokensConfig.MutableSecretInfo()->Add();
        secret->SetName(secretName);
        secret->SetSecret(secretValue);
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
            const TString body = "[]";
            const TString statusLine = (url.StartsWith("/api/search?") && hasTag && hasFolder)
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
                TStringBuf part = ampPos == TStringBuf::npos
                    ? query.SubStr(pos)
                    : query.SubStr(pos, ampPos - pos);
                const size_t eqPos = part.find('=');
                if (eqPos != TStringBuf::npos) {
                    TStringBuf key = part.SubStr(0, eqPos);
                    TStringBuf value = part.SubStr(eqPos + 1);
                    if (key == name) {
                        return TString(value);
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

    Y_UNIT_TEST(ReturnsErrorWhenMetaDatabaseTokenNameIsNotConfigured) {
        TMvpGuard mvpGuard;
        TTestActorRuntime runtime;
        TAutoPtr<NActors::IEventHandle> handle;

        auto parent = runtime.AllocateEdgeActor();
        auto anyHttpProxy = runtime.AllocateEdgeActor();

        NMVP::NSupportLinks::TLinkResolveContext context;
        context.Parent = parent;
        context.HttpProxyId = anyHttpProxy;
        context.SourceName = "grafana/dashboard/search";
        context.LinkConfig.SetSource("grafana/dashboard/search");
        NMVP::InstanceMVP->MetaSettings.GrafanaConfig = NMVP::TGrafanaSupportConfig{
            .Endpoint = "https://grafana.example.net",
        };

        runtime.Register(NMVP::NSupportLinks::BuildGrafanaDashboardSearchResolver(std::move(context), NMVP::InstanceMVP->MetaSettings));

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
        TMvpGuard mvpGuard;
        TTestActorRuntime runtime;
        TAutoPtr<NActors::IEventHandle> handle;
        ConfigureGrafanaSecret("grafana-secret", "test-token");

        auto parent = runtime.AllocateEdgeActor();
        auto grafanaMockProxy = runtime.Register(new TGrafanaMockServiceActor());

        NMVP::NSupportLinks::TLinkResolveContext context;
        context.Parent = parent;
        context.HttpProxyId = grafanaMockProxy;
        context.SourceName = "grafana/dashboard/search";
        context.LinkConfig.SetSource("grafana/dashboard/search");
        context.LinkConfig.SetUrl("/api/search?limit=100&type=dash-db");
        context.LinkConfig.SetTag("ydb-common");
        NMVP::InstanceMVP->MetaSettings.GrafanaConfig = NMVP::TGrafanaSupportConfig{
            .Endpoint = "https://grafana.nebius.dev",
        };
        context.ClusterColumns["k8s_namespace"] = "ydb-workspace";
        context.ClusterColumns["datasource"] = "3f8a1e2c-6b7d-4c91-9a52-1d7f0e8b4a63";
        context.QueryParams.emplace_back("cluster", "ydb-global");
        context.QueryParams.emplace_back("database", "/root/test");

        runtime.Register(NMVP::NSupportLinks::BuildGrafanaDashboardSearchResolver(std::move(context), NMVP::InstanceMVP->MetaSettings));

        auto* response = runtime.GrabEdgeEvent<NMVP::NSupportLinks::TEvPrivate::TEvSourceResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(response->Links.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(response->Links[0].Title, "CPU");
        UNIT_ASSERT_VALUES_EQUAL(
            response->Links[0].Url,
            "https://grafana.nebius.dev/d/ydb_cpu/cpu?var-workspace=ydb-workspace&var-ds=3f8a1e2c-6b7d-4c91-9a52-1d7f0e8b4a63&var-cluster=ydb-global&var-database=%2Froot%2Ftest"
        );
        UNIT_ASSERT_VALUES_EQUAL(response->Links[2].Title, "Interconnect");
        UNIT_ASSERT_VALUES_EQUAL(response->Errors.size(), 0);
    }

    Y_UNIT_TEST(ReturnsSingleMissingColumnErrorForMultipleDashboards) {
        TMvpGuard mvpGuard;
        TTestActorRuntime runtime;
        TAutoPtr<NActors::IEventHandle> handle;
        ConfigureGrafanaSecret("grafana-secret", "test-token");

        auto parent = runtime.AllocateEdgeActor();
        auto grafanaMockProxy = runtime.Register(new TGrafanaMockServiceActor());

        NMVP::NSupportLinks::TLinkResolveContext context;
        context.Parent = parent;
        context.HttpProxyId = grafanaMockProxy;
        context.SourceName = "grafana/dashboard/search";
        context.LinkConfig.SetSource("grafana/dashboard/search");
        context.LinkConfig.SetUrl("/api/search?limit=100&type=dash-db");
        context.LinkConfig.SetTag("ydb-common");
        NMVP::InstanceMVP->MetaSettings.GrafanaConfig = NMVP::TGrafanaSupportConfig{
            .Endpoint = "https://grafana.nebius.dev",
        };
        context.ClusterColumns["datasource"] = "3f8a1e2c-6b7d-4c91-9a52-1d7f0e8b4a63";
        context.QueryParams.emplace_back("cluster", "ydb-global");

        runtime.Register(NMVP::NSupportLinks::BuildGrafanaDashboardSearchResolver(std::move(context), NMVP::InstanceMVP->MetaSettings));

        auto* response = runtime.GrabEdgeEvent<NMVP::NSupportLinks::TEvPrivate::TEvSourceResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(response->Links.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(response->Errors.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Errors[0].Source, "meta");
        UNIT_ASSERT(response->Errors[0].Message.Contains("k8s_namespace"));
        UNIT_ASSERT(response->Errors[0].Message.Contains("is missing or empty"));
    }

    Y_UNIT_TEST(UsesDefaultSearchPathAndForwardsTagAndFolder) {
        TMvpGuard mvpGuard;
        TTestActorRuntime runtime;
        TAutoPtr<NActors::IEventHandle> handle;
        ConfigureGrafanaSecret("grafana-secret", "test-token");

        auto parent = runtime.AllocateEdgeActor();
        auto checkProxy = runtime.Register(new TSearchApiRequestCheckActor());

        NMVP::NSupportLinks::TLinkResolveContext context;
        context.Parent = parent;
        context.HttpProxyId = checkProxy;
        context.SourceName = "grafana/dashboard/search";
        context.LinkConfig.SetSource("grafana/dashboard/search");
        context.LinkConfig.SetTag("ydb-common");
        context.LinkConfig.SetFolder("team-folder");
        NMVP::InstanceMVP->MetaSettings.GrafanaConfig = NMVP::TGrafanaSupportConfig{
            .Endpoint = "https://grafana.nebius.dev",
        };
        context.ClusterColumns["k8s_namespace"] = "ydb-workspace";
        context.ClusterColumns["datasource"] = "3f8a1e2c-6b7d-4c91-9a52-1d7f0e8b4a63";

        runtime.Register(NMVP::NSupportLinks::BuildGrafanaDashboardSearchResolver(std::move(context), NMVP::InstanceMVP->MetaSettings));

        auto* response = runtime.GrabEdgeEvent<NMVP::NSupportLinks::TEvPrivate::TEvSourceResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(response->Links.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(response->Errors.size(), 0);
    }

    Y_UNIT_TEST(ReturnsStructuredErrorWhenGrafanaSearchReturnsForbidden) {
        TMvpGuard mvpGuard;
        TTestActorRuntime runtime;
        TAutoPtr<NActors::IEventHandle> handle;
        ConfigureGrafanaSecret("grafana-secret", "test-token");

        auto parent = runtime.AllocateEdgeActor();
        auto forbiddenProxy = runtime.Register(new TSearchApiForbiddenActor());

        NMVP::NSupportLinks::TLinkResolveContext context;
        context.Parent = parent;
        context.HttpProxyId = forbiddenProxy;
        context.SourceName = "grafana/dashboard/search";
        context.LinkConfig.SetSource("grafana/dashboard/search");
        NMVP::InstanceMVP->MetaSettings.GrafanaConfig = NMVP::TGrafanaSupportConfig{
            .Endpoint = "https://grafana.nebius.dev",
        };

        runtime.Register(NMVP::NSupportLinks::BuildGrafanaDashboardSearchResolver(std::move(context), NMVP::InstanceMVP->MetaSettings));

        auto* response = runtime.GrabEdgeEvent<NMVP::NSupportLinks::TEvPrivate::TEvSourceResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(response->Links.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(response->Errors.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Errors[0].Source, "grafana/dashboard/search");
        UNIT_ASSERT_VALUES_EQUAL(*response->Errors[0].Status, 403);
        UNIT_ASSERT_VALUES_EQUAL(response->Errors[0].Reason, "Forbidden");
        UNIT_ASSERT_VALUES_EQUAL(response->Errors[0].Message, "403 Forbidden");
    }
}
