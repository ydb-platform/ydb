#include <library/cpp/testing/unittest/registar.h>
#include <memory>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/mvp/core/mvp_test_runtime.h>
#include <ydb/mvp/meta/mvp.h>
#include <ydb/mvp/meta/support_links/events.h>
#include <ydb/mvp/meta/support_links/ut/grafana_mock.h>
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

    Y_UNIT_TEST(ReturnsErrorWhenSecretNameIsNotConfigured) {
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
        UNIT_ASSERT_VALUES_EQUAL(response->Errors[0].Message, "grafana.secret_name is required");
    }

    Y_UNIT_TEST(LoadsDashboardsFromMockServiceByTag) {
        TMvpGuard mvpGuard;
        TTestActorRuntime runtime;
        TAutoPtr<NActors::IEventHandle> handle;
        ConfigureGrafanaSecret("grafana-secret", "test-token");

        auto parent = runtime.AllocateEdgeActor();
        auto grafanaMockProxy = runtime.Register(new NMVP::NMeta::NUT::TGrafanaMockServiceActor());

        NMVP::NSupportLinks::TLinkResolveContext context;
        context.Parent = parent;
        context.HttpProxyId = grafanaMockProxy;
        context.SourceName = "grafana/dashboard/search";
        context.LinkConfig.SetSource("grafana/dashboard/search");
        context.LinkConfig.SetUrl("/api/search?limit=100&type=dash-db");
        context.LinkConfig.SetTag("ydb-common");
        NMVP::InstanceMVP->MetaSettings.GrafanaConfig = NMVP::TGrafanaSupportConfig{
            .Endpoint = "https://grafana.nebius.dev",
            .SecretName = "grafana-secret",
        };
        context.ClusterColumns["k8s_namespace"] = "ws";
        context.ClusterColumns["datasource"] = "ds";
        context.QueryParams.emplace_back("cluster", "testing-global");
        context.QueryParams.emplace_back("database", "/root/test");

        runtime.Register(NMVP::NSupportLinks::BuildGrafanaDashboardSearchResolver(std::move(context), NMVP::InstanceMVP->MetaSettings));

        auto* response = runtime.GrabEdgeEvent<NMVP::NSupportLinks::TEvPrivate::TEvSourceResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(response->Links.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(response->Links[0].Title, "CPU");
        UNIT_ASSERT_VALUES_EQUAL(
            response->Links[0].Url,
            "https://grafana.nebius.dev/d/ydb_cpu/cpu?var-workspace=ws&var-ds=ds&var-cluster=testing-global&var-database=%2Froot%2Ftest"
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
        auto grafanaMockProxy = runtime.Register(new NMVP::NMeta::NUT::TGrafanaMockServiceActor());

        NMVP::NSupportLinks::TLinkResolveContext context;
        context.Parent = parent;
        context.HttpProxyId = grafanaMockProxy;
        context.SourceName = "grafana/dashboard/search";
        context.LinkConfig.SetSource("grafana/dashboard/search");
        context.LinkConfig.SetUrl("/api/search?limit=100&type=dash-db");
        context.LinkConfig.SetTag("ydb-common");
        NMVP::InstanceMVP->MetaSettings.GrafanaConfig = NMVP::TGrafanaSupportConfig{
            .Endpoint = "https://grafana.nebius.dev",
            .SecretName = "grafana-secret",
        };
        context.ClusterColumns["datasource"] = "ds";
        context.QueryParams.emplace_back("cluster", "testing-global");

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
            .SecretName = "grafana-secret",
        };
        context.ClusterColumns["k8s_namespace"] = "ws";
        context.ClusterColumns["datasource"] = "ds";

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
            .SecretName = "grafana-secret",
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
