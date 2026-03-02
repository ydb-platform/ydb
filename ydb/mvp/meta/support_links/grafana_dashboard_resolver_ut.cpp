#include <library/cpp/testing/unittest/registar.h>
#include <memory>

#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/mvp/meta/mvp.h>
#include <ydb/mvp/meta/support_links/events.h>
#include <ydb/mvp/meta/support_links/grafana_dashboard_resolver.h>

Y_UNIT_TEST_SUITE(SupportLinksGrafanaDashboardSource) {
    class TMvpGuard {
    public:
        TMvpGuard() {
            const char* argv[] = {"source_grafana_dashboard_source_ut"};
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

    Y_UNIT_TEST(BuildsResolvedGrafanaDashboardUrl) {
        TMvpGuard mvpGuard;
        TTestActorRuntime runtime;
        TAutoPtr<NActors::IEventHandle> handle;

        auto parent = runtime.AllocateEdgeActor();
        NMVP::InstanceMVP->GrafanaSupportConfig = NMVP::TGrafanaSupportConfig{
            .Endpoint = "https://grafana.example.net",
            .WorkspaceColumn = "workspace",
            .DatasourceColumn = "grafana_ds",
        };

        NMVP::NSupportLinks::TLinkResolveContext context;
        context.Place = 1;
        context.Parent = parent;
        context.SourceName = "grafana/dashboard";
        context.LinkConfig.Source = "grafana/dashboard";
        context.LinkConfig.Title = "CPU";
        context.LinkConfig.Url = "/d/cpu";
        context.ClusterColumns["workspace"] = "ws";
        context.ClusterColumns["grafana_ds"] = "ds";
        context.QueryParams.emplace_back("cluster", "testing-global");
        context.QueryParams.emplace_back("database", "root_test");

        runtime.Register(NMVP::NSupportLinks::BuildGrafanaDashboardResolver(std::move(context)));

        auto* response = runtime.GrabEdgeEvent<NMVP::NSupportLinks::TEvPrivate::TEvSourceResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(response->Links.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Links[0].Title, "CPU");
        UNIT_ASSERT_VALUES_EQUAL(
            response->Links[0].Url,
            "https://grafana.example.net/d/cpu?var-workspace=ws&var-ds=ds&var-cluster=testing-global&var-database=root_test"
        );
        UNIT_ASSERT_VALUES_EQUAL(response->Errors.size(), 0);
    }

    Y_UNIT_TEST(ReturnsPartialUrlAndErrorWhenDatasourceMissing) {
        TMvpGuard mvpGuard;
        TTestActorRuntime runtime;
        TAutoPtr<NActors::IEventHandle> handle;

        auto parent = runtime.AllocateEdgeActor();
        NMVP::InstanceMVP->GrafanaSupportConfig = NMVP::TGrafanaSupportConfig{
            .Endpoint = "https://grafana.example.net",
            .WorkspaceColumn = "workspace",
            .DatasourceColumn = "grafana_ds",
        };

        NMVP::NSupportLinks::TLinkResolveContext context;
        context.Parent = parent;
        context.SourceName = "grafana/dashboard";
        context.LinkConfig.Source = "grafana/dashboard";
        context.LinkConfig.Title = "CPU";
        context.LinkConfig.Url = "/d/cpu";
        context.ClusterColumns["workspace"] = "ws";
        context.QueryParams.emplace_back("cluster", "testing-global");
        context.QueryParams.emplace_back("database", "/root/test");

        runtime.Register(NMVP::NSupportLinks::BuildGrafanaDashboardResolver(std::move(context)));

        auto* response = runtime.GrabEdgeEvent<NMVP::NSupportLinks::TEvPrivate::TEvSourceResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(response->Links.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            response->Links[0].Url,
            "https://grafana.example.net/d/cpu?var-workspace=ws&var-cluster=testing-global&var-database=%2Froot%2Ftest"
        );
        UNIT_ASSERT_VALUES_EQUAL(response->Errors.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Errors[0].Source, "meta");
        UNIT_ASSERT(response->Errors[0].Message.Contains("grafana_ds"));
    }

    Y_UNIT_TEST(EncodesQueryParamReservedCharacters) {
        TMvpGuard mvpGuard;
        TTestActorRuntime runtime;
        TAutoPtr<NActors::IEventHandle> handle;

        auto parent = runtime.AllocateEdgeActor();
        NMVP::InstanceMVP->GrafanaSupportConfig = NMVP::TGrafanaSupportConfig{
            .Endpoint = "https://grafana.example.net",
            .WorkspaceColumn = "workspace",
            .DatasourceColumn = "grafana_ds",
        };

        NMVP::NSupportLinks::TLinkResolveContext context;
        context.Parent = parent;
        context.SourceName = "grafana/dashboard";
        context.LinkConfig.Source = "grafana/dashboard";
        context.LinkConfig.Title = "CPU";
        context.LinkConfig.Url = "/d/cpu";
        context.ClusterColumns["workspace"] = "ws";
        context.ClusterColumns["grafana_ds"] = "ds";
        context.QueryParams.emplace_back("database", "root&x=y");

        runtime.Register(NMVP::NSupportLinks::BuildGrafanaDashboardResolver(std::move(context)));

        auto* response = runtime.GrabEdgeEvent<NMVP::NSupportLinks::TEvPrivate::TEvSourceResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(response->Links.size(), 1);
        UNIT_ASSERT(response->Links[0].Url.Contains("var-database=root%26x%3Dy"));
    }
}
