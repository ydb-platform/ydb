#include <library/cpp/testing/unittest/registar.h>
#include <memory>

#include <ydb/mvp/meta/mvp.h>
#include <ydb/mvp/meta/support_links/source.h>
#include <ydb/mvp/meta/support_links/grafana_dashboard_source.h>

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

    Y_UNIT_TEST(BuildsResolvedGrafanaDashboardUrl) {
        TMvpGuard mvpGuard;
        NMVP::InstanceMVP->MetaSettings.GrafanaConfig = NMVP::TGrafanaSupportConfig{
            .Endpoint = "https://grafana.example.net",
        };

        NMVP::TSupportLinkEntry config;
        config.SetSource("grafana/dashboard");
        config.SetTitle("CPU");
        config.SetUrl("/d/cpu");
        NMVP::TGrafanaDashboardSource source(std::move(config), NMVP::InstanceMVP->MetaSettings);

        THashMap<TString, TString> clusterColumns;
        clusterColumns["k8s_namespace"] = "ydb-workspace";
        clusterColumns["datasource"] = "3f8a1e2c-6b7d-4c91-9a52-1d7f0e8b4a63";
        TVector<std::pair<TString, TString>> queryParams;
        queryParams.emplace_back("cluster", "ydb-global");
        queryParams.emplace_back("database", "root_test");
        const NActors::TActorId parent;
        const NActors::TActorId httpProxyId;

        auto result = source.Resolve(NMVP::ILinkSource::TResolveInput{
            .ClusterColumns = clusterColumns,
            .QueryParams = queryParams,
            .Parent = parent,
            .HttpProxyId = httpProxyId,
        });

        UNIT_ASSERT_VALUES_EQUAL(result.Links.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result.Links[0].Title, "CPU");
        UNIT_ASSERT_VALUES_EQUAL(
            result.Links[0].Url,
            "https://grafana.example.net/d/cpu?var-workspace=ydb-workspace&var-ds=3f8a1e2c-6b7d-4c91-9a52-1d7f0e8b4a63&var-cluster=ydb-global&var-database=root_test"
        );
        UNIT_ASSERT_VALUES_EQUAL(result.Errors.size(), 0);
    }

    Y_UNIT_TEST(ReturnsPartialUrlAndErrorWhenDatasourceMissing) {
        TMvpGuard mvpGuard;
        NMVP::InstanceMVP->MetaSettings.GrafanaConfig = NMVP::TGrafanaSupportConfig{
            .Endpoint = "https://grafana.example.net",
        };

        NMVP::TSupportLinkEntry config;
        config.SetSource("grafana/dashboard");
        config.SetTitle("CPU");
        config.SetUrl("/d/cpu");
        NMVP::TGrafanaDashboardSource source(std::move(config), NMVP::InstanceMVP->MetaSettings);

        THashMap<TString, TString> clusterColumns;
        clusterColumns["k8s_namespace"] = "ydb-workspace";
        TVector<std::pair<TString, TString>> queryParams;
        queryParams.emplace_back("cluster", "ydb-global");
        queryParams.emplace_back("database", "/root/test");
        const NActors::TActorId parent;
        const NActors::TActorId httpProxyId;

        auto result = source.Resolve(NMVP::ILinkSource::TResolveInput{
            .ClusterColumns = clusterColumns,
            .QueryParams = queryParams,
            .Parent = parent,
            .HttpProxyId = httpProxyId,
        });

        UNIT_ASSERT_VALUES_EQUAL(result.Links.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            result.Links[0].Url,
            "https://grafana.example.net/d/cpu?var-workspace=ydb-workspace&var-cluster=ydb-global&var-database=%2Froot%2Ftest"
        );
        UNIT_ASSERT_VALUES_EQUAL(result.Errors.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result.Errors[0].Source, "meta");
        UNIT_ASSERT(result.Errors[0].Message.Contains("datasource"));
    }

    Y_UNIT_TEST(EncodesQueryParamReservedCharacters) {
        TMvpGuard mvpGuard;
        NMVP::InstanceMVP->MetaSettings.GrafanaConfig = NMVP::TGrafanaSupportConfig{
            .Endpoint = "https://grafana.example.net",
        };

        NMVP::TSupportLinkEntry config;
        config.SetSource("grafana/dashboard");
        config.SetTitle("CPU");
        config.SetUrl("/d/cpu");
        NMVP::TGrafanaDashboardSource source(std::move(config), NMVP::InstanceMVP->MetaSettings);

        THashMap<TString, TString> clusterColumns;
        clusterColumns["k8s_namespace"] = "ydb-workspace";
        clusterColumns["datasource"] = "3f8a1e2c-6b7d-4c91-9a52-1d7f0e8b4a63";
        TVector<std::pair<TString, TString>> queryParams;
        queryParams.emplace_back("database", "root&x=y");
        const NActors::TActorId parent;
        const NActors::TActorId httpProxyId;

        auto result = source.Resolve(NMVP::ILinkSource::TResolveInput{
            .ClusterColumns = clusterColumns,
            .QueryParams = queryParams,
            .Parent = parent,
            .HttpProxyId = httpProxyId,
        });

        UNIT_ASSERT_VALUES_EQUAL(result.Links.size(), 1);
        UNIT_ASSERT(result.Links[0].Url.Contains("var-database=root%26x%3Dy"));
    }
}
