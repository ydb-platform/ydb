#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/actors/http/http.h>
#include <ydb/mvp/meta/support_links/grafana_dashboard_source.h>

namespace {

NMVP::TMetaSettings MakeMetaSettings() {
    NMVP::TMetaSettings settings;
    settings.GrafanaEndpoint = "https://grafana.example.net";
    return settings;
}

NHttp::TUrlParameters MakeUrlParameters(TStringBuf url) {
    return NHttp::TUrlParameters(url);
}

} // namespace

Y_UNIT_TEST_SUITE(SupportLinksGrafanaDashboardSource) {
    Y_UNIT_TEST(BuildsResolvedGrafanaDashboardUrl) {
        NMVP::TSupportLinkEntryConfig config;
        config.SetSource("grafana/dashboard");
        config.SetTitle("CPU");
        config.SetUrl("/d/cpu");
        const auto settings = MakeMetaSettings();
        NMVP::TGrafanaDashboardSource source(std::move(config), settings);

        THashMap<TString, TString> clusterColumns;
        clusterColumns["k8s_namespace"] = "ydb-workspace";
        clusterColumns["datasource"] = "3f8a1e2c-6b7d-4c91-9a52-1d7f0e8b4a63";
        auto urlParameters = MakeUrlParameters("/?cluster=ydb-global&database=root_test");
        const NActors::TActorId parent;
        const NActors::TActorId httpProxyId;

        auto result = source.Resolve(NMVP::ILinkSource::TResolveInput{
            .ClusterColumns = clusterColumns,
            .UrlParameters = urlParameters,
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
        NMVP::TSupportLinkEntryConfig config;
        config.SetSource("grafana/dashboard");
        config.SetTitle("CPU");
        config.SetUrl("/d/cpu");
        const auto settings = MakeMetaSettings();
        NMVP::TGrafanaDashboardSource source(std::move(config), settings);

        THashMap<TString, TString> clusterColumns;
        clusterColumns["k8s_namespace"] = "ydb-workspace";
        auto urlParameters = MakeUrlParameters("/?cluster=ydb-global&database=%2Froot%2Ftest");
        const NActors::TActorId parent;
        const NActors::TActorId httpProxyId;

        auto result = source.Resolve(NMVP::ILinkSource::TResolveInput{
            .ClusterColumns = clusterColumns,
            .UrlParameters = urlParameters,
            .Parent = parent,
            .HttpProxyId = httpProxyId,
        });

        UNIT_ASSERT_VALUES_EQUAL(result.Links.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            result.Links[0].Url,
            "https://grafana.example.net/d/cpu?var-workspace=ydb-workspace&var-cluster=ydb-global&var-database=/root/test"
        );
        UNIT_ASSERT_VALUES_EQUAL(result.Errors.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result.Errors[0].Source, "meta");
        UNIT_ASSERT(result.Errors[0].Message.Contains("datasource"));
    }

    Y_UNIT_TEST(EncodesQueryParamReservedCharacters) {
        NMVP::TSupportLinkEntryConfig config;
        config.SetSource("grafana/dashboard");
        config.SetTitle("CPU");
        config.SetUrl("/d/cpu");
        const auto settings = MakeMetaSettings();
        NMVP::TGrafanaDashboardSource source(std::move(config), settings);

        THashMap<TString, TString> clusterColumns;
        clusterColumns["k8s_namespace"] = "ydb-workspace";
        clusterColumns["datasource"] = "3f8a1e2c-6b7d-4c91-9a52-1d7f0e8b4a63";
        auto urlParameters = MakeUrlParameters("/?database=root%26x%3Dy");
        const NActors::TActorId parent;
        const NActors::TActorId httpProxyId;

        auto result = source.Resolve(NMVP::ILinkSource::TResolveInput{
            .ClusterColumns = clusterColumns,
            .UrlParameters = urlParameters,
            .Parent = parent,
            .HttpProxyId = httpProxyId,
        });

        UNIT_ASSERT_VALUES_EQUAL(result.Links.size(), 1);
        UNIT_ASSERT(result.Links[0].Url.Contains("var-database=root%26x%3Dy"));
    }
}
