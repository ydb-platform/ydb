#include <ydb/mvp/meta/mvp.h>
#include <ydb/mvp/meta/support_links/source.h>
#include <ydb/mvp/core/utils.h>
#include <ydb/mvp/core/mvp_test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/yexception.h>
#include <yaml-cpp/yaml.h>

namespace {

NMvp::NMeta::TMetaAppConfig ParseConfig(const TString& yaml) {
    YAML::Node node = YAML::Load(yaml);
    NMvp::NMeta::TMetaAppConfig appConfig;
    NMVP::MergeYamlNodeToProto(node, appConfig);
    return appConfig;
}

NHttp::THttpIncomingRequestPtr BuildHttpRequest(TStringBuf url, TStringBuf method = "GET") {
    NHttp::THttpIncomingRequestPtr request = new NHttp::THttpIncomingRequest();
    EatWholeString(request, TStringBuilder() << method << " " << url << " HTTP/1.1\r\nHost: localhost\r\n\r\n");
    UNIT_ASSERT_EQUAL(request->Stage, NHttp::THttpIncomingRequest::EParseStage::Done);
    return request;
}

} // namespace

Y_UNIT_TEST_SUITE(MetaConfigurationValidation) {
    static NMVP::TMVP MakeTestMvp() {
        const char* argv[] = {"mvp_test"};
        return NMVP::TMVP(1, argv);
    }

    Y_UNIT_TEST(MetaBlockWithoutApiEndpointThrows) {
        auto mvp = MakeTestMvp();
        const TString yaml = R"(
generic:
  access_service_type: "yandex_v2"
meta:
  meta_database: "/Root/meta"
)";
        const NMvp::NMeta::TMetaAppConfig appConfig = ParseConfig(yaml);
        UNIT_ASSERT_EXCEPTION_CONTAINS(mvp.TryGetMetaOptionsFromConfig(appConfig), yexception, "meta.meta_api_endpoint must be specified");
    }

    Y_UNIT_TEST(MetaBlockWithoutDatabaseThrows) {
        auto mvp = MakeTestMvp();
        const TString yaml = R"(
generic:
  access_service_type: "yandex_v2"
meta:
  meta_api_endpoint: "grpc://meta.ydb.example.net:2135"
)";
        const NMvp::NMeta::TMetaAppConfig appConfig = ParseConfig(yaml);
        UNIT_ASSERT_EXCEPTION_CONTAINS(mvp.TryGetMetaOptionsFromConfig(appConfig), yexception, "meta.meta_database must be specified");
    }

    Y_UNIT_TEST(NebiusWithoutApiEndpointThrows) {
        auto mvp = MakeTestMvp();
        const TString yaml = R"(
generic:
  access_service_type: "nebius_v1"
meta:
  meta_database: "/Root/meta"
)";
        const NMvp::NMeta::TMetaAppConfig appConfig = ParseConfig(yaml);
        UNIT_ASSERT_EXCEPTION_CONTAINS(mvp.TryGetMetaOptionsFromConfig(appConfig), yexception, "meta.meta_api_endpoint must be specified");
    }

    Y_UNIT_TEST(NebiusWithoutDatabaseThrows) {
        auto mvp = MakeTestMvp();
        const TString yaml = R"(
generic:
  access_service_type: "nebius_v1"
meta:
  meta_api_endpoint: "grpc://meta.ydb.example.net:2135"
)";
        const NMvp::NMeta::TMetaAppConfig appConfig = ParseConfig(yaml);
        UNIT_ASSERT_EXCEPTION_CONTAINS(mvp.TryGetMetaOptionsFromConfig(appConfig), yexception, "meta.meta_database must be specified");
    }

    Y_UNIT_TEST(NebiusWithRequiredFieldsDoesNotThrow) {
        auto mvp = MakeTestMvp();
        const TString yaml = R"(
generic:
  access_service_type: "nebius_v1"
meta:
  meta_api_endpoint: "grpc://meta.ydb.example.net:2135"
  meta_database: "/Root/meta"
)";
        const NMvp::NMeta::TMetaAppConfig appConfig = ParseConfig(yaml);
        UNIT_ASSERT_NO_EXCEPTION(mvp.TryGetMetaOptionsFromConfig(appConfig));
        UNIT_ASSERT(appConfig.HasMeta());
        UNIT_ASSERT_VALUES_EQUAL(appConfig.GetMeta().GetMetaApiEndpoint(), "grpc://meta.ydb.example.net:2135");
        UNIT_ASSERT_VALUES_EQUAL(appConfig.GetMeta().GetMetaDatabase(), "/Root/meta");
    }

    Y_UNIT_TEST(WithoutMetaBlockThrows) {
        auto mvp = MakeTestMvp();
        const TString yaml = R"(
generic:
  access_service_type: "yandex_v2"
)";
        const NMvp::NMeta::TMetaAppConfig appConfig = ParseConfig(yaml);
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            mvp.TryGetMetaOptionsFromConfig(appConfig),
            yexception,
            "Check that `meta` section exists and is on the same indentation as `generic` section"
        );
    }
}

Y_UNIT_TEST_SUITE(SupportLinksSourceValidation) {
    static NMVP::TMVP MakeTestMvp() {
        const char* argv[] = {"mvp_test"};
        return NMVP::TMVP(1, argv);
    }

    Y_UNIT_TEST(RejectsMissingSourceInConfig) {
        auto mvp = MakeTestMvp();
        const TString yaml = R"(
generic:
  access_service_type: "yandex_v2"
meta:
  meta_api_endpoint: "grpc://meta.ydb.example.net:2135"
  meta_database: "/Root/meta"
  support_links:
    cluster:
      - title: "Broken"
        url: "https://example.test"
)";
        const NMvp::NMeta::TMetaAppConfig appConfig = ParseConfig(yaml);
        UNIT_ASSERT_EXCEPTION_CONTAINS(mvp.TryGetMetaOptionsFromConfig(appConfig), yexception, "source is required");
    }

    Y_UNIT_TEST(UnsupportedSourceInConfigDoesNotThrow) {
        const TString yaml = R"(
generic:
  access_service_type: "yandex_v2"
meta:
  meta_api_endpoint: "grpc://meta.ydb.example.net:2135"
  meta_database: "/Root/meta"
  support_links:
    cluster:
      - source: "unknown/source"
        title: "Unknown"
        url: "https://example.test"
)";
        const NMvp::NMeta::TMetaAppConfig appConfig = ParseConfig(yaml);

        UNIT_ASSERT(appConfig.HasMeta());
        UNIT_ASSERT(appConfig.GetMeta().HasSupportLinks());
        UNIT_ASSERT_VALUES_EQUAL(appConfig.GetMeta().GetSupportLinks().GetCluster().size(), 1);

        auto source = NMVP::MakeLinkSource(appConfig.GetMeta().GetSupportLinks().GetCluster(0));
        THashMap<TString, TString> clusterColumns;
        NHttp::TUrlParameters urlParameters("");
        const NMVP::ILinkSource::TResolveInput input{
            .Place = 0,
            .ClusterColumns = clusterColumns,
            .UrlParameters = urlParameters,
            .Parent = NActors::TActorId{},
            .HttpProxyId = NActors::TActorId{},
        };
        const NMVP::TResolveOutput output = source->Resolve(input);
        UNIT_ASSERT_VALUES_EQUAL(output.Name, "unknown/source");
        UNIT_ASSERT_VALUES_EQUAL(output.Errors.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(output.Errors.front().Source, "unknown/source");
        UNIT_ASSERT(output.Errors.front().Message.Contains("unsupported support_links source"));
    }

    Y_UNIT_TEST(GrafanaDashboardSourceIsRegisteredAndResolves) {
        auto mvp = MakeTestMvp();
        const TString yaml = R"(
generic:
  access_service_type: "yandex_v2"
meta:
  meta_api_endpoint: "grpc://meta.ydb.example.net:2135"
  meta_database: "/Root/meta"
  grafana:
    endpoint: "https://grafana.example.test"
  support_links:
    cluster:
      - source: "grafana/dashboard"
        title: "Overview"
        url: "/d/ydb/overview"
)";
        const NMvp::NMeta::TMetaAppConfig appConfig = ParseConfig(yaml);
        UNIT_ASSERT_NO_EXCEPTION(mvp.TryGetMetaOptionsFromConfig(appConfig));

        UNIT_ASSERT_VALUES_EQUAL(mvp.MetaSettings.ClusterLinkSources.size(), 1);

        THashMap<TString, TString> clusterColumns{
            {"k8s_namespace", "ydb-workspace"},
            {"datasource", "3f8a1e2c-6b7d-4c91-9a52-1d7f0e8b4a63"},
        };
        NHttp::TUrlParameters urlParameters("/meta/support_links?cluster=ydb-global&database=%2Froot%2Ftest");
        const NMVP::ILinkSource::TResolveInput input{
            .Place = 0,
            .ClusterColumns = clusterColumns,
            .UrlParameters = urlParameters,
            .Parent = NActors::TActorId{},
            .HttpProxyId = NActors::TActorId{},
        };
        const NMVP::TResolveOutput output = mvp.MetaSettings.ClusterLinkSources.front()->Resolve(input);
        UNIT_ASSERT_VALUES_EQUAL(output.Name, "grafana/dashboard");
        UNIT_ASSERT_VALUES_EQUAL(output.Errors.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(output.Links.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(output.Links.front().Title, "Overview");
        UNIT_ASSERT_VALUES_EQUAL(
            output.Links.front().Url,
            "https://grafana.example.test/d/ydb/overview?var-workspace=ydb-workspace&var-ds=3f8a1e2c-6b7d-4c91-9a52-1d7f0e8b4a63&var-cluster=ydb-global&var-database=/root/test"
        );
    }

    Y_UNIT_TEST(GrafanaDashboardSearchSourceRequiresMetaDatabaseTokenName) {
        auto mvp = MakeTestMvp();
        const TString yaml = R"(
generic:
  access_service_type: "yandex_v2"
meta:
  meta_api_endpoint: "grpc://meta.ydb.example.net:2135"
  meta_database: "/Root/meta"
  grafana:
    endpoint: "https://grafana.example.test"
  support_links:
    cluster:
      - source: "grafana/dashboard/search"
        tag: "ydb-common"
)";
        const NMvp::NMeta::TMetaAppConfig appConfig = ParseConfig(yaml);
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            mvp.TryGetMetaOptionsFromConfig(appConfig),
            yexception,
            "meta.meta_database_token_name is required for source=grafana/dashboard/search"
        );
    }

    Y_UNIT_TEST(MetaDatabaseClientSettingsUseExplicitDatabaseParameterOnlyWhenRequested) {
        const TYdbLocation location("meta", "meta", {}, "/Root/meta");
        auto request = BuildHttpRequest("/meta/support_links?cluster=ydb-global&metadb=/root/test");
        const TRequest parsedRequest(NActors::TActorId{}, request);
        auto requestWithoutMetadb = BuildHttpRequest("/meta/support_links?cluster=ydb-global");
        const TRequest parsedRequestWithoutMetadb(NActors::TActorId{}, requestWithoutMetadb);

        const auto defaultSettings = NMVP::TMVP::GetMetaDatabaseClientSettings(parsedRequest, location);
        UNIT_ASSERT(defaultSettings.Database_);
        UNIT_ASSERT_VALUES_EQUAL(*defaultSettings.Database_, "/Root/meta");

        const auto supportLinksSettings = NMVP::TMVP::GetMetaDatabaseClientSettings(parsedRequest, location, "metadb");
        UNIT_ASSERT(supportLinksSettings.Database_);
        UNIT_ASSERT_VALUES_EQUAL(*supportLinksSettings.Database_, "/Root/meta/root/test");

        const auto settingsWithoutMetadb = NMVP::TMVP::GetMetaDatabaseClientSettings(parsedRequestWithoutMetadb, location, "metadb");
        UNIT_ASSERT(settingsWithoutMetadb.Database_);
        UNIT_ASSERT_VALUES_EQUAL(*settingsWithoutMetadb.Database_, "/Root/meta");
    }
}
