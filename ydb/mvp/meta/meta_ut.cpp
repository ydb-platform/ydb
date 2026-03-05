#include <library/cpp/testing/unittest/registar.h>

#include <ydb/mvp/core/mvp_test_runtime.h>
#include <ydb/mvp/meta/mvp.h>
#include <ydb/mvp/meta/support_links/common.h>
#include <ydb/mvp/meta/link_source.h>

#include <util/generic/yexception.h>

#include <memory>

namespace {

NMVP::TSupportLinkEntry MakeLink(TString source, TString url = {}) {
    NMVP::TSupportLinkEntry link;
    link.SetSource(std::move(source));
    link.SetUrl(std::move(url));
    return link;
}

void Validate(
    const TVector<NMVP::TSupportLinkEntry>& clusterLinks,
    const TVector<NMVP::TSupportLinkEntry>& databaseLinks,
    NMVP::TGrafanaSupportConfig grafanaConfig)
{
    NMVP::TMetaSettings settings;
    settings.GrafanaConfig = std::move(grafanaConfig);

    for (size_t i = 0; i < clusterLinks.size(); ++i) {
        auto source = NMVP::MakeLinkSource(i, clusterLinks[i], settings);
        (void)source;
    }
    for (size_t i = 0; i < databaseLinks.size(); ++i) {
        auto source = NMVP::MakeLinkSource(i, databaseLinks[i], settings);
        (void)source;
    }
}

TString ValidateAndCatch(
    const TVector<NMVP::TSupportLinkEntry>& clusterLinks,
    const TVector<NMVP::TSupportLinkEntry>& databaseLinks,
    NMVP::TGrafanaSupportConfig grafanaConfig)
{
    try {
        Validate(clusterLinks, databaseLinks, std::move(grafanaConfig));
    } catch (const yexception& e) {
        return e.what();
    }
    return {};
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(MetaBaseConfiguration) {

    Y_UNIT_TEST(AcceptsValidGrafanaDashboardAndSearchConfig) {
        const char* argv[] = {"support_links_configuration_ut"};
        auto mvp = std::make_unique<NMVP::TMVP>(1, argv);

        NMVP::TGrafanaSupportConfig grafana;
        grafana.Endpoint = "https://grafana.example.net";
        grafana.SecretName = "grafana-secret";

        Validate(
            {MakeLink(NMVP::NSupportLinks::SOURCE_GRAFANA_DASHBOARD, "/d/cpu")},
            {MakeLink(NMVP::NSupportLinks::SOURCE_GRAFANA_DASHBOARD_SEARCH)},
            std::move(grafana));
    }

}

Y_UNIT_TEST_SUITE(MetaConfigurationValidation) {
    Y_UNIT_TEST(MetaBlockWithoutApiEndpointAllowsEmptyValues) {
        TTempFileHandle tmpYaml = MakeTestFile(R"(
generic:
  access_service_type: "yandex_v2"
meta:
  meta_database: "/Root/meta"
)", "mvp_meta_missing_endpoint", ".yaml");

        const char* argv[] = {"mvp_test", "--config", tmpYaml.Name().c_str()};
        UNIT_ASSERT_NO_EXCEPTION(NMVP::TMVP(3, argv));
    }

    Y_UNIT_TEST(MetaBlockWithoutDatabaseAllowsEmptyValues) {
        TTempFileHandle tmpYaml = MakeTestFile(R"(
generic:
  access_service_type: "yandex_v2"
meta:
  meta_api_endpoint: "grpc://meta.ydb.example.net:2135"
)", "mvp_meta_missing_database", ".yaml");

        const char* argv[] = {"mvp_test", "--config", tmpYaml.Name().c_str()};
        UNIT_ASSERT_NO_EXCEPTION(NMVP::TMVP(3, argv));
    }

    Y_UNIT_TEST(NebiusWithoutApiEndpointAllowsEmptyValues) {
        TTempFileHandle tmpYaml = MakeTestFile(R"(
generic:
  access_service_type: "nebius_v1"
)", "mvp_meta_nebius_without_endpoint", ".yaml");

        const char* argv[] = {"mvp_test", "--config", tmpYaml.Name().c_str()};
        UNIT_ASSERT_NO_EXCEPTION(NMVP::TMVP(3, argv));
    }

    Y_UNIT_TEST(NebiusWithoutDatabaseAllowsEmptyValues) {
        TTempFileHandle tmpYaml = MakeTestFile(R"(
generic:
  access_service_type: "nebius_v1"
meta:
  meta_api_endpoint: "grpc://meta.ydb.example.net:2135"
  meta_database: ""
)", "mvp_meta_nebius_without_database", ".yaml");

        const char* argv[] = {"mvp_test", "--config", tmpYaml.Name().c_str()};
        UNIT_ASSERT_NO_EXCEPTION(NMVP::TMVP(3, argv));
    }

    Y_UNIT_TEST(NebiusWithRequiredFieldsDoesNotThrow) {
        TTempFileHandle tmpYaml = MakeTestFile(R"(
generic:
  access_service_type: "nebius_v1"
meta:
  meta_api_endpoint: "grpc://meta.ydb.example.net:2135"
  meta_database: "/Root/meta"
)", "mvp_meta_nebius_with_required_fields", ".yaml");

        const char* argv[] = {"mvp_test", "--config", tmpYaml.Name().c_str()};
        UNIT_ASSERT_NO_EXCEPTION(NMVP::TMVP(3, argv));
    }

    Y_UNIT_TEST(YandexWithoutMetaBlockAllowsEmptyValues) {
        TTempFileHandle tmpYaml = MakeTestFile(R"(
generic:
  access_service_type: "yandex_v2"
)", "mvp_meta_yandex_without_meta_block", ".yaml");

        const char* argv[] = {"mvp_test", "--config", tmpYaml.Name().c_str()};
        UNIT_ASSERT_NO_EXCEPTION(NMVP::TMVP(3, argv));
    }
}

Y_UNIT_TEST_SUITE(SupportUrlConfiguration) {
    Y_UNIT_TEST(RejectsMissingSource) {
        const char* argv[] = {"support_links_configuration_ut"};
        auto mvp = std::make_unique<NMVP::TMVP>(1, argv);

        const TString error = ValidateAndCatch(
            {MakeLink("", "/d/cpu")},
            {},
            {});

        UNIT_ASSERT(!error.empty());
        UNIT_ASSERT(error.Contains("source is required"));
    }

    Y_UNIT_TEST(RejectsUnsupportedSource) {
        const char* argv[] = {"support_links_configuration_ut"};
        auto mvp = std::make_unique<NMVP::TMVP>(1, argv);

        const TString error = ValidateAndCatch(
            {MakeLink("unknown/source", "/x")},
            {},
            {});

        UNIT_ASSERT(!error.empty());
        UNIT_ASSERT(error.Contains("unsupported source=unknown/source"));
    }

    Y_UNIT_TEST(RejectsGrafanaDashboardWithoutUrl) {
        const char* argv[] = {"support_links_configuration_ut"};
        auto mvp = std::make_unique<NMVP::TMVP>(1, argv);

        const TString error = ValidateAndCatch(
            {MakeLink(NMVP::NSupportLinks::SOURCE_GRAFANA_DASHBOARD)},
            {},
            {});

        UNIT_ASSERT(!error.empty());
        UNIT_ASSERT(error.Contains("url is required"));
        UNIT_ASSERT(error.Contains("grafana/dashboard"));
    }
}

Y_UNIT_TEST_SUITE(GrafanaConfiguration) {
    Y_UNIT_TEST(RejectsRelativeGrafanaDashboardUrlWithoutEndpoint) {
        const char* argv[] = {"support_links_configuration_ut"};
        auto mvp = std::make_unique<NMVP::TMVP>(1, argv);

        const TString error = ValidateAndCatch(
            {MakeLink(NMVP::NSupportLinks::SOURCE_GRAFANA_DASHBOARD, "/d/cpu")},
            {},
            {});

        UNIT_ASSERT(!error.empty());
        UNIT_ASSERT(error.Contains("grafana.endpoint is required for relative url"));
    }

    Y_UNIT_TEST(RejectsGrafanaSearchWithoutSecretName) {
        const char* argv[] = {"support_links_configuration_ut"};
        auto mvp = std::make_unique<NMVP::TMVP>(1, argv);

        NMVP::TGrafanaSupportConfig grafana;
        grafana.Endpoint = "https://grafana.example.net";

        const TString error = ValidateAndCatch(
            {},
            {MakeLink(NMVP::NSupportLinks::SOURCE_GRAFANA_DASHBOARD_SEARCH)},
            std::move(grafana));

        UNIT_ASSERT(!error.empty());
        UNIT_ASSERT(error.Contains("grafana.secret_name is required"));
        UNIT_ASSERT(error.Contains("grafana/dashboard/search"));
    }

    Y_UNIT_TEST(RejectsGrafanaSearchDefaultRelativeUrlWithoutEndpoint) {
        const char* argv[] = {"support_links_configuration_ut"};
        auto mvp = std::make_unique<NMVP::TMVP>(1, argv);

        NMVP::TGrafanaSupportConfig grafana;
        grafana.SecretName = "grafana-secret";

        const TString error = ValidateAndCatch(
            {},
            {MakeLink(NMVP::NSupportLinks::SOURCE_GRAFANA_DASHBOARD_SEARCH)},
            std::move(grafana));

        UNIT_ASSERT(!error.empty());
        UNIT_ASSERT(error.Contains("grafana.endpoint is required for relative url"));
    }

    Y_UNIT_TEST(AcceptsGrafanaSearchWithSecretName) {
        const char* argv[] = {"support_links_configuration_ut"};
        auto mvp = std::make_unique<NMVP::TMVP>(1, argv);

        NMVP::TGrafanaSupportConfig grafana;
        grafana.Endpoint = "https://grafana.example.net";
        grafana.SecretName = "grafana-secret";

        Validate(
            {},
            {MakeLink(NMVP::NSupportLinks::SOURCE_GRAFANA_DASHBOARD_SEARCH)},
            std::move(grafana));
    }
}
