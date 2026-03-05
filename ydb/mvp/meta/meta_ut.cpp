#include <library/cpp/testing/unittest/registar.h>

#include <ydb/mvp/meta/mvp.h>
#include <ydb/mvp/meta/support_links/common.h>
#include <ydb/mvp/meta/link_source.h>

#include <util/generic/yexception.h>

#include <memory>

namespace {

class TMvpGuard {
public:
    TMvpGuard() {
        const char* argv[] = {"support_links_configuration_ut"};
        Mvp = std::make_unique<NMVP::TMVP>(1, argv);
    }

private:
    std::unique_ptr<NMVP::TMVP> Mvp;
};

NMVP::TSupportLinkEntryConfig MakeLink(TString source, TString url = {}) {
    NMVP::TSupportLinkEntryConfig link;
    link.Source = std::move(source);
    link.Url = std::move(url);
    return link;
}

void Validate(
    const TVector<NMVP::TSupportLinkEntryConfig>& clusterLinks,
    const TVector<NMVP::TSupportLinkEntryConfig>& databaseLinks,
    NMVP::TGrafanaSupportConfig grafanaConfig)
{
    for (size_t i = 0; i < clusterLinks.size(); ++i) {
        const TString where = TStringBuilder() << "support_links.cluster[" << i << "]";
        auto source = NMVP::MakeLinkSource(i, clusterLinks[i], grafanaConfig, where);
        (void)source;
    }
    for (size_t i = 0; i < databaseLinks.size(); ++i) {
        const TString where = TStringBuilder() << "support_links.database[" << i << "]";
        auto source = NMVP::MakeLinkSource(i, databaseLinks[i], grafanaConfig, where);
        (void)source;
    }
}

TString ValidateAndCatch(
    const TVector<NMVP::TSupportLinkEntryConfig>& clusterLinks,
    const TVector<NMVP::TSupportLinkEntryConfig>& databaseLinks,
    NMVP::TGrafanaSupportConfig grafanaConfig)
{
    try {
        Validate(clusterLinks, databaseLinks, std::move(grafanaConfig));
    } catch (const yexception& e) {
        return e.what();
    }
    return {};
}

void AssertMetaValidationThrows(
    TStringBuf metaApiEndpoint,
    TStringBuf metaDatabase,
    bool hasMetaConfigBlock,
    NMvp::EAccessServiceType accessServiceType,
    TStringBuf errorPart)
{
    NMVP::TMetaSettings settings{
        TString(metaApiEndpoint),
        TString(metaDatabase),
        hasMetaConfigBlock,
        accessServiceType,
        {},
        {}
    };
    TString error;
    try {
        NMVP::ValidateMetaBaseConfig(settings);
    } catch (const yexception& e) {
        error = e.what();
    }

    UNIT_ASSERT(!error.empty());
    UNIT_ASSERT(error.Contains(errorPart));
}

void AssertMetaValidationNoThrow(
    TStringBuf metaApiEndpoint,
    TStringBuf metaDatabase,
    bool hasMetaConfigBlock,
    NMvp::EAccessServiceType accessServiceType)
{
    NMVP::TMetaSettings settings{
        TString(metaApiEndpoint),
        TString(metaDatabase),
        hasMetaConfigBlock,
        accessServiceType,
        {},
        {}
    };
    UNIT_ASSERT_NO_EXCEPTION(
        NMVP::ValidateMetaBaseConfig(settings)
    );
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(MetaBaseConfiguration) {

    Y_UNIT_TEST(AcceptsValidGrafanaDashboardAndSearchConfig) {
        TMvpGuard guard;

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
    Y_UNIT_TEST(MetaBlockWithoutApiEndpointThrows) {
        AssertMetaValidationThrows(
            "",
            "/Root/meta",
            true,
            NMvp::yandex_v2,
            "meta.meta_api_endpoint must be specified in meta config.");
    }

    Y_UNIT_TEST(MetaBlockWithoutDatabaseThrows) {
        AssertMetaValidationThrows(
            "grpc://meta.ydb.example.net:2135",
            "",
            true,
            NMvp::yandex_v2,
            "meta.meta_database must be specified in meta config.");
    }

    Y_UNIT_TEST(NebiusWithoutApiEndpointThrows) {
        AssertMetaValidationThrows(
            "",
            "/Root/meta",
            false,
            NMvp::nebius_v1,
            "meta.meta_api_endpoint must be specified for access_service_type=nebius_v1.");
    }

    Y_UNIT_TEST(NebiusWithoutDatabaseThrows) {
        AssertMetaValidationThrows(
            "grpc://meta.ydb.example.net:2135",
            "",
            false,
            NMvp::nebius_v1,
            "meta.meta_database must be specified for access_service_type=nebius_v1.");
    }

    Y_UNIT_TEST(NebiusWithRequiredFieldsDoesNotThrow) {
        AssertMetaValidationNoThrow(
            "grpc://meta.ydb.example.net:2135",
            "/Root/meta",
            false,
            NMvp::nebius_v1);
    }

    Y_UNIT_TEST(YandexWithoutMetaBlockAllowsEmptyValues) {
        AssertMetaValidationNoThrow("", "", false, NMvp::yandex_v2);
    }
}

Y_UNIT_TEST_SUITE(SupportUrlConfiguration) {
    Y_UNIT_TEST(RejectsMissingSource) {
        TMvpGuard guard;

        const TString error = ValidateAndCatch(
            {MakeLink("", "/d/cpu")},
            {},
            {});

        UNIT_ASSERT(!error.empty());
        UNIT_ASSERT(error.Contains("support_links.cluster[0]"));
        UNIT_ASSERT(error.Contains("source is required"));
    }

    Y_UNIT_TEST(RejectsUnsupportedSource) {
        TMvpGuard guard;

        const TString error = ValidateAndCatch(
            {MakeLink("unknown/source", "/x")},
            {},
            {});

        UNIT_ASSERT(!error.empty());
        UNIT_ASSERT(error.Contains("unsupported source=unknown/source"));
    }

    Y_UNIT_TEST(RejectsGrafanaDashboardWithoutUrl) {
        TMvpGuard guard;

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
        TMvpGuard guard;

        const TString error = ValidateAndCatch(
            {MakeLink(NMVP::NSupportLinks::SOURCE_GRAFANA_DASHBOARD, "/d/cpu")},
            {},
            {});

        UNIT_ASSERT(!error.empty());
        UNIT_ASSERT(error.Contains("grafana.endpoint is required for relative url"));
    }

    Y_UNIT_TEST(RejectsGrafanaSearchWithoutSecretName) {
        TMvpGuard guard;

        NMVP::TGrafanaSupportConfig grafana;
        grafana.Endpoint = "https://grafana.example.net";

        const TString error = ValidateAndCatch(
            {},
            {MakeLink(NMVP::NSupportLinks::SOURCE_GRAFANA_DASHBOARD_SEARCH)},
            std::move(grafana));

        UNIT_ASSERT(!error.empty());
        UNIT_ASSERT(error.Contains("support_links.database[0]"));
        UNIT_ASSERT(error.Contains("grafana.secret_name is required"));
        UNIT_ASSERT(error.Contains("grafana/dashboard/search"));
    }

    Y_UNIT_TEST(RejectsGrafanaSearchDefaultRelativeUrlWithoutEndpoint) {
        TMvpGuard guard;

        NMVP::TGrafanaSupportConfig grafana;
        grafana.SecretName = "grafana-secret";

        const TString error = ValidateAndCatch(
            {},
            {MakeLink(NMVP::NSupportLinks::SOURCE_GRAFANA_DASHBOARD_SEARCH)},
            std::move(grafana));

        UNIT_ASSERT(!error.empty());
        UNIT_ASSERT(error.Contains("support_links.database[0]"));
        UNIT_ASSERT(error.Contains("grafana.endpoint is required for relative url"));
    }

    Y_UNIT_TEST(AcceptsGrafanaSearchWithSecretName) {
        TMvpGuard guard;

        NMVP::TGrafanaSupportConfig grafana;
        grafana.Endpoint = "https://grafana.example.net";
        grafana.SecretName = "grafana-secret";

        Validate(
            {},
            {MakeLink(NMVP::NSupportLinks::SOURCE_GRAFANA_DASHBOARD_SEARCH)},
            std::move(grafana));
    }
}
