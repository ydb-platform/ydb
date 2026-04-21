#include <ydb/mvp/meta/mvp.h>
#include <ydb/mvp/core/core_ydb.h>
#include <ydb/mvp/core/mvp_test_runtime.h>
#include <ydb/mvp/meta/support_links/source.h>
#include <ydb/mvp/core/utils.h>

#include <ydb/library/actors/http/http.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/yexception.h>
#include <yaml-cpp/yaml.h>

static NMvp::NMeta::TMetaAppConfig ParseConfig(const TString& yaml) {
    YAML::Node node = YAML::Load(yaml);
    NMvp::NMeta::TMetaAppConfig appConfig;
    NMVP::MergeYamlNodeToProto(node, appConfig);
    return appConfig;
}

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

    Y_UNIT_TEST(UnsupportedSourceInConfigThrows) {
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
        auto mvp = MakeTestMvp();
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            mvp.TryGetMetaOptionsFromConfig(appConfig),
            yexception,
            "unsupported support_links source: unknown/source"
        );
    }

}
