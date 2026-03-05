#include <library/cpp/testing/unittest/registar.h>

#include <ydb/mvp/core/utils.h>
#include <ydb/mvp/core/mvp_test_runtime.h>
#include <ydb/mvp/meta/mvp.h>
#include <ydb/mvp/meta/support_links/source.h>

#include <util/generic/yexception.h>
#include <yaml-cpp/yaml.h>

namespace {

template <size_t N>
NMVP::TMVP* CreateMetaMvpNoDestroy(const char* (&argv)[N]) {
    return new NMVP::TMVP(N, argv);
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(MetaConfigurationValidation) {
    Y_UNIT_TEST(MetaBlockWithoutApiEndpointThrows) {
        TTempFileHandle tmpYaml = MakeTestFile(R"(
generic:
  access_service_type: "yandex_v2"
meta:
  meta_database: "/Root/meta"
)", "mvp_meta_missing_endpoint", ".yaml");
        const char* argv[] = {"mvp_test", "--config", tmpYaml.Name().c_str()};
        UNIT_ASSERT_EXCEPTION_CONTAINS(CreateMetaMvpNoDestroy(argv), yexception, "meta.meta_api_endpoint must be specified");
    }

    Y_UNIT_TEST(MetaBlockWithoutDatabaseThrows) {
        TTempFileHandle tmpYaml = MakeTestFile(R"(
generic:
  access_service_type: "yandex_v2"
meta:
  meta_api_endpoint: "grpc://meta.ydb.example.net:2135"
)", "mvp_meta_missing_database", ".yaml");
        const char* argv[] = {"mvp_test", "--config", tmpYaml.Name().c_str()};
        UNIT_ASSERT_EXCEPTION_CONTAINS(CreateMetaMvpNoDestroy(argv), yexception, "meta.meta_database must be specified");
    }

    Y_UNIT_TEST(NebiusWithoutApiEndpointThrows) {
        TTempFileHandle tmpYaml = MakeTestFile(R"(
generic:
  access_service_type: "nebius_v1"
meta:
  meta_database: "/Root/meta"
)", "mvp_meta_nebius_without_endpoint", ".yaml");
        const char* argv[] = {"mvp_test", "--config", tmpYaml.Name().c_str()};
        UNIT_ASSERT_EXCEPTION_CONTAINS(CreateMetaMvpNoDestroy(argv), yexception, "meta.meta_api_endpoint must be specified");
    }

    Y_UNIT_TEST(NebiusWithoutDatabaseThrows) {
        TTempFileHandle tmpYaml = MakeTestFile(R"(
generic:
  access_service_type: "nebius_v1"
meta:
  meta_api_endpoint: "grpc://meta.ydb.example.net:2135"
)", "mvp_meta_nebius_without_database", ".yaml");
        const char* argv[] = {"mvp_test", "--config", tmpYaml.Name().c_str()};
        UNIT_ASSERT_EXCEPTION_CONTAINS(CreateMetaMvpNoDestroy(argv), yexception, "meta.meta_database must be specified");
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
        UNIT_ASSERT_NO_EXCEPTION(CreateMetaMvpNoDestroy(argv));
    }

    Y_UNIT_TEST(YandexWithoutMetaBlockAllowsEmptyValues) {
        TTempFileHandle tmpYaml = MakeTestFile(R"(
generic:
  access_service_type: "yandex_v2"
)", "mvp_meta_yandex_without_meta_block", ".yaml");
        const char* argv[] = {"mvp_test", "--config", tmpYaml.Name().c_str()};
        UNIT_ASSERT_NO_EXCEPTION(CreateMetaMvpNoDestroy(argv));
    }
}

Y_UNIT_TEST_SUITE(SupportLinksSourceValidation) {
    Y_UNIT_TEST(RejectsMissingSourceInConfig) {
        TTempFileHandle tmpYaml = MakeTestFile(R"(
generic:
  access_service_type: "yandex_v2"
meta:
  meta_api_endpoint: "grpc://meta.ydb.example.net:2135"
  meta_database: "/Root/meta"
  support_links:
    cluster:
      - title: "Broken"
        url: "https://example.test"
)", "mvp_meta_support_links_missing_source", ".yaml");
        const char* argv[] = {"mvp_test", "--config", tmpYaml.Name().c_str()};
        UNIT_ASSERT_EXCEPTION_CONTAINS(CreateMetaMvpNoDestroy(argv), yexception, "source is required");
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

        YAML::Node node = YAML::Load(yaml);
        NMvp::NMeta::TMetaAppConfig appConfig;
        NMVP::MergeYamlNodeToProto(node, appConfig);

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
}
