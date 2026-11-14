#include "run.h"

#include <library/cpp/testing/common/scope.h>
#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(XdsBootstrapConfigInitializer) {

using namespace NKikimr;

class TTestKikimrRunner : public TKikimrRunner {
    TTestKikimrRunner() = default;

    void InitializeXdsBootstrapConfig(NKikimrConfig::TAppConfig& appConfig) {
        TKikimrRunner::InitializeXdsBootstrapConfig(TKikimrRunConfig(appConfig));
    }

public:
    static void InitXdsBootstrapConfig(NKikimrConfig::TAppConfig& appConfig) {
        TTestKikimrRunner runner;
        runner.InitializeXdsBootstrapConfig(appConfig);
    }
};

const TString XDS_BOOTSTRAP_ENV = "GRPC_XDS_BOOTSTRAP";
const TString XDS_BOOTSTRAP_CONFIG_ENV = "GRPC_XDS_BOOTSTRAP_CONFIG";

struct TXdsBootstrapConfigFixture : public NUnitTest::TBaseFixture {
    NTesting::TScopedEnvironment XdsEnv{{
        {XDS_BOOTSTRAP_ENV, ""},
        {XDS_BOOTSTRAP_CONFIG_ENV, ""},
    }};
};

Y_UNIT_TEST_F(CanNotSetEnvIfXdsBootstrapConfigIsAbsent, TXdsBootstrapConfigFixture) {
    NKikimrConfig::TAppConfig appConfig;
    TTestKikimrRunner::InitXdsBootstrapConfig(appConfig);
    TString jsonXdsBootstrapConfig = GetEnv(XDS_BOOTSTRAP_CONFIG_ENV);
    UNIT_ASSERT_STRINGS_EQUAL_C(jsonXdsBootstrapConfig, "", "The checked value: " + jsonXdsBootstrapConfig);
}

Y_UNIT_TEST_F(CanSetGrpcXdsBootstrapConfigEnv, TXdsBootstrapConfigFixture) {
    NKikimrConfig::TAppConfig appConfig;
    auto* xdsBootstrapConfig = appConfig.MutableGRpcConfig()->MutableXdsBootstrap();
    auto* xdsServers = xdsBootstrapConfig->AddXdsServers();
    xdsServers->SetServerUri("xds-provider.bootstrap.my-company.net:18000");
    *xdsServers->AddServerFeatures() = "xds_v3";
    auto* channelCreds = xdsServers->AddChannelCreds();
    channelCreds->SetType("insecure");
    channelCreds->SetConfig("{\"k1\": \"v1\", \"k2\": \"v2\"}");
    auto* node = xdsBootstrapConfig->MutableNode();
    node->SetId("dc-000-host");
    node->SetCluster("testing");
    node->SetMeta("{\"service\": \"ydb\"}");
    node->MutableLocality()->SetZone("test-zone");

    TTestKikimrRunner::InitXdsBootstrapConfig(appConfig);
    const TString expectedJson = R"({"node":{"cluster":"testing","locality":{"zone":"test-zone"},"metadata":{"service":"ydb"},"id":"dc-000-host"},"xds_servers":[{"channel_creds":[{"config":{"k2":"v2","k1":"v1"},"type":"insecure"}],"server_uri":"xds-provider.bootstrap.my-company.net:18000","server_features":["xds_v3"]}]})";
    TString jsonXdsBootstrapConfig = GetEnv(XDS_BOOTSTRAP_CONFIG_ENV);
    UNIT_ASSERT_STRINGS_EQUAL_C(jsonXdsBootstrapConfig, expectedJson, "The checked value: " + jsonXdsBootstrapConfig);
}

Y_UNIT_TEST_F(CanSetGrpcXdsBootstrapConfigEnvWithSomeNumberOfXdsServers, TXdsBootstrapConfigFixture) {
    NKikimrConfig::TAppConfig appConfig;
    auto* xdsBootstrapConfig = appConfig.MutableGRpcConfig()->MutableXdsBootstrap();
    {
        auto* xdsServers = xdsBootstrapConfig->AddXdsServers();
        xdsServers->SetServerUri("xds-provider-000.bootstrap.my-company.net:18000");
        *xdsServers->AddServerFeatures() = "xds_v3";
        auto* channelCreds = xdsServers->AddChannelCreds();
        channelCreds->SetType("insecure");
        channelCreds->SetConfig("{\"k1\": \"v1\", \"k2\": \"v2\"}");
    }
    {
        auto* xdsServers = xdsBootstrapConfig->AddXdsServers();
        xdsServers->SetServerUri("xds-provider-001.bootstrap.my-company.net:18000");
        *xdsServers->AddServerFeatures() = "xds_v3";
        auto* channelCreds = xdsServers->AddChannelCreds();
        channelCreds->SetType("secure");
        channelCreds->SetConfig("{\"k1\": \"v11\", \"k2\": \"v21\"}");
    }
    auto* node = xdsBootstrapConfig->MutableNode();
    node->SetId("dc-000-host");
    node->SetCluster("testing");
    node->SetMeta("{\"service\": \"ydb\"}");
    node->MutableLocality()->SetZone("test-zone");

    TTestKikimrRunner::InitXdsBootstrapConfig(appConfig);
    const TString expectedJson = R"({"node":{"cluster":"testing","locality":{"zone":"test-zone"},"metadata":{"service":"ydb"},"id":"dc-000-host"},"xds_servers":[{"channel_creds":[{"config":{"k2":"v2","k1":"v1"},"type":"insecure"}],"server_uri":"xds-provider-000.bootstrap.my-company.net:18000","server_features":["xds_v3"]},{"channel_creds":[{"config":{"k2":"v21","k1":"v11"},"type":"secure"}],"server_uri":"xds-provider-001.bootstrap.my-company.net:18000","server_features":["xds_v3"]}]})";
    TString jsonXdsBootstrapConfig = GetEnv(XDS_BOOTSTRAP_CONFIG_ENV);
    UNIT_ASSERT_STRINGS_EQUAL_C(jsonXdsBootstrapConfig, expectedJson, "The checked value: " + jsonXdsBootstrapConfig);
}

Y_UNIT_TEST_F(CanNotSetGrpcXdsBootstrapConfigEnvIfVariableAlreadySet, TXdsBootstrapConfigFixture) {
    NKikimrConfig::TAppConfig appConfig;
    auto* xdsBootstrapConfig = appConfig.MutableGRpcConfig()->MutableXdsBootstrap();
    auto* xdsServers = xdsBootstrapConfig->AddXdsServers();
    xdsServers->SetServerUri("xds-provider.bootstrap.my-company.net:18000");
    *xdsServers->AddServerFeatures() = "xds_v3";
    auto* channelCreds = xdsServers->AddChannelCreds();
    channelCreds->SetType("insecure");
    channelCreds->SetConfig("{\"k1\": \"v1\", \"k2\": \"v2\"}");
    auto* node = xdsBootstrapConfig->MutableNode();
    node->SetId("dc-000-host");
    node->SetCluster("testing");
    node->SetMeta("{\"service\": \"ydb\"}");
    node->MutableLocality()->SetZone("test-zone");

    SetEnv(XDS_BOOTSTRAP_CONFIG_ENV, "{xds bootstrap config already set}");

    TTestKikimrRunner::InitXdsBootstrapConfig(appConfig);
    TString jsonXdsBootstrapConfig = GetEnv(XDS_BOOTSTRAP_CONFIG_ENV);
    UNIT_ASSERT_STRINGS_EQUAL_C(jsonXdsBootstrapConfig, "{xds bootstrap config already set}", "The checked value: " + jsonXdsBootstrapConfig);
}

} // XdsBootstrapConfigInitializer

Y_UNIT_TEST_SUITE(GrpcConfigurationInitializer) {
    class TTestKikimrRunner : public NKikimr::TKikimrRunner {
    public:
        using TKikimrRunner::InitializeGRpc;

        bool IsGrpcEnabled() const {
            return EnabledGrpcService;
        }
    };

    Y_UNIT_TEST(EnabledBeforeFactoryRuns) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableGRpcConfig()->SetStartGRpcProxy(true);
        TTestKikimrRunner runner;
        runner.InitializeGRpc(NKikimr::TKikimrRunConfig(appConfig));
        UNIT_ASSERT(runner.IsGrpcEnabled());
    }

    Y_UNIT_TEST(DisabledWithoutGrpcConfig) {
        NKikimrConfig::TAppConfig appConfig;
        TTestKikimrRunner runner;
        runner.InitializeGRpc(NKikimr::TKikimrRunConfig(appConfig));
        UNIT_ASSERT(!runner.IsGrpcEnabled());
    }

    Y_UNIT_TEST(DisabledByGrpcConfig) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableGRpcConfig()->SetStartGRpcProxy(false);
        TTestKikimrRunner runner;
        runner.InitializeGRpc(NKikimr::TKikimrRunConfig(appConfig));
        UNIT_ASSERT(!runner.IsGrpcEnabled());
    }
}
