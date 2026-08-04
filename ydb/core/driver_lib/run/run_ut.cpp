#include "run.h"
#include "config_helpers.h"
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

Y_UNIT_TEST(CanNotSetEnvIfXdsBootstrapConfigIsAbsent) {
    NKikimrConfig::TAppConfig appConfig;
    TTestKikimrRunner::InitXdsBootstrapConfig(appConfig);
    TString jsonXdsBootstrapConfig = GetEnv(XDS_BOOTSTRAP_CONFIG_ENV);
    UNIT_ASSERT_STRINGS_EQUAL_C(jsonXdsBootstrapConfig, "", "The checked value: " + jsonXdsBootstrapConfig);
}

Y_UNIT_TEST(CanSetGrpcXdsBootstrapConfigEnv) {
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

Y_UNIT_TEST(CanSetGrpcXdsBootstrapConfigEnvWithSomeNumberOfXdsServers) {
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

Y_UNIT_TEST(CanNotSetGrpcXdsBootstrapConfigEnvIfVariableAlreadySet) {
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

Y_UNIT_TEST_SUITE(ActorSystemConfigHelpers) {

using namespace NKikimr;

Y_UNIT_TEST(HarmonizerNeedyCpuWindow) {
    NKikimrConfig::TActorSystemConfig systemConfig;
    NActors::TCpuManagerConfig cpuManager;

    auto* defaultExecutor = systemConfig.AddExecutor();
    defaultExecutor->SetType(NKikimrConfig::TActorSystemConfig::TExecutor::BASIC);
    defaultExecutor->SetName("System");
    defaultExecutor->SetThreads(1);
    defaultExecutor->SetMaxThreads(2);
    NActorSystemConfigHelpers::AddExecutorPool(cpuManager, *defaultExecutor, systemConfig, 0, nullptr);

    auto* configuredExecutor = systemConfig.AddExecutor();
    configuredExecutor->SetType(NKikimrConfig::TActorSystemConfig::TExecutor::BASIC);
    configuredExecutor->SetName("User");
    configuredExecutor->SetThreads(1);
    configuredExecutor->SetMaxThreads(2);
    configuredExecutor->SetHarmonizerNeedyCpuWindowSeconds(30);
    NActorSystemConfigHelpers::AddExecutorPool(cpuManager, *configuredExecutor, systemConfig, 1, nullptr);

    UNIT_ASSERT_VALUES_EQUAL(cpuManager.Basic.size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(cpuManager.Basic[0].HarmonizerNeedyCpuWindowSeconds, 1);
    UNIT_ASSERT_VALUES_EQUAL(cpuManager.Basic[1].HarmonizerNeedyCpuWindowSeconds, 30);
}

} // ActorSystemConfigHelpers
