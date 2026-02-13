#include "init_impl.h"
#include "mock.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/system/tempfile.h>
#include <util/stream/output.h>
#include <ydb/core/config/init/init.h>
#include <ydb/core/config/validation/validators.h>
#include <ydb/library/yaml_config/yaml_config.h>

using namespace NKikimr;
using namespace NKikimr::NConfig;

Y_UNIT_TEST_SUITE(Init) {
    Y_UNIT_TEST(TWithDefaultParser) {
        {
            NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
            TCommonAppOptions commonOpts;
            commonOpts.RegisterCliOptions(opts);
            TVector<const char*> args = {"ydbd"};
            NLastGetopt::TOptsParseResult res(&opts, args.size(), args.data());

            UNIT_ASSERT(!commonOpts.ClusterName);
            UNIT_ASSERT_VALUES_EQUAL(*commonOpts.ClusterName, "unknown");

            UNIT_ASSERT(!commonOpts.LogSamplingLevel);
            UNIT_ASSERT_VALUES_EQUAL(*commonOpts.LogSamplingLevel, NActors::NLog::PRI_DEBUG);
        }

        {
            NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
            TCommonAppOptions commonOpts;
            commonOpts.RegisterCliOptions(opts);
            TVector<const char*> args = {"ydbd", "--cluster-name", "unknown", "--log-sampling-level", "7"};
            NLastGetopt::TOptsParseResult res(&opts, args.size(), args.data());

            UNIT_ASSERT(commonOpts.ClusterName);
            UNIT_ASSERT_VALUES_EQUAL(*commonOpts.ClusterName, "unknown");

            UNIT_ASSERT(commonOpts.LogSamplingLevel);
            UNIT_ASSERT_VALUES_EQUAL(*commonOpts.LogSamplingLevel, NActors::NLog::PRI_DEBUG);
        }

        {
            NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
            TCommonAppOptions commonOpts;
            commonOpts.RegisterCliOptions(opts);
            TVector<const char*> args = {"ydbd", "--log-sampling-level", "string"};

            auto parse = [&]() {
                NLastGetopt::TOptsParseResultException res(&opts, args.size(), args.data());
            };

            UNIT_ASSERT_EXCEPTION(parse(), NLastGetopt::TUsageException);
        }
    }
}

namespace {

TTempFileHandle CreateConfigFile(const TString& config) {
    TTempFileHandle tempFile = TTempFileHandle::InCurrentDir("test_config", ".yaml");
    TUnbufferedFileOutput fileOutput(tempFile.Name());
    fileOutput.Write(config);
    fileOutput.Finish();
    return tempFile;
}

void PreFillArgs(std::vector<TString>& args, const TString& configPath) {
    args.push_back("server");

    args.push_back("--node");
    args.push_back("static");

    args.push_back("--ic-port");
    args.push_back("9001");

    args.push_back("--grpc-port");
    args.push_back("2135");

    args.push_back("--mon-port");
    args.push_back("8765");

    args.push_back("--yaml-config");
    args.push_back(configPath);
}

NKikimrConfig::TAppConfig TransformConfig(const std::vector<TString>& args, std::unique_ptr<NConfig::IEnv> envMock) {
    auto errorCollector = NConfig::MakeDefaultErrorCollector();
    auto protoConfigFileProvider = NConfig::MakeDefaultProtoConfigFileProvider();
    auto configUpdateTracer = NConfig::MakeDefaultConfigUpdateTracer();
    auto memLogInit = NConfig::MakeNoopMemLogInitializer();
    auto nodeBrokerClient = NConfig::MakeNoopNodeBrokerClient();
    auto dynConfigClient = NConfig::MakeNoopDynConfigClient();
    auto configClient = NConfig::MakeNoopConfigClient();
    auto logger = NConfig::MakeNoopInitLogger();

    NConfig::TInitialConfiguratorDependencies deps{
        *errorCollector,
        *protoConfigFileProvider,
        *configUpdateTracer,
        *memLogInit,
        *nodeBrokerClient,
        *dynConfigClient,
        *configClient,
        *envMock,
        *logger,
    };
    auto initCfg = NConfig::MakeDefaultInitialConfigurator(deps);

    std::vector<const char*> argv;

    for (const auto& arg : args) {
        argv.push_back(arg.data());
    }

    NLastGetopt::TOpts opts;
    initCfg->RegisterCliOptions(opts);
    protoConfigFileProvider->RegisterCliOptions(opts);

    NLastGetopt::TOptsParseResult parseResult(&opts, argv.size(), argv.data());

    initCfg->ValidateOptions(opts, parseResult);
    initCfg->Parse(parseResult.GetFreeArgs(), nullptr);

    NKikimrConfig::TAppConfig appConfig;
    ui32 nodeId;
    TKikimrScopeId scopeId;
    TString tenantName;
    TBasicKikimrServicesMask servicesMask;
    bool tinyMode;
    TString clusterName;
    NConfig::TConfigsDispatcherInitInfo configsDispatcherInitInfo;

    initCfg->Apply(
        appConfig,
        nodeId,
        scopeId,
        tenantName,
        servicesMask,
        tinyMode,
        clusterName,
        configsDispatcherInitInfo);

    return appConfig;
}

std::unique_ptr<NConfig::IEnv> GetEnvMock(const TString& hostName = "localhost", const TString& fqdnHostName = "localhost") {
    auto envMock = std::make_unique<NConfig::TEnvMock>();
    envMock->SavedHostName = hostName;
    envMock->SavedFQDNHostName = fqdnHostName;
    return envMock;
}

} // namespace


Y_UNIT_TEST_SUITE(StaticNodeSelectorsInit) {

    TString config = R"(
metadata:
  kind: MainConfig
  version: 0
  cluster: test_cluster
config:
  default_disk_type: NVME
  self_management_config:
    enabled: true

  actor_system_config:
    use_auto_config: true
    node_type: COMPUTE
    cpu_count: 10

  host_configs:
  - nvme:
    - disk1
    - disk2
  hosts:
    - host: localhost
      port: 9001

  log_config:
    default_level: 5
    entry:
    - component: BS_CONTROLLER
      level: 7

allowed_labels:
  test:
    type: string

selector_config:
  - description: "Selector for static nodes"
    selector:
      test: abc
    config:
      actor_system_config:
        use_auto_config: true
        node_type: STORAGE
        cpu_count: 100
      log_config: !inherit
        entry: !append
        - component: CONSOLE_HANDSHAKE
          level: 6
  - description: "Selector by node_id"
    selector:
      node_id: 1
      test: node_id
    config:
      actor_system_config:
        use_auto_config: true
        node_type: STORAGE
        cpu_count: 1
  - description: "Selector by node_host"
    selector:
      node_host: localhost
      test: node_host
    config:
      actor_system_config:
        use_auto_config: true
        node_type: STORAGE
        cpu_count: 50
  - description: "Selector by node_kind"
    selector:
      node_kind: static
      test: node_kind
    config:
      actor_system_config:
        use_auto_config: true
        node_type: STORAGE
        cpu_count: 42
)";

    Y_UNIT_TEST(TestStaticNodeSelectorForActorSystem) {
        TTempFileHandle configFile = CreateConfigFile(config);
        TVector<TString> args;
        PreFillArgs(args, configFile.Name());
        args.push_back("--label");
        args.push_back("test=abc");
        NKikimrConfig::TAppConfig appConfig = TransformConfig(args, GetEnvMock());

        UNIT_ASSERT(appConfig.HasActorSystemConfig());
        const auto& actorConfig = appConfig.GetActorSystemConfig();
        UNIT_ASSERT_EQUAL(actorConfig.GetCpuCount(), 100);
    }

    Y_UNIT_TEST(TestStaticNodeSelectorWithAnotherLabel) {
        TTempFileHandle configFile = CreateConfigFile(config);
        TVector<TString> args;
        PreFillArgs(args, configFile.Name());
        args.push_back("--label");
        args.push_back("test=abd");
        NKikimrConfig::TAppConfig appConfig = TransformConfig(args, GetEnvMock());

        UNIT_ASSERT(appConfig.HasActorSystemConfig());
        const auto& actorConfig = appConfig.GetActorSystemConfig();
        UNIT_ASSERT_VALUES_EQUAL(actorConfig.GetCpuCount(), 10);

    }

    Y_UNIT_TEST(TestStaticNodeSelectorInheritance) {
        TTempFileHandle configFile = CreateConfigFile(config);
        TVector<TString> args;
        PreFillArgs(args, configFile.Name());
        args.push_back("--label");
        args.push_back("test=abc");
        NKikimrConfig::TAppConfig appConfig = TransformConfig(args, GetEnvMock());

        UNIT_ASSERT(appConfig.HasLogConfig());
        const auto& logConfig = appConfig.GetLogConfig();
        UNIT_ASSERT_EQUAL(logConfig.GetDefaultLevel(), 5);
        UNIT_ASSERT_EQUAL(logConfig.GetEntry(0).GetLevel(), 7);
        UNIT_ASSERT_EQUAL(logConfig.GetEntry(1).GetLevel(), 6);
    }

    Y_UNIT_TEST(TestStaticNodeSelectorByNodeId) {
        TTempFileHandle configFile = CreateConfigFile(config);
        TVector<TString> args;
        PreFillArgs(args, configFile.Name());
        args.push_back("--label");
        args.push_back("test=node_id");
        NKikimrConfig::TAppConfig appConfig = TransformConfig(args, GetEnvMock());

        UNIT_ASSERT(appConfig.HasActorSystemConfig());
        const auto& actorConfig = appConfig.GetActorSystemConfig();
        UNIT_ASSERT_EQUAL(actorConfig.GetCpuCount(), 1);
    }

    Y_UNIT_TEST(TestStaticNodeSelectorByNodeHost) {
        TTempFileHandle configFile = CreateConfigFile(config);
        TVector<TString> args;
        PreFillArgs(args, configFile.Name());
        args.push_back("--label");
        args.push_back("test=node_host");

        NKikimrConfig::TAppConfig appConfig = TransformConfig(args, GetEnvMock());

        UNIT_ASSERT(appConfig.HasActorSystemConfig());
        const auto& actorConfig = appConfig.GetActorSystemConfig();
        UNIT_ASSERT_EQUAL(actorConfig.GetCpuCount(), 50);
    }

    Y_UNIT_TEST(TestStaticNodeSelectorByNodeKind) {
        TTempFileHandle configFile = CreateConfigFile(config);
        TVector<TString> args;
        PreFillArgs(args, configFile.Name());
        args.push_back("--label");
        args.push_back("test=node_kind");
        NKikimrConfig::TAppConfig appConfig = TransformConfig(args, GetEnvMock());

        UNIT_ASSERT(appConfig.HasActorSystemConfig());
        const auto& actorConfig = appConfig.GetActorSystemConfig();
        UNIT_ASSERT_EQUAL(actorConfig.GetCpuCount(), 42);
    }
}

Y_UNIT_TEST_SUITE(XdsBootstrapConfig) {
  TString config = R"(
metadata:
  kind: MainConfig
  version: 0
  cluster: test_cluster
config:
  default_disk_type: NVME
  self_management_config:
    enabled: false

  channel_profile_config:
    profile:
    - channel:
      - erasure_species: mirror-3-dc
        pdisk_category: 1
        storage_pool_kind: ssdencrypted
        vdisk_category: Default
      - erasure_species: mirror-3-dc
        pdisk_category: 1
        storage_pool_kind: ssdencrypted
        vdisk_category: Default
      - erasure_species: mirror-3-dc
        pdisk_category: 1
        storage_pool_kind: ssdencrypted
        vdisk_category: Default
      profile_id: 0

  actor_system_config:
    use_auto_config: true
    node_type: COMPUTE
    cpu_count: 10

  domains_config:
    disable_builtin_security: true
    domain:
    - domain_id: 1
      name: testing
      plan_resolution: '10'
      scheme_root: '72057594046678944'
      ssid:
      - 1
      storage_pool_types:
      - kind: ssd
        pool_config:
          box_id: '1'
          erasure_species: mirror-3-dc
          kind: ssd
          pdisk_filter:
          - property:
            - type: SSD
          vdisk_kind: Default

  host_configs:
  - nvme:
    - disk1
    - disk2
  hosts:
    - host: sas-000-localhost
      port: 9001
      node_id: 1
      walle_location:
        body: 102526684
        data_center: SAS
        rack: SAS-12#12.1.02
    - host: vla-000-localhost
      port: 9001
      node_id: 2
      walle_location:
        body: 102526684
        data_center: VLA
        rack: VLA-12#12.1.02
    - host: klg-000-localhost
      port: 9001
      node_id: 3
      walle_location:
        body: 102526684
        data_center: KLG
        rack: KLG-12#12.1.02

  log_config:
    default_level: 5
    entry:
    - component: BS_CONTROLLER
      level: 7
)";

    Y_UNIT_TEST(CanSetHostnameAndDataCenterFromYdbNode) {
        TString tmpConfig(config);
        tmpConfig.append(R"(
  grpc_config:
    xds_bootstrap:
      xds_servers:
      - server_uri: "xds-provider.bootstrap.cloud-testing.yandex.net:18000"
        server_features:
        - "xds_v3"
        channel_creds:
        - type: "insecure"
          config: {"k1": "v1", "k2": "v2"}
      node:
        # id: Do not set id. Try set id from ydb node instance
        cluster: "testing"
        meta: {"service": "ydb"}
        #locality:
          #zone: Try set zone as data center of ydb node instance
)");
        TTempFileHandle configFile = CreateConfigFile(tmpConfig);
        TVector<TString> args;
        PreFillArgs(args, configFile.Name());
        NKikimrConfig::TAppConfig appConfig = TransformConfig(args, GetEnvMock(/*hostname*/"vla-000-localhost", /*fqdnHostName*/"vla-000-localhost"));

        UNIT_ASSERT(appConfig.HasGRpcConfig());
        UNIT_ASSERT(appConfig.GetGRpcConfig().HasXdsBootstrap());
        const auto& xdsBootstrap = appConfig.GetGRpcConfig().GetXdsBootstrap();
        UNIT_ASSERT_EQUAL(xdsBootstrap.GetNode().GetId(), "vla-000-localhost");
        UNIT_ASSERT_EQUAL(xdsBootstrap.GetNode().GetLocality().GetZone(), "vla");
    }

    Y_UNIT_TEST(CanSetDataCenterFromYdbNodeArgument) {
      TString tmpConfig(config);
      tmpConfig.append(R"(
  grpc_config:
    xds_bootstrap:
      xds_servers:
      - server_uri: "xds-provider.bootstrap.cloud-testing.yandex.net:18000"
        server_features:
        - "xds_v3"
        channel_creds:
        - type: "insecure"
          config: {"k1": "v1", "k2": "v2"}
      node:
        # id: Do not set id. Try set id from ydb node instance
        cluster: "testing"
        meta: {"service": "ydb"}
        #locality:
          #zone: Try set zone as data center of ydb node instance
)");
        TTempFileHandle configFile = CreateConfigFile(tmpConfig);
        TVector<TString> args;
        PreFillArgs(args, configFile.Name());
        args.push_back("--data-center");
        args.push_back("dc-klg");
        NKikimrConfig::TAppConfig appConfig = TransformConfig(args, GetEnvMock(/*hostname*/"klg-000-localhost", /*fqdnHostName*/"klg-000-localhost"));

        UNIT_ASSERT(appConfig.HasGRpcConfig());
        UNIT_ASSERT(appConfig.GetGRpcConfig().HasXdsBootstrap());
        const auto& xdsBootstrap = appConfig.GetGRpcConfig().GetXdsBootstrap();
        UNIT_ASSERT_EQUAL(xdsBootstrap.GetNode().GetId(), "klg-000-localhost");
        UNIT_ASSERT_EQUAL(xdsBootstrap.GetNode().GetLocality().GetZone(), "dc-klg");
    }

    Y_UNIT_TEST(CanCheckThatXdsBootstrapIsAbsent) {
        TString tmpConfig(config);
        tmpConfig.append(R"(
  grpc_config:
    port: 2135
)");
        TTempFileHandle configFile = CreateConfigFile(tmpConfig);
        TVector<TString> args;
        PreFillArgs(args, configFile.Name());
        args.push_back("--data-center");
        args.push_back("dc-klg");
        NKikimrConfig::TAppConfig appConfig = TransformConfig(args, GetEnvMock(/*hostname*/"klg-000-localhost", /*fqdnHostName*/"klg-000-localhost"));

        UNIT_ASSERT(appConfig.HasGRpcConfig());
        UNIT_ASSERT(!appConfig.GetGRpcConfig().HasXdsBootstrap());
    }

    Y_UNIT_TEST(CanUseNodeIdFromYamlConfig) {
      TString tmpConfig(config);
      tmpConfig.append(R"(
  grpc_config:
    xds_bootstrap:
      xds_servers:
      - server_uri: "xds-provider.bootstrap.cloud-testing.yandex.net:18000"
        server_features:
        - "xds_v3"
        channel_creds:
        - type: "insecure"
          config: {"k1": "v1", "k2": "v2"}
      node:
        id: "test-node-id"
        cluster: "testing"
        meta: {"service": "ydb"}
        #locality:
          #zone: Try set zone as data center of ydb node instance
)");
        TTempFileHandle configFile = CreateConfigFile(tmpConfig);
        TVector<TString> args;
        PreFillArgs(args, configFile.Name());
        args.push_back("--data-center");
        args.push_back("dc-klg");
        NKikimrConfig::TAppConfig appConfig = TransformConfig(args, GetEnvMock(/*hostname*/"klg-000-localhost", /*fqdnHostName*/"klg-000-localhost"));

        UNIT_ASSERT(appConfig.HasGRpcConfig());
        UNIT_ASSERT(appConfig.GetGRpcConfig().HasXdsBootstrap());
        const auto& xdsBootstrap = appConfig.GetGRpcConfig().GetXdsBootstrap();
        UNIT_ASSERT_EQUAL(xdsBootstrap.GetNode().GetId(), "test-node-id");
        UNIT_ASSERT_EQUAL(xdsBootstrap.GetNode().GetLocality().GetZone(), "dc-klg");
    }
}
