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


Y_UNIT_TEST_SUITE(StaticNodeSelectorsInit) {

    TTempFileHandle CreateConfigFile() {
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
      port: 2135

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
        args.push_back("2135");

        args.push_back("--grpc-port");
        args.push_back("9001");

        args.push_back("--mon-port");
        args.push_back("8765");

        args.push_back("--yaml-config");
        args.push_back(configPath);
    }

    NKikimrConfig::TAppConfig TransformConfig(const std::vector<TString>& args) {
        auto errorCollector = NConfig::MakeDefaultErrorCollector();
        auto protoConfigFileProvider = NConfig::MakeDefaultProtoConfigFileProvider();
        auto configUpdateTracer = NConfig::MakeDefaultConfigUpdateTracer();
        auto memLogInit = NConfig::MakeNoopMemLogInitializer();
        auto nodeBrokerClient = NConfig::MakeNoopNodeBrokerClient();
        auto dynConfigClient = NConfig::MakeNoopDynConfigClient();
        auto configClient = NConfig::MakeNoopConfigClient();
        auto logger = NConfig::MakeNoopInitLogger();

        auto envMock = std::make_unique<NConfig::TEnvMock>();
        envMock->SavedHostName = "localhost";
        envMock->SavedFQDNHostName = "localhost";

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
        TString clusterName;
        NConfig::TConfigsDispatcherInitInfo configsDispatcherInitInfo;

        initCfg->Apply(
            appConfig,
            nodeId,
            scopeId,
            tenantName,
            servicesMask,
            clusterName,
            configsDispatcherInitInfo);

        return appConfig;
    }

    Y_UNIT_TEST(TestStaticNodeSelectorForActorSystem) {
        TTempFileHandle configFile = CreateConfigFile();
        TVector<TString> args;
        PreFillArgs(args, configFile.Name());
        args.push_back("--label");
        args.push_back("test=abc");
        NKikimrConfig::TAppConfig appConfig = TransformConfig(args);

        UNIT_ASSERT(appConfig.HasActorSystemConfig());
        const auto& actorConfig = appConfig.GetActorSystemConfig();
        UNIT_ASSERT_EQUAL(actorConfig.GetCpuCount(), 100);
    }

    Y_UNIT_TEST(TestStaticNodeSelectorWithAnotherLabel) {
        TTempFileHandle configFile = CreateConfigFile();
        TVector<TString> args;
        PreFillArgs(args, configFile.Name());
        args.push_back("--label");
        args.push_back("test=abd");
        NKikimrConfig::TAppConfig appConfig = TransformConfig(args);

        UNIT_ASSERT(appConfig.HasActorSystemConfig());
        const auto& actorConfig = appConfig.GetActorSystemConfig();
        UNIT_ASSERT_VALUES_EQUAL(actorConfig.GetCpuCount(), 10);
        
    }

    Y_UNIT_TEST(TestStaticNodeSelectorInheritance) {
        TTempFileHandle configFile = CreateConfigFile();
        TVector<TString> args;
        PreFillArgs(args, configFile.Name());
        args.push_back("--label");
        args.push_back("test=abc");
        NKikimrConfig::TAppConfig appConfig = TransformConfig(args);

        UNIT_ASSERT(appConfig.HasLogConfig());
        const auto& logConfig = appConfig.GetLogConfig();
        UNIT_ASSERT_EQUAL(logConfig.GetDefaultLevel(), 5);
        UNIT_ASSERT_EQUAL(logConfig.GetEntry(0).GetLevel(), 7);
        UNIT_ASSERT_EQUAL(logConfig.GetEntry(1).GetLevel(), 6);
    }

    Y_UNIT_TEST(TestStaticNodeSelectorByNodeId) {
        TTempFileHandle configFile = CreateConfigFile();
        TVector<TString> args;
        PreFillArgs(args, configFile.Name());
        args.push_back("--label");
        args.push_back("test=node_id");
        NKikimrConfig::TAppConfig appConfig = TransformConfig(args);

        UNIT_ASSERT(appConfig.HasActorSystemConfig());
        const auto& actorConfig = appConfig.GetActorSystemConfig();
        UNIT_ASSERT_EQUAL(actorConfig.GetCpuCount(), 1);
    }

    Y_UNIT_TEST(TestStaticNodeSelectorByNodeHost) {
        TTempFileHandle configFile = CreateConfigFile();
        TVector<TString> args;
        PreFillArgs(args, configFile.Name());
        args.push_back("--label");
        args.push_back("test=node_host");

        NKikimrConfig::TAppConfig appConfig = TransformConfig(args);

        UNIT_ASSERT(appConfig.HasActorSystemConfig());
        const auto& actorConfig = appConfig.GetActorSystemConfig();
        UNIT_ASSERT_EQUAL(actorConfig.GetCpuCount(), 50);
    }

    Y_UNIT_TEST(TestStaticNodeSelectorByNodeKind) {
        TTempFileHandle configFile = CreateConfigFile();
        TVector<TString> args;
        PreFillArgs(args, configFile.Name());
        args.push_back("--label");
        args.push_back("test=node_kind");
        NKikimrConfig::TAppConfig appConfig = TransformConfig(args);

        UNIT_ASSERT(appConfig.HasActorSystemConfig());
        const auto& actorConfig = appConfig.GetActorSystemConfig();
        UNIT_ASSERT_EQUAL(actorConfig.GetCpuCount(), 42);
    }
}
