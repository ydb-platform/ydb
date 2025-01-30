#include "cli_cmds_server.h"

namespace NKikimr::NDriverClient {

TClientCommandServer::TClientCommandServer(std::shared_ptr<TModuleFactories> factories)
    : TClientCommand("server", {}, "Execute YDB server")
    , Factories(std::move(factories))
    , ErrorCollector(NConfig::MakeDefaultErrorCollector())
    , ProtoConfigFileProvider(NConfig::MakeDefaultProtoConfigFileProvider())
    , ConfigUpdateTracer(NConfig::MakeDefaultConfigUpdateTracer())
    , MemLogInit(NConfig::MakeDefaultMemLogInitializer())
    , NodeBrokerClient(NConfig::MakeDefaultNodeBrokerClient())
    , DynConfigClient(NConfig::MakeDefaultDynConfigClient())
    , Env(NConfig::MakeDefaultEnv())
    , Logger(NConfig::MakeDefaultInitLogger())
    , DepsRecorder(NConfig::MakeDefaultInitialConfiguratorDepsRecorder({
        *ErrorCollector,
        *ProtoConfigFileProvider,
        *ConfigUpdateTracer,
        *MemLogInit,
        *NodeBrokerClient,
        *DynConfigClient,
        *Env,
        *Logger
    }))
    , InitCfg(DepsRecorder->GetDeps())
{}

int TClientCommandServer::Run(TConfig& config) {
    NKikimrConfig::TAppConfig appConfig;

    TKikimrRunConfig runConfig(appConfig);

    InitCfg.Apply(
        appConfig,
        runConfig.NodeId,
        runConfig.ScopeId,
        runConfig.TenantName,
        runConfig.ServicesMask,
        runConfig.ClusterName,
        runConfig.ConfigsDispatcherInitInfo);

    runConfig.ConfigsDispatcherInitInfo.RecordedInitialConfiguratorDeps =
        std::make_shared<NConfig::TRecordedInitialConfiguratorDeps>(DepsRecorder->GetRecordedDeps());

    for (int i = 0; i < config.ArgC; ++i) {
        runConfig.ConfigsDispatcherInitInfo.Args.push_back(config.ArgV[i]);
    }

    Y_ABORT_UNLESS(runConfig.NodeId);
    return MainRun(runConfig, Factories);
}

void TClientCommandServer::Config(TConfig& config) {
    TClientCommand::Config(config);

    NConfig::AddProtoConfigOptions(DepsRecorder->GetDeps().ProtoConfigFileProvider);
    InitCfg.RegisterCliOptions(*config.Opts);
    ProtoConfigFileProvider->RegisterCliOptions(*config.Opts);
    config.SetFreeArgsMin(0);

    config.Opts->AddHelpOption('h');
}

void TClientCommandServer::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    InitCfg.ValidateOptions(*config.Opts, *config.ParseResult);
    InitCfg.Parse(config.ParseResult->GetFreeArgs());
}

void AddClientCommandServer(TClientCommandTree& parent, std::shared_ptr<TModuleFactories> factories) {
    parent.AddCommand(std::make_unique<TClientCommandServer>(factories));
}

} // namespace NKikimr::NDriverClient
