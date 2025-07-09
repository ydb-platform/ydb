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
    , ConfigClient(NConfig::MakeDefaultConfigClient())
    , Env(NConfig::MakeDefaultEnv())
    , Logger(NConfig::MakeDefaultInitLogger())
    , DepsRecorder(NConfig::MakeDefaultInitialConfiguratorDepsRecorder({
        *ErrorCollector,
        *ProtoConfigFileProvider,
        *ConfigUpdateTracer,
        *MemLogInit,
        *NodeBrokerClient,
        *DynConfigClient,
        *ConfigClient,
        *Env,
        *Logger
    }))
    , InitCfg(DepsRecorder->GetDeps())
    , RunConfig(AppConfig)
{}

int TClientCommandServer::Run(TConfig& config) {
    InitCfg.Apply(
        AppConfig,
        RunConfig.NodeId,
        RunConfig.ScopeId,
        RunConfig.TenantName,
        RunConfig.ServicesMask,
        RunConfig.ClusterName,
        RunConfig.ConfigsDispatcherInitInfo);

    RunConfig.ConfigsDispatcherInitInfo.RecordedInitialConfiguratorDeps =
        std::make_shared<NConfig::TRecordedInitialConfiguratorDeps>(DepsRecorder->GetRecordedDeps());

    for (int i = 0; i < config.ArgC; ++i) {
        RunConfig.ConfigsDispatcherInitInfo.Args.push_back(config.ArgV[i]);
    }

    Y_ABORT_UNLESS(RunConfig.NodeId);
    return MainRun(RunConfig, Factories);
}

void TClientCommandServer::Config(TConfig& config) {
    TClientCommand::Config(config);
    for (auto plugin: RunConfig.Plugins) {
        plugin->SetupOpts(config.Opts->GetOpts());
    }
    NConfig::AddProtoConfigOptions(DepsRecorder->GetDeps().ProtoConfigFileProvider);
    InitCfg.RegisterCliOptions(config.Opts->GetOpts());
    ProtoConfigFileProvider->RegisterCliOptions(config.Opts->GetOpts());
    config.SetFreeArgsMin(0);

    config.Opts->GetOpts().AddHelpOption('h');
}

void TClientCommandServer::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    InitCfg.ValidateOptions(config.Opts->GetOpts(), config.ParseResult->GetCommandLineParseResult());
    InitCfg.Parse(config.ParseResult->GetFreeArgs(), Factories->ConfigSwissKnife.get());
}

void AddClientCommandServer(TClientCommandTree& parent, std::shared_ptr<TModuleFactories> factories) {
    parent.AddCommand(std::make_unique<TClientCommandServer>(factories));
}

} // namespace NKikimr::NDriverClient
