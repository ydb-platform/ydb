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
    , InitCfg({
        *ErrorCollector,
        *ProtoConfigFileProvider,
        *ConfigUpdateTracer,
        *MemLogInit,
        *NodeBrokerClient,
        *DynConfigClient,
        *Env,
        *Logger
    })
{}

int TClientCommandServer::Run(TConfig& /*config*/) {
    NKikimrConfig::TAppConfig appConfig;

    TKikimrRunConfig RunConfig(appConfig);

    InitCfg.Apply(
        appConfig,
        RunConfig.NodeId,
        RunConfig.ScopeId,
        RunConfig.TenantName,
        RunConfig.ServicesMask,
        RunConfig.Labels,
        RunConfig.ClusterName,
        RunConfig.InitialCmsConfig,
        RunConfig.InitialCmsYamlConfig,
        RunConfig.ConfigInitInfo);

    Y_ABORT_UNLESS(RunConfig.NodeId);
    return MainRun(RunConfig, Factories);
}

void TClientCommandServer::Config(TConfig& config) {
    TClientCommand::Config(config);

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
