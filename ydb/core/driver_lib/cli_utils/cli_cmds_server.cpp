#include "cli.h"
#include "cli_cmds.h"

#include <ydb/core/driver_lib/run/run.h>
#include <ydb/core/config/init/init.h>

#include <memory>

namespace NKikimr::NDriverClient {

class TClientCommandServer : public TClientCommand {
public:
    TClientCommandServer(std::shared_ptr<TModuleFactories> factories)
        : TClientCommand("server", {}, "Execute YDB server")
        , Factories(std::move(factories))
        , ErrorCollector(NConfig::MakeDefaultErrorCollector())
        , ProtoConfigFileProvider(NConfig::MakeDefaultProtoConfigFileProvider())
        , ConfigUpdateTracer(NConfig::MakeDefaultConfigUpdateTracer())
        , MemLogInit(NConfig::MakeDefaultMemLogInitializer())
        , Env(NConfig::MakeDefaultEnv())
        , InitCfg(*ErrorCollector, *ProtoConfigFileProvider, *ConfigUpdateTracer, *MemLogInit, *Env)
    {}

    int Run(TConfig &/*config*/) override {
        NKikimrConfig::TAppConfig appConfig;

        TKikimrRunConfig RunConfig(appConfig);
        // FIXME: fill from InitCfg
        Y_ABORT_UNLESS(RunConfig.NodeId);
        return MainRun(RunConfig, Factories);
    }
protected:
    std::shared_ptr<TModuleFactories> Factories;

    std::unique_ptr<NConfig::IErrorCollector> ErrorCollector;
    std::unique_ptr<NConfig::IProtoConfigFileProvider> ProtoConfigFileProvider;
    std::unique_ptr<NConfig::IConfigUpdateTracer> ConfigUpdateTracer;
    std::unique_ptr<NConfig::IMemLogInitializer> MemLogInit;
    std::unique_ptr<NConfig::IEnv> Env;

    NConfig::TInitialConfigurator InitCfg;

    void Config(TConfig& config) override {
        TClientCommand::Config(config);

        InitCfg.RegisterCliOptions(*config.Opts);
        ProtoConfigFileProvider->RegisterCliOptions(*config.Opts);
        config.SetFreeArgsMin(0);

        config.Opts->AddHelpOption('h');
    }

    void Parse(TConfig& config) override {
        TClientCommand::Parse(config);
        InitCfg.ValidateOptions(*config.Opts, *config.ParseResult);
        InitCfg.Parse(config.ParseResult->GetFreeArgs());
    }
};

void AddClientCommandServer(TClientCommandTree& parent, std::shared_ptr<TModuleFactories> factories) {
    parent.AddCommand(std::make_unique<TClientCommandServer>(factories));
}

} // namespace NKikimr::NDriverClient
