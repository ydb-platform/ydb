#include "cli_cmds.h"

#include <ydb/core/config/init/init.h>
#include <ydb/core/config/validation/validators.h>

namespace NKikimr::NDriverClient {

NKikimrConfig::TAppConfig TransformConfig(const std::vector<TString>& args) {
    auto errorCollector = NConfig::MakeDefaultErrorCollector();
    auto protoConfigFileProvider = NConfig::MakeDefaultProtoConfigFileProvider();
    auto configUpdateTracer = NConfig::MakeDefaultConfigUpdateTracer();
    auto memLogInit = NConfig::MakeNoopMemLogInitializer();
    auto nodeBrokerClient = NConfig::MakeNoopNodeBrokerClient();
    auto dynConfigClient = NConfig::MakeNoopDynConfigClient();
    auto env = NConfig::MakeDefaultEnv();
    auto logger = NConfig::MakeNoopInitLogger();

    NConfig::TInitialConfiguratorDependencies deps{
        *errorCollector,
        *protoConfigFileProvider,
        *configUpdateTracer,
        *memLogInit,
        *nodeBrokerClient,
        *dynConfigClient,
        *env,
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
    initCfg->Parse(parseResult.GetFreeArgs());

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

void PreFillArgs(std::vector<TString>& args) {
    args.push_back("server");

    args.push_back("--node");
    args.push_back("1");

    args.push_back("--grpc-port");
    args.push_back("2135");

    args.push_back("--grpc-port");
    args.push_back("9001");

    args.push_back("--mon-port");
    args.push_back("8765");
}

int ValidateConfigs(const std::vector<TString>& argsCurrent, const std::vector<TString>& argsCandidate) {
    auto currentConfig = TransformConfig(argsCurrent);
    auto candidateConfig = TransformConfig(argsCandidate);

    std::vector<TString> msgs;
    auto res = NConfig::ValidateStaticGroup(currentConfig, candidateConfig, msgs);

    if (res == NConfig::EValidationResult::Warn) {
        for (const auto& msg : msgs) {
            Cerr << "Warning: " << msg << Endl;
        }

        return 2;
    }

    if (res == NConfig::EValidationResult::Error) {
        for (const auto& msg : msgs) {
            Cerr << "Error: " << msg << Endl;
        }

        return 1;
    }

    Cerr << "OK" << Endl;

    return 0;
}

class TClientCommandValidateConfig : public TClientCommand {
public:
    TClientCommandValidateConfig()
        : TClientCommand("validate", {}, "Config validation utils")
    {}

    void Config(TConfig& config) override {
        TClientCommand::Config(config);

        config.Opts->AddLongOption("current", "Current config")
            .Required()
            .RequiredArgument("PATH")
            .StoreResult(&CurrentConfig);

        config.Opts->AddLongOption("candidate", "Candidate config")
            .Required()
            .RequiredArgument("PATH")
            .StoreResult(&CandidateConfig);
    }

    int Run(TConfig& /*config*/) override {
        std::vector<TString> argsCurrent;
        PreFillArgs(argsCurrent);
        argsCurrent.push_back("--yaml-config");
        argsCurrent.push_back(CurrentConfig);

        std::vector<TString> argsCandidate;
        PreFillArgs(argsCandidate);
        argsCandidate.push_back("--yaml-config");
        argsCandidate.push_back(CandidateConfig);

        return ValidateConfigs(argsCurrent, argsCandidate);
    }
private:
    TString CurrentConfig;
    TString CandidateConfig;
};

TClientCommandConfig::TClientCommandConfig()
    : TClientCommandTree("config", {}, "config utils")
{
    AddCommand(std::make_unique<TClientCommandValidateConfig>());
}

} // NKikimr::NDriverClient
