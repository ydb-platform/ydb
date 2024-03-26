#pragma once

#include "cli.h"
#include "cli_cmds.h"

#include <ydb/core/driver_lib/run/run.h>
#include <ydb/core/config/init/init.h>

#include <memory>

namespace NKikimr::NDriverClient {

class TClientCommandServer : public TClientCommand {
public:
    TClientCommandServer(std::shared_ptr<TModuleFactories> factories);

    int Run(TConfig& config) override;
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;

protected:
    std::shared_ptr<TModuleFactories> Factories;

    std::unique_ptr<NConfig::IErrorCollector> ErrorCollector;
    std::unique_ptr<NConfig::IProtoConfigFileProvider> ProtoConfigFileProvider;
    std::unique_ptr<NConfig::IConfigUpdateTracer> ConfigUpdateTracer;
    std::unique_ptr<NConfig::IMemLogInitializer> MemLogInit;
    std::unique_ptr<NConfig::INodeBrokerClient> NodeBrokerClient;
    std::unique_ptr<NConfig::IDynConfigClient> DynConfigClient;
    std::unique_ptr<NConfig::IEnv> Env;
    std::unique_ptr<NConfig::IInitLogger> Logger;

    std::unique_ptr<NConfig::IInitialConfiguratorDepsRecorder> DepsRecorder;

    NConfig::TInitialConfigurator InitCfg;
};

} // namespace NKikimr::NDriverClient
