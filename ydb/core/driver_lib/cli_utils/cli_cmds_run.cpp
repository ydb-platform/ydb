#include "cli_cmds_standalone.h"

#include <ydb/core/driver_lib/run/config.h>
#include <ydb/core/driver_lib/run/config_parser.h>
#include <ydb/core/driver_lib/run/run.h>

namespace NKikimr::NDriverClient {

class TCommandRun : public NYdb::NConsoleClient::TClientCommand {
    std::shared_ptr<TModuleFactories> Factories;

public:
    TCommandRun(std::shared_ptr<TModuleFactories> factories)
        : TClientCommand("run", {}, "Run YDB node (legacy)")
        , Factories(std::move(factories))
    {}

    void Config(TConfig& config) override {
        TClientCommand::Config(config);
        config.Opts->GetOpts().AllowUnknownCharOptions_ = true;
        config.Opts->GetOpts().AllowUnknownLongOptions_ = true;
        config.SetFreeArgsMin(0);
        config.SetFreeArgsMax(static_cast<size_t>(-1));
    }

    int Run(TConfig& config) override {
        // Re-parse from original argv — reproduces the exact old main.cpp flow:
        // 1. SetupGlobalOpts parses --cluster-name etc. from before "run"
        // 2. ParseRunOpts parses --node etc. from after "run"
        using namespace NLastGetopt;

        NKikimrConfig::TAppConfig appConfig;
        TKikimrRunConfig runConfig(appConfig);
        TRunCommandConfigParser configParser(runConfig);

        TOpts opts;
        configParser.SetupGlobalOpts(opts);
        opts.AllowUnknownCharOptions_ = true;
        opts.AllowUnknownLongOptions_ = true;
        opts.SetFreeArgsMin(0);
        opts.ArgPermutation_ = REQUIRE_ORDER;

        TOptsParseResult res(&opts, config.InitialArgC, config.InitialArgV);
        size_t freeArgsPos = res.GetFreeArgsPos();

        configParser.ParseGlobalOpts(res);
        configParser.ParseRunOpts(
            config.InitialArgC - freeArgsPos,
            config.InitialArgV + freeArgsPos);
        configParser.ApplyParsedOptions();

        return MainRun(runConfig, std::move(Factories));
    }
};

std::unique_ptr<NYdb::NConsoleClient::TClientCommand> NewCommandRun(std::shared_ptr<TModuleFactories> factories) {
    return std::make_unique<TCommandRun>(std::move(factories));
}

} // namespace NKikimr::NDriverClient
