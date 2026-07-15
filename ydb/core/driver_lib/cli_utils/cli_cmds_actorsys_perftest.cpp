#include "cli.h"
#include "cli_cmds_standalone.h"

namespace NKikimr::NDriverClient {

class TCommandActorsysPerfTest : public NYdb::NConsoleClient::TClientCommand {
public:
    TCommandActorsysPerfTest()
        : TClientCommand("actorsys-perf-test", {}, "Make actorsystem performance test")
    {}

    void Config(TConfig& config) override {
        TClientCommand::Config(config);
        config.Opts->GetOpts().AllowUnknownCharOptions_ = true;
        config.Opts->GetOpts().AllowUnknownLongOptions_ = true;
        config.SetFreeArgsMin(0);
        config.SetFreeArgsMax(static_cast<size_t>(-1));
    }

    int Run(TConfig& config) override {
        TCommandConfig cmdConf;
        return ActorsysPerfTest(cmdConf, config.ArgC, config.ArgV);
    }
};

std::unique_ptr<NYdb::NConsoleClient::TClientCommand> NewCommandActorsysPerfTest() {
    return std::make_unique<TCommandActorsysPerfTest>();
}

} // namespace NKikimr::NDriverClient
