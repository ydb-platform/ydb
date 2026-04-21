#include "cli.h"
#include "cli_cmds_standalone.h"

namespace NKikimr::NDriverClient {

class TCommandSchemeInitRoot : public NYdb::NConsoleClient::TClientCommand {
public:
    TCommandSchemeInitRoot()
        : TClientCommand("scheme-initroot", {}, "Init scheme root")
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
        return SchemeInitRoot(cmdConf, config.ArgC, config.ArgV);
    }
};

std::unique_ptr<NYdb::NConsoleClient::TClientCommand> NewCommandSchemeInitRoot() {
    return std::make_unique<TCommandSchemeInitRoot>();
}

} // namespace NKikimr::NDriverClient
