#include "cli.h"
#include "cli_cmds_standalone.h"

namespace NKikimr::NDriverClient {

class TCommandPersQueueRequest : public NYdb::NConsoleClient::TClientCommand {
public:
    TCommandPersQueueRequest()
        : TClientCommand("persqueue-request", {}, "Send protobuf request to a persqueue tablet")
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
        return PersQueueRequest(cmdConf, config.ArgC, config.ArgV);
    }
};

class TCommandPersQueueStress : public NYdb::NConsoleClient::TClientCommand {
public:
    TCommandPersQueueStress()
        : TClientCommand("persqueue-stress", {}, "Stress read or write to a persqueue tablet")
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
        return PersQueueStress(cmdConf, config.ArgC, config.ArgV);
    }
};

class TCommandPersQueueDiscoverClusters : public NYdb::NConsoleClient::TClientCommand {
public:
    TCommandPersQueueDiscoverClusters()
        : TClientCommand("persqueue-discover-clusters", {}, "PersQueue session clusters discovery")
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
        return PersQueueDiscoverClustersRequest(cmdConf, config.ArgC, config.ArgV);
    }
};

std::unique_ptr<NYdb::NConsoleClient::TClientCommand> NewCommandPersQueueRequest() {
    return std::make_unique<TCommandPersQueueRequest>();
}

std::unique_ptr<NYdb::NConsoleClient::TClientCommand> NewCommandPersQueueStress() {
    return std::make_unique<TCommandPersQueueStress>();
}

std::unique_ptr<NYdb::NConsoleClient::TClientCommand> NewCommandPersQueueDiscoverClusters() {
    return std::make_unique<TCommandPersQueueDiscoverClusters>();
}

} // namespace NKikimr::NDriverClient
