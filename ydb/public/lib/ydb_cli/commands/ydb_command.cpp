#include "ydb_command.h"
#include "ydb_common.h"

#include <ydb/public/lib/ydb_cli/common/interactive.h>

#include <util/system/info.h>

namespace NYdb {
namespace NConsoleClient {

bool TLeafCommand::Prompt(TConfig& config) {
    Y_UNUSED(config);
    if (Dangerous && !config.AssumeYes) {
        return AskPrompt("This command may damage your cluster, do you want to continue?", false);
    }

    return true;
}

TYdbCommand::TYdbCommand(const TString& name, const std::initializer_list<TString>& aliases, const TString& description)
    : TLeafCommand(name, aliases, description)
{}

TDriverConfig TYdbCommand::CreateDriverConfig(TConfig& config) {
    auto driverConfig = TDriverConfig()
        .SetEndpoint(config.Address)
        .SetDatabase(config.Database)
        .SetCredentialsProviderFactory(config.GetSingletonCredentialsProviderFactory())
        .SetUsePerChannelTcpConnection(config.UsePerChannelTcpConnection);

    if (config.EnableSsl) {
        driverConfig.UseSecureConnection(config.CaCerts);
    }

    if (config.IsNetworkIntensive) {
        size_t networkThreadNum = GetNetworkThreadNum(config);
        driverConfig.SetNetworkThreadsNum(networkThreadNum);
    }

    if (config.SkipDiscovery) {
        driverConfig.SetDiscoveryMode(EDiscoveryMode::Off);
    }

    driverConfig.UseClientCertificate(config.ClientCert, config.ClientCertPrivateKey);

    return driverConfig;
}

size_t TYdbCommand::GetNetworkThreadNum(TConfig& config) {
    if (config.IsNetworkIntensive) {
        size_t cpuCount = NSystemInfo::CachedNumberOfCpus();
        if (cpuCount >= 64) {
            // doubtfully there is a reason to have more. Even this is too much.
            return 32;
        } else if (cpuCount >= 32 && cpuCount < 64) {
            // leave the half of CPUs to the client's logic
            return cpuCount / 2;
        } else if (cpuCount >= 16 && cpuCount < 32) {
            // Originally here we had a constant value 16.
            // To not break things this heuristic tries to use this constant as well.
            return 16;
        } else {
            return std::min(size_t(2), cpuCount / 2);
        }
    }
    return 1; // TODO: check default
}

TDriver TYdbCommand::CreateDriver(TConfig& config) {
    return TDriver(CreateDriverConfig(config));
}

TDriver TYdbCommand::CreateDriver(TConfig& config, std::unique_ptr<TLogBackend>&& loggingBackend) {
    auto driverConfig = CreateDriverConfig(config);
    driverConfig.SetLog(std::move(loggingBackend));

    return TDriver(driverConfig);
}

bool TYdbReadOnlyCommand::Prompt(TConfig& config) {
    Y_UNUSED(config);

    return true;
}

TYdbSimpleCommand::TYdbSimpleCommand(const TString& name, const std::initializer_list<TString>& aliases, const TString& description)
    :TYdbCommand(name, aliases, description)
{}

void TYdbSimpleCommand::Config(TConfig& config) {
    TClientCommand::Config(config);

    TClientCommandOptions& opts = *config.Opts;
    opts.AddLongOption("timeout", "Client timeout. There is no point waiting for the result after this long.")
        .RequiredArgument("ms").StoreResult(&ClientTimeout);
}

TYdbOperationCommand::TYdbOperationCommand(const TString& name, const std::initializer_list<TString>& aliases, const TString& description)
    :TYdbCommand(name, aliases, description)
{}

void TYdbOperationCommand::Config(TConfig& config) {
    TYdbCommand::Config(config);

    TClientCommandOptions& opts = *config.Opts;
    opts.AddLongOption("timeout", "Operation timeout. Operation should be executed on server within this timeout. "
            "There could also be a delay up to 200ms to receive timeout error from server.")
        .RequiredArgument("ms").StoreResult(&OperationTimeout);
}

}
}
