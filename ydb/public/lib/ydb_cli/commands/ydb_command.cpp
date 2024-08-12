#include "ydb_command.h"
#include "ydb_common.h"

namespace NYdb {
namespace NConsoleClient {

TYdbCommand::TYdbCommand(const TString& name, const std::initializer_list<TString>& aliases, const TString& description)
    :TClientCommand(name, aliases, description)
{}

TDriverConfig TYdbCommand::CreateDriverConfig(const TConfig& config) {
    auto driverConfig = TDriverConfig()
        .SetEndpoint(config.Address)
        .SetDatabase(config.Database)
        .SetCredentialsProviderFactory(config.CredentialsGetter(config))        ;

    if (config.EnableSsl)
        driverConfig.UseSecureConnection(config.CaCerts);
    if (config.IsNetworkIntensive)
        driverConfig.SetNetworkThreadsNum(16);

    return driverConfig;
}

TDriver TYdbCommand::CreateDriver(const TConfig& config) {
    return TDriver(CreateDriverConfig(config));
}

TDriver TYdbCommand::CreateDriver(const TConfig& config, THolder<TLogBackend>&& loggingBackend) {
    auto driverConfig = CreateDriverConfig(config);
    driverConfig.SetLog(std::move(loggingBackend));

    return TDriver(driverConfig);
}

TYdbSimpleCommand::TYdbSimpleCommand(const TString& name, const std::initializer_list<TString>& aliases, const TString& description)
    :TYdbCommand(name, aliases, description)
{}

void TYdbSimpleCommand::Config(TConfig& config) {
    TClientCommand::Config(config);

    NLastGetopt::TOpts& opts = *config.Opts;
    opts.AddLongOption("timeout", "Client timeout. There is no point waiting for the result after this long.")
        .RequiredArgument("ms").StoreResult(&ClientTimeout);
}

TYdbOperationCommand::TYdbOperationCommand(const TString& name, const std::initializer_list<TString>& aliases, const TString& description)
    :TYdbCommand(name, aliases, description)
{}

void TYdbOperationCommand::Config(TConfig& config) {
    TYdbCommand::Config(config);

    NLastGetopt::TOpts& opts = *config.Opts;
    opts.AddLongOption("timeout", "Operation timeout. Operation should be executed on server within this timeout. "
            "There could also be a delay up to 200ms to receive timeout error from server.")
        .RequiredArgument("ms").StoreResult(&OperationTimeout);
}

NScripting::TExplainYqlResult TYdbOperationCommand::ExplainQuery(TClientCommand::TConfig& config, const TString& queryText,
        NScripting::ExplainYqlRequestMode mode) {
    NScripting::TScriptingClient client(CreateDriver(config));

    NScripting::TExplainYqlRequestSettings explainSettings;
    explainSettings.Mode(mode);

    auto result = client.ExplainYqlScript(
        queryText,
        explainSettings
    ).GetValueSync();
    ThrowOnError(result);
    return result;
}

}
}
