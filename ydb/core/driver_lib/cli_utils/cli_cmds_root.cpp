#include "cli.h"
#include "cli_cmds.h"
#include "cli_cmds_standalone.h"
#include <ydb/public/lib/ydb_cli/commands/ydb_service_discovery.h> // for NConsoleClient::TCommandWhoAmI
#include <ydb/core/driver_lib/run/factories.h>
#include <ydb/core/driver_lib/run/main.h>
#include <util/folder/path.h>
#include <util/folder/dirut.h>
#include <util/string/strip.h>
#include <util/system/env.h>

#include <filesystem>

namespace NKikimr {
namespace NDriverClient {

using namespace NYdb::NConsoleClient;

extern void AddClientCommandServer(TClientCommandTree& parent, std::shared_ptr<TModuleFactories> factories);

class TClientCommandRoot : public TClientCommandRootKikimrBase {
public:
    TClientCommandRoot(const TString& name, std::shared_ptr<TModuleFactories> factories)
        : TClientCommandRootKikimrBase(name)
    {
        // Visible commands
        AddCommand(std::make_unique<TClientCommandAdmin>());
        AddCommand(std::make_unique<TClientCommandDb>());
        AddCommand(std::make_unique<TClientCommandCms>());
        AddCommand(std::make_unique<TCommandWhoAmI>());
        AddCommand(std::make_unique<TClientCommandDiscovery>());
        AddClientCommandServer(*this, std::move(factories));
        AddCommand(std::make_unique<TClientCommandConfig>());

        // Hidden commands (shown only in -hh verbose help)
        AddHiddenCommand(NewCommandFormatInfo());
        AddHiddenCommand(NewCommandFormatUtil());
        AddHiddenCommand(NewCommandNodeByHost());
        AddHiddenCommand(NewCommandSchemeInitRoot());
        AddHiddenCommand(NewCommandPersQueueRequest());
        AddHiddenCommand(NewCommandPersQueueStress());
        AddHiddenCommand(NewCommandPersQueueDiscoverClusters());
        AddHiddenCommand(NewCommandActorsysPerfTest());
    }

    void Config(TConfig& config) override {
        TClientCommandOptions& opts = *config.Opts;
        HideOptions(config.Opts->GetOpts());
        opts.AddLongOption('k', "token", "security token").RequiredArgument("TOKEN").StoreResult(&Token);
        opts.AddLongOption('s', "server", "server address to connect")
            .RequiredArgument("HOST[:PORT]").StoreResult(&Address);

        NLastGetopt::TOpts& nOpts = config.Opts->GetOpts();
        nOpts.AddLongOption(0, "allocator-info", "Print the name of allocator linked to the binary and exit")
            .NoArgument().Handler(&PrintAllocatorInfoAndExit);
        nOpts.AddLongOption(0, "compatibility-info", "Print compatibility info of this binary and exit")
            .NoArgument().Handler(&PrintCompatibilityInfoAndExit);

        TClientCommandRootKikimrBase::Config(config);
    }

    void ParseAddress(TConfig& config) override {
        if (Address.empty()) {
            TString ydbServer = GetEnv("YDB_SERVER");
            if (ydbServer == nullptr) {
                ydbServer = GetEnv("KIKIMR_SERVER");
            }
            if (ydbServer != nullptr) {
                Address = ydbServer;
            }
        }
        config.Address = Address;

        TCommandConfig::TServerEndpoint endpoint = TCommandConfig::ParseServerAddress(Address);
        if (endpoint.EnableSsl.Defined()) {
            config.EnableSsl = endpoint.EnableSsl.GetRef();
        }
        ParseCaCerts(config);
        ParseClientCert(config);

        CommandConfig.ClientConfig = NYdbGrpc::TGRpcClientConfig(endpoint.Address);
        if (config.EnableSsl) {
            CommandConfig.ClientConfig.EnableSsl = config.EnableSsl;
            CommandConfig.ClientConfig.SslCredentials.pem_root_certs = config.CaCerts;
            if (config.ClientCert) {
                CommandConfig.ClientConfig.SslCredentials.pem_cert_chain = config.ClientCert;
                CommandConfig.ClientConfig.SslCredentials.pem_private_key = config.ClientCertPrivateKey;
            }
        }
    }
};

int NewClient(int argc, char** argv, std::shared_ptr<TModuleFactories> factories) {
    auto commandsRoot = MakeHolder<TClientCommandRoot>(std::filesystem::path(argv[0]).stem().string(), std::move(factories));
    TClientCommand::TConfig config(argc, argv);
    // TODO: process flags from environment KIKIMR_FLAGS before command line processing
    return commandsRoot->Process(config);
}

}
}
