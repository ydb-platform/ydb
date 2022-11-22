#include "cli.h"
#include "cli_cmds.h"
#include <ydb/core/driver_lib/run/factories.h>
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
        AddCommand(std::make_unique<TClientCommandAdmin>());
        AddCommand(std::make_unique<TClientCommandDb>());
        AddCommand(std::make_unique<TClientCommandCms>());
        AddCommand(std::make_unique<TClientCommandWhoAmI>());
        AddCommand(std::make_unique<TClientCommandDiscovery>());
        AddClientCommandServer(*this, std::move(factories));
    }

    void Config(TConfig& config) override {
        NLastGetopt::TOpts& opts = *config.Opts;
        MsgBusClientConfig.ConfigureLastGetopt(opts, "mb-");
        HideOptions(*config.Opts);
        opts.AddLongOption('k', "token", "security token").RequiredArgument("TOKEN").StoreResult(&Token);
        opts.AddLongOption('s', "server", "server address to connect")
            .RequiredArgument("HOST[:PORT]").StoreResult(&Address);
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

        switch (endpoint.ServerType) {
        case TCommandConfig::EServerType::GRpc:
            CommandConfig.ClientConfig = NGrpc::TGRpcClientConfig(endpoint.Address);
            if (config.EnableSsl) {
                CommandConfig.ClientConfig.EnableSsl = config.EnableSsl;
                CommandConfig.ClientConfig.SslCredentials.pem_root_certs = config.CaCerts;
            }
            break;
        case TCommandConfig::EServerType::MessageBus:
            Y_FAIL("MessageBus is no longer supported");
            break;
        }
    }

private:
    NMsgBusProxy::TMsgBusClientConfig MsgBusClientConfig;
};

int NewClient(int argc, char** argv, std::shared_ptr<TModuleFactories> factories) {
    auto commandsRoot = MakeHolder<TClientCommandRoot>(std::filesystem::path(argv[0]).stem().string(), std::move(factories));
    TClientCommand::TConfig config(argc, argv);
    // TODO: process flags from environment KIKIMR_FLAGS before command line processing
    return commandsRoot->Process(config);
}

TString NewClientCommandsDescription(const TString& name, std::shared_ptr<TModuleFactories> factories) {
    THolder<TClientCommandRoot> commandsRoot = MakeHolder<TClientCommandRoot>(name, std::move(factories));
    TStringStream stream;
    NColorizer::TColors colors = NColorizer::AutoColors(Cout);
    stream << " [options] <subcommand>" << Endl << Endl
        << colors.BoldColor() << "Subcommands" << colors.OldColor() << ":" << Endl;
    commandsRoot->RenderCommandsDescription(stream, colors);
    return stream.Str();
}

}
}
