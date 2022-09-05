#include "cli.h"
#include "cli_cmds.h"
#include <util/folder/path.h>
#include <util/folder/dirut.h>
#include <util/string/strip.h>
#include <util/system/env.h>

namespace NKikimr {
namespace NDriverClient {

const TString defaultProfileFile = "~/.ydb/default_profile";

TClientCommandRootKikimrBase::TClientCommandRootKikimrBase(const TString& name)
    : TClientCommandRootBase(name)
{
}

void TClientCommandRootKikimrBase::Config(TConfig& config) {
    TClientCommandRootBase::Config(config);
    NLastGetopt::TOpts& opts = *config.Opts;
    opts.AddLongOption('d', "dump", "Dump requests to error log").NoArgument().Hidden().SetFlag(&DumpRequests);

    TStringBuilder tokenHelp;
    tokenHelp << "Security token file" << Endl
        << "  Token search order:" << Endl
        << "    1. This option" << Endl
        << "    2. \"YDB_TOKEN\" environment variable" << Endl
        << "    3. Connection settings profile (\"~/.ydb/ProfileName/token\")" << Endl
        << "    4. Default token file \"" << defaultTokenFile << "\" file";
    opts.AddLongOption('f', "token-file", tokenHelp).RequiredArgument("PATH").StoreResult(&TokenFile);
    TStringBuilder profileHelp;
    profileHelp << "Connection settings profile name" << Endl
        << "  Profile search order:" << Endl
        << "    1. This option" << Endl
        << "    2. \"YDB_PROFILE\" environment variable" << Endl
        << "    3. Default profile file \"" << defaultProfileFile << "\" file";
    opts.AddLongOption("profile", profileHelp).RequiredArgument("NAME").StoreResult(&LocalProfileName);

    TStringStream stream;
    NColorizer::TColors colors = NColorizer::AutoColors(Cout);
    stream << " -s <[protocol://]host[:port]> [options] <subcommand>" << Endl << Endl
        << colors.BoldColor() << "Subcommands" << colors.OldColor() << ":" << Endl;
    RenderCommandsDescription(stream, colors);
    opts.SetCmdLineDescr(stream.Str());
}

void TClientCommandRootKikimrBase::Parse(TConfig& config) {
    ParseProfile();
    GetProfileVariable("path", config.Path);
    TClientCommandRootBase::Parse(config);
    NClient::TKikimr::DUMP_REQUESTS = DumpRequests;
}

void TClientCommandRootKikimrBase::ParseProfile() {
    if (LocalProfileName.empty()) {
        LocalProfileName = GetEnv("YDB_PROFILE");

        if (LocalProfileName.empty()) {
            TString profileFile = defaultProfileFile;
            ReadFromFileIfExists(profileFile, "default profile", LocalProfileName);
        }
    }

    if (LocalProfileName) {
        ProfileConfig = MakeHolder<TProfileConfig>(LocalProfileName);
    }
}

bool TClientCommandRootKikimrBase::GetProfileVariable(const TString& name, TString& value) {
    if (!ProfileConfig) {
        return false;
    }
    return ProfileConfig->GetVariable(name, value);
}

class TClientCommandRootLite : public TClientCommandRootKikimrBase {
public:
    TClientCommandRootLite()
        : TClientCommandRootKikimrBase("ydb")
    {
        AddCommand(std::make_unique<TClientCommandSchemaLite>());
        AddCommand(std::make_unique<TClientCommandWhoAmI>());
        AddCommand(std::make_unique<TClientCommandDiscoveryLite>());
    }

    void Config(TConfig& config) override {
        config.Opts->AddLongOption('s', "server", "[Required] server address to connect")
            .RequiredArgument("[PROTOCOL://]HOST[:PORT]").StoreResult(&Address);
        TClientCommandRootKikimrBase::Config(config);
    }

    void Parse(TConfig& config) override {
        TClientCommandRootBase::Parse(config);
        NClient::TKikimr::DUMP_REQUESTS = DumpRequests;
    }

    void ParseAddress(TConfig& config) override {
        TString hostname;
        ui32 port = 2135;
        if (Address.empty()) {
            if (GetProfileVariable("host", hostname)) {
                TString portStr;
                if (GetProfileVariable("port", portStr)) {
                    port = FromString<ui32>(portStr);
                }
            } else {
                return;
            }
        }
        ParseProtocol(config);
        ParseCaCerts(config);
        config.Address = Address;

        if (!hostname) {
            NMsgBusProxy::TMsgBusClientConfig::CrackAddress(Address, hostname, port);
        }
        CommandConfig.ClientConfig = NGrpc::TGRpcClientConfig(hostname + ':' + ToString(port));
        if (config.EnableSsl) {
            auto *p = std::get_if<NGrpc::TGRpcClientConfig>(&CommandConfig.ClientConfig.GetRef());
            p->EnableSsl = config.EnableSsl;
            p->SslCaCert = config.CaCerts;
        }
    }

    void Validate(TConfig& config) override {
        TClientCommandRootBase::Validate(config);

        if (Address.empty() && config.NeedToConnect) {
            throw TMisuseException()
                << "Missing required option 'server'. Also couldn't find 'host' variable in profile config.";
        }
    }

private:
    bool DumpRequests = false;
};

int NewLiteClient(int argc, char** argv) {
    THolder<TClientCommandRootLite> commandsRoot = MakeHolder<TClientCommandRootLite>();
    commandsRoot->Opts.SetTitle("YDB client");
    TClientCommand::TConfig config(argc, argv);
    return commandsRoot->Process(config);
}

}
}
