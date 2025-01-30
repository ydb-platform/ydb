#include "cli.h"
#include "cli_cmds.h"
#include <ydb/public/lib/ydb_cli/commands/ydb_service_discovery.h> // for NConsoleClient::TCommandWhoAmI
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

    // Static credentials
    TStringBuilder userHelp;
    userHelp << "User name to authenticate with" << Endl
        << "  User name search order:" << Endl
        << "    1. This option" << Endl
        << "    2. \"YDB_USER\" environment variable" << Endl;
    opts.AddLongOption("user", userHelp).RequiredArgument("STR").StoreResult(&UserName);

    TStringBuilder passwordHelp;
    passwordHelp << "File with password to authenticate with" << Endl
        << "  Password search order:" << Endl
        << "    1. This option" << Endl
        << "    2. \"YDB_PASSWORD\" environment variable" << Endl;
    opts.AddLongOption("password-file", passwordHelp).RequiredArgument("PATH").StoreResult(&PasswordFile);

    opts.AddLongOption("no-password", "Do not ask for user password (if empty)").Optional().StoreTrue(&DoNotAskForPassword);

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

void TClientCommandRootKikimrBase::ParseCredentials(TConfig& config) {
    if (!Token.empty()) {
        config.SecurityToken = Token;
        return;
    }
    // 1. command line options
    if (TokenFile) {
        if (UserName) {
            throw TMisuseException() << "Both TokenFile and User options are used. Use only one of them";
        }
        Token = ReadFromFile(TokenFile, "token");
        config.SecurityToken = Token;
        return;
    }
    if (UserName) {
        config.StaticCredentials.User = UserName;
        if (PasswordFile) {
            config.StaticCredentials.Password = ReadFromFile(PasswordFile, "password", true);
        } else if (!DoNotAskForPassword) {
            Cerr << "Enter password for user " << UserName << ": ";
            config.StaticCredentials.Password = InputPassword();
        }
        return;
    } else if (PasswordFile) {
        throw TMisuseException() << "PasswordFile option used without User option";
    }

    // 2. Environment variables
    TString ydbToken = GetEnv("YDB_TOKEN");
    if (!ydbToken.empty()) {
        Token = ydbToken;
        config.SecurityToken = Token;
        return;
    }

    TString envUser = GetEnv("YDB_USER");
    if (!envUser.empty()) {
        config.StaticCredentials.User = envUser;
        TString envPassword = GetEnv("YDB_PASSWORD");
        if (!envPassword.empty()) {
            config.StaticCredentials.Password = envPassword;
        } else if (!DoNotAskForPassword) {
            Cerr << "Enter password for user " << envUser << ": ";
            config.StaticCredentials.Password = InputPassword();
        }
        return;
    }

    // 3. Default token file
    TokenFile = defaultTokenFile;
    ReadFromFileIfExists(TokenFile, "default token", Token);
    config.SecurityToken = Token;
}

class TClientCommandRootLite : public TClientCommandRootKikimrBase {
public:
    TClientCommandRootLite()
        : TClientCommandRootKikimrBase("ydb")
    {
        AddCommand(std::make_unique<TClientCommandSchemaLite>());
        AddCommand(std::make_unique<TCommandWhoAmI>());
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
        TString message;
        if (!ParseProtocol(config, message)) {
            throw TMisuseException() << message;
        }
        ParseCaCerts(config);
        config.Address = Address;

        if (!hostname) {
            NMsgBusProxy::TMsgBusClientConfig::CrackAddress(Address, hostname, port);
        }
        CommandConfig.ClientConfig = NYdbGrpc::TGRpcClientConfig(hostname + ':' + ToString(port));
        if (config.EnableSsl) {
            CommandConfig.ClientConfig.EnableSsl = config.EnableSsl;
            CommandConfig.ClientConfig.SslCredentials.pem_root_certs = config.CaCerts;
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
