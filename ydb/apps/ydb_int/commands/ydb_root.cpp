#include "ydb_root.h"
#include "ydb_service_experimental.h"

#include <filesystem>

namespace NYdb {
namespace NConsoleClient {

namespace {
    void RemoveOption(NLastGetopt::TOpts& opts, const TString& name) {
        for (auto opt = opts.Opts_.begin(); opt != opts.Opts_.end(); ++opt) {
            if (opt->Get()->GetName() == name) {
                opts.Opts_.erase(opt);
                return;
            }
        }
    }
} // anonymous namespace

TClientCommandInternalRoot::TClientCommandInternalRoot(const TString& name, const TClientSettings& settings)
    : TClientCommandRoot(name, settings)
{
    AddCommand(std::make_unique<TCommandExperimental>());
}

void TClientCommandInternalRoot::Config(TConfig& config) {
    TClientCommandRoot::Config(config);
    config.Opts->AddLongOption("y-scope", "Scope in which the commands will be executed")
        .RequiredArgument("SCOPE")
        .StoreResult(&config.YScope);
    RemoveOption(config.Opts->GetOpts(), "svnrevision");
}

int NewInternalClient(int argc, char** argv) {
    NYdb::NConsoleClient::TClientSettings settings;
    settings.EnableSsl = true;
    settings.UseAccessToken = true;
    settings.UseDefaultTokenFile = false;
    settings.UseIamAuth = true;
    settings.UseStaticCredentials = true;
    settings.UseOauth2TokenExchange = true;
    settings.UseExportToYt = false;
    settings.MentionUserAccount = false;
    settings.YdbDir = "ydb";

    settings.BuildInfoProvider = []() -> NYdb::NConsoleClient::TYdbCliBuildInfo {
        return {"ydb-int", "0.0.0"};
    };

    settings.EnableAiInteractive = true;

    auto commandsRoot = MakeHolder<TClientCommandInternalRoot>(std::filesystem::path(argv[0]).stem().string(), settings);
    commandsRoot->Opts.SetTitle("YDB client with experimental features support");
    TClientCommand::TConfig config(argc, argv);
    return commandsRoot->Process(config);
}

}
}
