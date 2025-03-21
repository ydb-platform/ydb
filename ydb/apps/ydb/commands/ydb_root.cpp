#include "ydb_root.h"
#include "ydb_update.h"
#include "ydb_version.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/iam.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/oauth2_token_exchange/from_file.h>

#include <ydb/public/lib/ydb_cli/common/ydb_updater.h>

#include <filesystem>

namespace NYdb {
namespace NConsoleClient {

TClientCommandRoot::TClientCommandRoot(const TString& name, const TClientSettings& settings)
    : TClientCommandRootCommon(name, settings)
{
}

void TClientCommandRoot::FillConfig(TConfig& config) {
    TClientCommandRootCommon::FillConfig(config);
    config.IamEndpoint = NYdb::NIam::DEFAULT_ENDPOINT;
}

void TClientCommandRoot::SetCredentialsGetter(TConfig& config) {
    config.CredentialsGetter = [](const TClientCommand::TConfig& config) {
        if (config.SecurityToken) {
            return CreateOAuthCredentialsProviderFactory(config.SecurityToken);
        }
        if (config.UseStaticCredentials) {
            if (!config.StaticCredentials.User.empty()) {
                return CreateLoginCredentialsProviderFactory(config.StaticCredentials);
            }
        }
        if (config.UseOauth2TokenExchange) {
            if (config.Oauth2KeyFile) {
                return CreateOauth2TokenExchangeFileCredentialsProviderFactory(config.Oauth2KeyFile, config.IamEndpoint);
            }
        }
        if (config.UseIamAuth) {
            if (config.YCToken) {
                return CreateIamOAuthCredentialsProviderFactory(
                    { {.Endpoint = config.IamEndpoint}, config.YCToken });
            }
            if (config.UseMetadataCredentials) {
                return CreateIamCredentialsProviderFactory();
            }
            if (config.SaKeyFile) {
                return CreateIamJwtFileCredentialsProviderFactory(
                    { {.Endpoint = config.IamEndpoint}, config.SaKeyFile });
            }
        }
        return CreateInsecureCredentialsProviderFactory();
    };
}

TYdbClientCommandRoot::TYdbClientCommandRoot(const TString& name, const TClientSettings& settings)
    : TClientCommandRoot(name, settings)
{
    if (settings.StorageUrl.has_value()) {
        AddCommand(std::make_unique<TCommandUpdate>());
    }
    AddCommand(std::make_unique<TCommandVersion>());
}

namespace {
    void RemoveOption(NLastGetopt::TOpts& opts, const TString& name) {
        for (auto opt = opts.Opts_.begin(); opt != opts.Opts_.end(); ++opt) {
            if (opt->Get()->GetName() == name) {
                opts.Opts_.erase(opt);
                return;
            }
        }
    }
}

void TYdbClientCommandRoot::Config(TConfig& config) {
    TClientCommandRoot::Config(config);

    NLastGetopt::TOpts& opts = *config.Opts;
    RemoveOption(opts, "svnrevision");
}

int TYdbClientCommandRoot::Run(TConfig& config) {
    if (config.StorageUrl.has_value() && config.NeedToCheckForUpdate) {
        TYdbUpdater updater(config.StorageUrl.value());
        if (config.ForceVersionCheck) {
            Cout << "Force checking if there is a newer version..." << Endl;
        }
        if (updater.CheckIfUpdateNeeded(config.ForceVersionCheck)) {
            NColorizer::TColors colors = NColorizer::AutoColors(Cerr);
            Cerr << colors.Green() << "(!) New version of YDB CLI is available. Run 'ydb update' command for update. "
                << "You can also disable further version checks with 'ydb version --disable-checks' command"
                << colors.OldColor() << Endl;
        } else if (config.ForceVersionCheck) {
            NColorizer::TColors colors = NColorizer::AutoColors(Cerr);
            Cout << colors.GreenColor() << "Current version is up to date"
                << colors.OldColor() << Endl;
        }
    }

    return TClientCommandRoot::Run(config);
}

int NewYdbClient(int argc, char** argv) {
    NYdb::NConsoleClient::TClientSettings settings;
    settings.EnableSsl = true;
    settings.UseAccessToken = true;
    settings.UseDefaultTokenFile = false;
    settings.UseIamAuth = true;
    settings.UseStaticCredentials = true;
    settings.UseOauth2TokenExchange = true;
    settings.UseExportToYt = false;
    settings.MentionUserAccount = false;
    settings.StorageUrl = "https://storage.yandexcloud.net/yandexcloud-ydb/release";
    settings.YdbDir = "ydb";

    auto commandsRoot = MakeHolder<TYdbClientCommandRoot>(std::filesystem::path(argv[0]).stem().string(), settings);
    commandsRoot->Opts.SetTitle("YDB client");
    TClientCommand::TConfig config(argc, argv);
    return commandsRoot->Process(config);
}

}
}
