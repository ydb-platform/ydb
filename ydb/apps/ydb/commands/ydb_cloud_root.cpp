#include "ydb_cloud_root.h"
#include "ydb_update.h"
#include "ydb_version.h"

#include <ydb/public/sdk/cpp/client/iam/iam.h>
#include <ydb/public/lib/ydb_cli/common/ydb_updater.h>

namespace NYdb {
namespace NConsoleClient {

TClientCommandRoot::TClientCommandRoot(const TClientSettings& settings)
    : TClientCommandRootCommon(settings)
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
            if (config.StaticCredentials.User && config.StaticCredentials.Password) {
                return CreateLoginCredentialsProviderFactory(config.StaticCredentials);
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

TYCloudClientCommandRoot::TYCloudClientCommandRoot(const TClientSettings& settings)
    : TClientCommandRoot(settings)
{
    AddCommand(std::make_unique<TCommandUpdate>());
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

void TYCloudClientCommandRoot::Config(TConfig& config) {
    TClientCommandRoot::Config(config);

    NLastGetopt::TOpts& opts = *config.Opts;
    RemoveOption(opts, "svnrevision");
}

int TYCloudClientCommandRoot::Run(TConfig& config) {
    if (config.NeedToCheckForUpdate) {
        TYdbUpdater updater;
        if (config.ForceVersionCheck) {
            Cout << "Force checking if there is a newer version..." << Endl;
        }
        if (updater.CheckIfUpdateNeeded(config.ForceVersionCheck)) {
            NColorizer::TColors colors = NColorizer::AutoColors(Cerr);
            Cerr << colors.RedColor() << "(!) New version of YDB CLI is available. Run 'ydb update' command for update. "
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

int NewYCloudClient(int argc, char** argv) {
    NYdb::NConsoleClient::TClientSettings settings;
    settings.EnableSsl = true;
    settings.UseOAuthToken = true;
    settings.UseDefaultTokenFile = false;
    settings.UseIamAuth = true;
    settings.UseStaticCredentials = true;
    settings.UseExportToYt = false;
    settings.MentionUserAccount = false;
    settings.YdbDir = "ydb";

    THolder<TYCloudClientCommandRoot> commandsRoot = MakeHolder<TYCloudClientCommandRoot>(settings);
    commandsRoot->Opts.SetTitle("YDB client for Yandex.Cloud");
    TClientCommand::TConfig config(argc, argv);
    return commandsRoot->Process(config);
}

}
}
