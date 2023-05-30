#include "ydb_version.h"

#include <ydb/public/lib/ydb_cli/common/ydb_updater.h>

#include <library/cpp/resource/resource.h>
#include <util/string/strip.h>

namespace NYdb::NConsoleClient {

TCommandVersion::TCommandVersion()
    : TClientCommand("version", {}, "Print YDB CLI version")
{}

void TCommandVersion::Config(TConfig& config) {
    TClientCommand::Config(config);

    config.NeedToConnect = false;

    config.SetFreeArgsNum(0);

    config.Opts->AddLongOption("semantic", "Print semantic version only")
        .StoreTrue(&Semantic);
    config.Opts->AddLongOption("check", "Force to check latest version available")
        .StoreTrue(&config.ForceVersionCheck);
    config.Opts->AddLongOption("disable-checks", "Disable version checks. CLI will not check whether there is a newer version available")
        .StoreTrue(&DisableChecks);
    config.Opts->AddLongOption("enable-checks", "Enable version checks. CLI will regularly check whether there is a newer version available")
        .StoreTrue(&EnableChecks);

    config.Opts->MutuallyExclusive("disable-checks", "semantic");
    config.Opts->MutuallyExclusive("disable-checks", "enable-checks");
    config.Opts->MutuallyExclusive("disable-checks", "check");
    config.Opts->MutuallyExclusive("enable-checks", "semantic");
    config.Opts->MutuallyExclusive("enable-checks", "check");
    config.Opts->MutuallyExclusive("check", "semantic");
}

void TCommandVersion::Parse(TConfig& config) {
    TClientCommand::Parse(config);

    config.NeedToCheckForUpdate = !DisableChecks && !EnableChecks;
}

int TCommandVersion::Run(TConfig& config) {
    Y_UNUSED(config);

    if (EnableChecks) {
        TYdbUpdater updater;
        updater.SetCheckVersion(true);
        Cout << "Latest version checks enabled" << Endl;
        return EXIT_SUCCESS;
    }
    if (DisableChecks) {
        TYdbUpdater updater;
        updater.SetCheckVersion(false);
        Cout << "Latest version checks disabled" << Endl;
        return EXIT_SUCCESS;
    }
    if (!Semantic) {
        Cout << "YDB CLI ";
    }
    Cout << StripString(NResource::Find(TStringBuf(VersionResourceName))) << Endl;
    return EXIT_SUCCESS;
}

} // NYdb::NConsoleClient
