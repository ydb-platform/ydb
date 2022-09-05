#include "ydb_update.h"
#include <ydb/public/lib/ydb_cli/common/ydb_updater.h>

namespace NYdb::NConsoleClient {

TCommandUpdate::TCommandUpdate()
    : TClientCommand("update", {}, "Update current YDB CLI binary if there is a newer version available")
{}

void TCommandUpdate::Config(TConfig& config) {
    TClientCommand::Config(config);

    config.NeedToConnect = false;
    config.NeedToCheckForUpdate = false;

    config.SetFreeArgsNum(0);

    config.Opts->AddLongOption('f', "force", "Force update. Do not check if there is a newer version available.")
        .StoreTrue(&ForceUpdate);
}

int TCommandUpdate::Run(TConfig& config) {
    Y_UNUSED(config);


    TYdbUpdater updater;
    return updater.Update(ForceUpdate);
}

} // NYdb::NConsoleClient
