#include "ydb_root.h"

namespace NYdb {
namespace NConsoleClient {

int NewClient(int argc, char** argv) {
    NYdb::NConsoleClient::TClientSettings settings;
    settings.EnableSsl = true;
    settings.UseOAuthToken = true;
    settings.UseDefaultTokenFile = false;
    settings.UseIamAuth = false;
    settings.UseStaticCredentials = true;
    settings.UseExportToYt = false;
    settings.MentionUserAccount = false;
    settings.YdbDir = "ydb";

    THolder<TClientCommandRootCommon> commandsRoot = MakeHolder<TClientCommandRootCommon>(settings);
    commandsRoot->Opts.SetTitle("YDB client");
    TClientCommand::TConfig config(argc, argv);
    return commandsRoot->Process(config);
}

}
}
