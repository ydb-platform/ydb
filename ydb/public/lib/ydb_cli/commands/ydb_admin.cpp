#include "ydb_admin.h"

#include "ydb_dynamic_config.h"
#include "ydb_node_config.h"
#include "ydb_storage_config.h"
#include "ydb_cluster.h"

#include <ydb/public/lib/ydb_cli/common/command_utils.h>
#include <ydb/public/lib/ydb_cli/dump/dump.h>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/src/client/impl/ydb_internal/logger/log.h>
#undef INCLUDE_YDB_INTERNAL_H

namespace NYdb {
namespace NConsoleClient {

using namespace NUtils;

class TCommandNode : public TClientCommandTree {
public:
    TCommandNode()
        : TClientCommandTree("node", {}, "Node-wide administration")
    {
        AddCommand(std::make_unique<NNodeConfig::TCommandNodeConfig>());
    }
};

class TCommandDatabase : public TClientCommandTree {
public:
    TCommandDatabase()
        : TClientCommandTree("database", {}, "Database-wide administration")
    {
        AddCommand(std::make_unique<NDynamicConfig::TCommandConfig>(false));
        AddCommand(std::make_unique<TCommandDatabaseDump>());
        AddCommand(std::make_unique<TCommandDatabaseRestore>());
    }
};

TCommandDatabaseDump::TCommandDatabaseDump()
    : TYdbReadOnlyCommand("dump", {}, "Dump database into local directory")
{}

void TCommandDatabaseDump::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.SetFreeArgsNum(0);

    config.Opts->AddLongOption('o', "output", "Path in a local filesystem to a directory to place dump into."
            " Directory should either not exist or be empty."
            " If not specified, the dump is placed in the directory backup_YYYYYYMMDDDThhmmss.")
        .RequiredArgument("PATH")
        .StoreResult(&FilePath);
}

void TCommandDatabaseDump::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandDatabaseDump::Run(TConfig& config) {
    auto log = std::make_shared<TLog>(CreateLogBackend("cerr", TConfig::VerbosityLevelToELogPriorityChatty(config.VerbosityLevel)));
    log->SetFormatter(GetPrefixLogFormatter(""));

    NDump::TClient client(CreateDriver(config), std::move(log));
    NStatusHelpers::ThrowOnErrorOrPrintIssues(client.DumpDatabase(config.Database, FilePath));

    return EXIT_SUCCESS;
}

TCommandDatabaseRestore::TCommandDatabaseRestore()
    : TYdbCommand("restore", {}, "Restore database from local dump")
{}

void TCommandDatabaseRestore::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.SetFreeArgsNum(0);

    config.Opts->AddLongOption('i', "input", "Path in a local filesystem to a directory with dump.")
        .RequiredArgument("PATH")
        .StoreResult(&FilePath);

    config.Opts->AddLongOption('w', "wait-nodes-duration", "Wait for available database nodes for specified duration. Example: 10s, 5m, 1h.")
        .DefaultValue(TDuration::Minutes(1))
        .RequiredArgument("DURATION")
        .StoreResult(&WaitNodesDuration);
}

void TCommandDatabaseRestore::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandDatabaseRestore::Run(TConfig& config) {
    auto log = std::make_shared<TLog>(CreateLogBackend("cerr", TConfig::VerbosityLevelToELogPriorityChatty(config.VerbosityLevel)));
    log->SetFormatter(GetPrefixLogFormatter(""));

    auto settings = NDump::TRestoreDatabaseSettings()
        .WaitNodesDuration(WaitNodesDuration)
        .Database(config.Database);

    NDump::TClient client(CreateDriver(config), std::move(log));
    NStatusHelpers::ThrowOnErrorOrPrintIssues(client.RestoreDatabase(FilePath, settings));

    return EXIT_SUCCESS;
}

TCommandAdmin::TCommandAdmin()
    : TClientCommandTree("admin", {}, "Administrative cluster operations")
{
    MarkDangerous();
    UseOnlyExplicitProfile();
    // keep old commands "safe", to keep old behavior
    AddHiddenCommand(std::make_unique<NDynamicConfig::TCommandConfig>(
                         true,
                         NDynamicConfig::TCommandFlagsOverrides{.Dangerous = false, .OnlyExplicitProfile = false},
                         false));
    AddHiddenCommand(std::make_unique<NDynamicConfig::TCommandVolatileConfig>());
    AddHiddenCommand(std::make_unique<NStorageConfig::TCommandStorageConfig>(false));
    AddCommand(std::make_unique<NCluster::TCommandCluster>());
    AddCommand(std::make_unique<TCommandNode>());
    AddCommand(std::make_unique<TCommandDatabase>());
}

void TCommandAdmin::Config(TConfig& config) {
    TClientCommand::Config(config);
    SetFreeArgs(config);
    TString commands;
    SetFreeArgTitle(0, "<subcommand>", commands);
    TStringStream stream;
    NColorizer::TColors colors = NColorizer::AutoColors(Cout);
    stream << Endl << Endl
           << colors.BoldColor()
           << "Commands in this subtree may damage your cluster if used wrong" << Endl
           << "Due to dangerous nature of this commands ALL global parameters must be set explicitly" << Endl
           << "Profiles are disabled by default, and used only if set explicitly (--profile <profile-name>)" << Endl
           << "Some commands do not require global options that are required otherwise"
           << colors.OldColor();
    stream << Endl << Endl
        << colors.BoldColor() << "Description" << colors.OldColor() << ": " << Description << Endl << Endl
        << colors.BoldColor() << "Subcommands" << colors.OldColor() << ":" << Endl;
    RenderCommandDescription(stream, config.HelpCommandVerbosiltyLevel > 1, colors, BEGIN, "", true);
    stream << Endl;
    PrintParentOptions(stream, config, colors);
    config.Opts->SetCmdLineDescr(stream.Str());
}


}
}
