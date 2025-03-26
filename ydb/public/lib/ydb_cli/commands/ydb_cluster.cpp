#include "ydb_cluster.h"

#include "ydb_dynamic_config.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/config/config.h>
#include <ydb/public/lib/ydb_cli/dump/dump.h>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/src/client/impl/ydb_internal/logger/log.h>
#undef INCLUDE_YDB_INTERNAL_H

using namespace NKikimr;

namespace NYdb::NConsoleClient::NCluster {

TCommandCluster::TCommandCluster()
    : TClientCommandTree("cluster", {}, "Cluster-wide administration")
{
    AddCommand(std::make_unique<TCommandClusterBootstrap>());
    AddCommand(std::make_unique<NDynamicConfig::TCommandConfig>(true));
    AddCommand(std::make_unique<TCommandClusterDump>());
    AddCommand(std::make_unique<TCommandClusterRestore>());
}

TCommandClusterBootstrap::TCommandClusterBootstrap()
    : TYdbCommand("bootstrap", {}, "Bootstrap automatically-assembled cluster")
{}

void TCommandClusterBootstrap::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("uuid", "Self-assembly UUID").RequiredArgument("STRING").StoreResult(&SelfAssemblyUUID);
    config.SetFreeArgsNum(0);
    config.AllowEmptyDatabase = true;
}

void TCommandClusterBootstrap::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandClusterBootstrap::Run(TConfig& config) {
    auto driver = std::make_unique<NYdb::TDriver>(CreateDriver(config));
    NYdb::NConfig::TConfigClient client(*driver);
    auto result = client.BootstrapCluster(SelfAssemblyUUID).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);
    return EXIT_SUCCESS;
}

TCommandClusterDump::TCommandClusterDump()
    : TYdbReadOnlyCommand("dump", {}, "Dump cluster into local directory")
{}

void TCommandClusterDump::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.SetFreeArgsNum(0);
    config.AllowEmptyDatabase = true;

    config.Opts->AddLongOption('o', "output", "Path in a local filesystem to a directory to place dump into."
            " Directory should either not exist or be empty."
            " If not specified, the dump is placed in the directory backup_YYYYYYMMDDDThhmmss.")
        .RequiredArgument("PATH")
        .StoreResult(&FilePath);
}

void TCommandClusterDump::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandClusterDump::Run(TConfig& config) {
    auto log = std::make_shared<TLog>(CreateLogBackend("cerr", TConfig::VerbosityLevelToELogPriorityChatty(config.VerbosityLevel)));
    log->SetFormatter(GetPrefixLogFormatter(""));

    NDump::TClient client(CreateDriver(config), std::move(log));
    NStatusHelpers::ThrowOnErrorOrPrintIssues(client.DumpCluster(FilePath));

    return EXIT_SUCCESS;
}

TCommandClusterRestore::TCommandClusterRestore()
    : TYdbCommand("restore", {}, "Restore cluster from local dump")
{}

void TCommandClusterRestore::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.SetFreeArgsNum(0);
    config.AllowEmptyDatabase = true;

    config.Opts->AddLongOption('i', "input", "Path in a local filesystem to a directory with dump.")
        .RequiredArgument("PATH")
        .StoreResult(&FilePath);

     config.Opts->AddLongOption('w', "wait-nodes-duration", "Wait for available database nodes for specified duration. Example: 10s, 5m, 1h.")
        .DefaultValue(TDuration::Minutes(1))
        .RequiredArgument("DURATION")
        .StoreResult(&WaitNodesDuration);
}

void TCommandClusterRestore::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandClusterRestore::Run(TConfig& config) {
    auto log = std::make_shared<TLog>(CreateLogBackend("cerr", TConfig::VerbosityLevelToELogPriorityChatty(config.VerbosityLevel)));
    log->SetFormatter(GetPrefixLogFormatter(""));

    auto settings = NDump::TRestoreClusterSettings()
        .WaitNodesDuration(WaitNodesDuration);

    NDump::TClient client(CreateDriver(config), std::move(log));
    NStatusHelpers::ThrowOnErrorOrPrintIssues(client.RestoreCluster(FilePath, settings));

    return EXIT_SUCCESS;
}

} // namespace NYdb::NConsoleClient::NCluster
