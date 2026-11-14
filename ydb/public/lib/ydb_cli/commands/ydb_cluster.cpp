#include "ydb_cluster.h"

#include "ydb_bridge.h"
#include "ydb_diagnostics.h"
#include "ydb_dynamic_config.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/config/config.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_tablet.h>
#include <ydb/public/lib/ydb_cli/common/log.h>
#include <ydb/public/lib/ydb_cli/common/print_operation.h>
#include <ydb/public/lib/ydb_cli/dump/dump.h>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/src/client/impl/internal/logger/log.h>
#undef INCLUDE_YDB_INTERNAL_H

using namespace NKikimr;

namespace NYdb::NConsoleClient::NCluster {

TCommandCluster::TCommandCluster()
    : TClientCommandTree("cluster", {}, "Cluster-wide administration")
{
    AddCommand(std::make_unique<TCommandClusterBootstrap>());
    AddCommand(std::make_unique<NDynamicConfig::TCommandConfig>(false, true));
    AddCommand(std::make_unique<TCommandClusterDump>());
    AddCommand(std::make_unique<TCommandClusterRestore>());
    AddCommand(std::make_unique<TCommandBridge>(true));
    AddCommand(std::make_unique<TCommandClusterDiagnostics>());
    AddCommand(std::make_unique<TCommandClusterReassignTablets>());
}

TCommandClusterBootstrap::TCommandClusterBootstrap()
    : TYdbCommand("bootstrap", {}, "Bootstrap automatically-assembled cluster")
{}

void TCommandClusterBootstrap::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("uuid", "Self-assembly UUID").RequiredArgument("STRING").StoreResult(&SelfAssemblyUUID);
    config.Opts->AddLongOption("allow-unknown-fields", "Allow fields not present in config")
        .StoreTrue(&AllowUnknownFields);
    config.SetFreeArgsNum(0);
    config.AllowEmptyDatabase = true;
}

void TCommandClusterBootstrap::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandClusterBootstrap::Run(TConfig& config) {
    auto driver = CreateDriver(config);
    NYdb::NConfig::TConfigClient client(driver);
    auto settings = NYdb::NConfig::TBootstrapClusterSettings().AllowUnknownFields(AllowUnknownFields);
    auto result = client.BootstrapCluster(SelfAssemblyUUID, settings).GetValueSync();
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
    auto log = std::make_shared<TLog>(CreateLogBackend("cerr", VerbosityLevelToELogPriorityChatty(config.VerbosityLevel)));
    log->SetFormatter(GetPrefixLogFormatter(""));

    auto driver = CreateDriver(config);
    NDump::TClient client(driver, std::move(log));
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
    auto log = std::make_shared<TLog>(CreateLogBackend("cerr", VerbosityLevelToELogPriorityChatty(config.VerbosityLevel)));
    log->SetFormatter(GetPrefixLogFormatter(""));

    auto settings = NDump::TRestoreClusterSettings()
        .WaitNodesDuration(WaitNodesDuration);

    auto driver = CreateDriver(config);
    NDump::TClient client(driver, std::move(log));
    NStatusHelpers::ThrowOnErrorOrPrintIssues(client.RestoreCluster(FilePath, settings));

    return EXIT_SUCCESS;
}

TCommandClusterReassignTablets::TCommandClusterReassignTablets()
    : TYdbCommand("reassign-tablets", {},
        "Reassign tablet channels in the Hive serving the database specified with -d. "
        "The database selects the Hive and does not restrict affected tablets to that database.")
{}

void TCommandClusterReassignTablets::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.SetFreeArgsNum(0);
    config.Opts->AddLongOption("storage-pool", "Filter channels by storage pool name. Specify this or a nonzero --group-id, or both.")
        .RequiredArgument("NAME").StoreResult(&StoragePool);
    config.Opts->AddLongOption("group-id", "Filter channels by their current storage group ID. If omitted or zero, select all groups in --storage-pool.")
        .RequiredArgument("ID").StoreResult(&GroupId);
    config.Opts->AddLongOption("max-in-flight", "Maximum number of tablets being reassigned concurrently.")
        .RequiredArgument("N").DefaultValue(1).StoreResult(&MaxInFlight);
    config.Opts->AddLongOption("percent", "Percentage of matching tablets to reassign, starting with the oldest channel assignments.")
        .RequiredArgument("N").DefaultValue(100).StoreResult(&Percent);
    config.Opts->AddLongOption("on-restart", "Mark channels for reassignment on a subsequent tablet restart.")
        .NoArgument().StoreTrue(&OnRestart);
    AddOutputFormats(config, {EDataFormat::Pretty, EDataFormat::ProtoJsonBase64});
}

void TCommandClusterReassignTablets::Parse(TConfig& config) {
    TYdbCommand::Parse(config);
    ParseOutputFormats();
    if (StoragePool.empty() && GroupId == 0) {
        throw TMisuseException() << "Specify --storage-pool or a nonzero --group-id.";
    }
    if (MaxInFlight == 0) {
        throw TMisuseException() << "--max-in-flight must be positive.";
    }
    if (Percent > 100) {
        throw TMisuseException() << "--percent must be between 0 and 100.";
    }
}

int TCommandClusterReassignTablets::Run(TConfig& config) {
    auto driver = CreateDriver(config);
    NTablet::TTabletClient client(driver);
    auto settings = NTablet::TReassignTabletsSettings()
        .StoragePool(StoragePool).Group(GroupId).MaxInFlight(MaxInFlight).Percent(Percent).Async(OnRestart);
    auto operation = client.ReassignTablets(settings).GetValueSync();
    ThrowOnError(operation);
    PrintOperation(operation, OutputFormat);
    return EXIT_SUCCESS;
}

} // namespace NYdb::NConsoleClient::NCluster
