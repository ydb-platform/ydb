#include "ydb_workload_testshard.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_test_shard.h>
#include <ydb/public/lib/ydb_cli/common/command_utils.h>

#include <util/stream/file.h>

namespace NYdb::NConsoleClient {

TCommandTestShard::TCommandTestShard()
    : TClientCommandTree("testshard", {}, "Manage TestShard tablets for load testing")
{
    AddCommand(std::make_unique<TCommandTestShardCreate>());
    AddCommand(std::make_unique<TCommandTestShardDelete>());
}

TCommandTestShardCreate::TCommandTestShardCreate()
    : TYdbCommand("create", {}, "Create TestShard tablets with load generation (like tstool)")
{
}

void TCommandTestShardCreate::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("owner-idx", "Unique owner index for idempotent tablet creation")
        .Required().RequiredArgument("IDX").StoreResult(&OwnerIdx);
    config.Opts->AddLongOption("channels", "Storage pool names for tablet channels (comma-separated, optional - uses database storage pools if not specified)")
        .Optional().RequiredArgument("POOLS").Handler([this](const TString& value) {
            Channels = StringSplitter(value).Split(',').ToList<TString>();
        });
    config.Opts->AddLongOption("count", "Number of tablets to create (default: 1)")
        .DefaultValue(1).RequiredArgument("NUM").StoreResult(&Count);
    config.Opts->AddLongOption('f', "config-file", "Path to YAML configuration file")
        .RequiredArgument("PATH").StoreResult(&ConfigFile);
    config.Opts->MutuallyExclusive("config", "config-file");
    config.SetFreeArgsNum(0);
}

void TCommandTestShardCreate::Parse(TConfig& config) {
    TYdbCommand::Parse(config);

    if (OwnerIdx == 0) {
        ythrow yexception() << "owner-idx must be non-zero";
    }

    if (ConfigFile.empty()) {
        ythrow yexception() << "config-file must be specified";
    }

    if (!ConfigFile.empty()) {
        ConfigYaml = TFileInput(ConfigFile).ReadAll();
    }
}

int TCommandTestShardCreate::Run(TConfig& config) {
    auto driver = std::make_unique<TDriver>(CreateDriver(config));
    auto client = NYdb::NTestShard::TTestShardClient(*driver);

    auto result = client.CreateTestShard(
        OwnerIdx, std::vector<std::string>(Channels.begin(), Channels.end()), Count,
        std::string(ConfigYaml), config.Database).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);

    Cout << "TestShard tablet(s) created successfully." << Endl;
    const auto& tabletIds = result.GetTabletIds();
    Cout << "Tablet IDs: ";
    for (size_t i = 0; i < tabletIds.size(); ++i) {
        if (i > 0) Cout << ", ";
        Cout << tabletIds[i];
    }
    Cout << Endl;

    return EXIT_SUCCESS;
}

TCommandTestShardDelete::TCommandTestShardDelete()
    : TYdbCommand("delete", {}, "Delete TestShard tablets by owner_idx range")
{
}

void TCommandTestShardDelete::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("owner-idx", "Owner index identifying tablets to delete")
        .Required().RequiredArgument("IDX").StoreResult(&OwnerIdx);
    config.Opts->AddLongOption("count", "Number of consecutive tablets to delete (default: 1)")
        .DefaultValue(1).RequiredArgument("NUM").StoreResult(&Count);
    config.SetFreeArgsNum(0);
}

void TCommandTestShardDelete::Parse(TConfig& config) {
    TYdbCommand::Parse(config);

    if (OwnerIdx == 0) {
        ythrow yexception() << "owner-idx must be non-zero";
    }
}

int TCommandTestShardDelete::Run(TConfig& config) {
    auto driver = std::make_unique<TDriver>(CreateDriver(config));
    auto client = NYdb::NTestShard::TTestShardClient(*driver);

    auto result = client.DeleteTestShard(OwnerIdx, Count, config.Database).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);

    Cout << "TestShard tablet(s) deleted successfully." << Endl;

    return EXIT_SUCCESS;
}

} // namespace NYdb::NConsoleClient
