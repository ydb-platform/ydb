#include "ydb_workload_testshard.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/test_shard/test_shard.h>
#include <ydb/public/lib/ydb_cli/common/command_utils.h>

#include <util/stream/file.h>
#include <util/system/fs.h>

namespace NYdb::NConsoleClient {

TCommandTestShard::TCommandTestShard()
    : TClientCommandTree("testshard", {}, "Manage TestShard tablets for load testing")
{
    AddCommand(std::make_unique<TCommandTestShardInit>());
    AddCommand(std::make_unique<TCommandTestShardClean>());
}

TCommandTestShardInit::TCommandTestShardInit()
    : TYdbCommand("init", {}, "Create and initialize TestShard tablets with load generation (like tstool)")
{
}

void TCommandTestShardInit::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("channels", "Storage pool names for tablet channels (comma-separated, optional - uses database storage pools if not specified)")
        .Optional().RequiredArgument("POOLS").Handler([this](const TString& value) {
            Channels = StringSplitter(value).Split(',').ToList<TString>();
        });
    config.Opts->AddLongOption("count", "Number of tablets to create (default: 1)")
        .DefaultValue(1).RequiredArgument("NUM").StoreResult(&Count);
    config.Opts->AddLongOption('f', "config-file", "Path to YAML configuration file")
        .RequiredArgument("PATH").StoreResult(&ConfigFile);
    config.SetFreeArgsNum(1);
    SetFreeArgTitle(0, "<path>", "Path to TestShard object");
}

void TCommandTestShardInit::Parse(TConfig& config) {
    TYdbCommand::Parse(config);
    Path = config.ParseResult->GetFreeArgs()[0];

    if (ConfigFile.empty()) {
        ythrow yexception() << "config-file must be specified";
    }

    if (!NFs::Exists(ConfigFile)) {
        ythrow yexception() << "Config file not found: " << ConfigFile;
    }

    ConfigYaml = TFileInput(ConfigFile).ReadAll();
}

int TCommandTestShardInit::Run(TConfig& config) {
    auto driver = std::make_unique<TDriver>(CreateDriver(config));
    auto client = NYdb::NTestShard::TTestShardClient(*driver);

    auto result = client.CreateTestShard(
        Path, std::vector<std::string>(Channels.begin(), Channels.end()), Count,
        std::string(ConfigYaml)).GetValueSync();
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

TCommandTestShardClean::TCommandTestShardClean()
    : TYdbCommand("clean", {}, "Delete TestShard object")
{
}

void TCommandTestShardClean::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.SetFreeArgsNum(1);
    SetFreeArgTitle(0, "<path>", "Path to TestShard object");
}

void TCommandTestShardClean::Parse(TConfig& config) {
    TYdbCommand::Parse(config);
    Path = config.ParseResult->GetFreeArgs()[0];
}

int TCommandTestShardClean::Run(TConfig& config) {
    auto driver = std::make_unique<TDriver>(CreateDriver(config));
    auto client = NYdb::NTestShard::TTestShardClient(*driver);

    auto result = client.DeleteTestShard(Path).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);

    Cout << "TestShard object deleted successfully." << Endl;

    return EXIT_SUCCESS;
}

} // namespace NYdb::NConsoleClient

