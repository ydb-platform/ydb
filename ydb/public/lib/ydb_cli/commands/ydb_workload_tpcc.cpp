#include "ydb_workload_tpcc.h"

#include <ydb/library/workload/tpcc/check.h>
#include <ydb/library/workload/tpcc/clean.h>
#include <ydb/library/workload/tpcc/import.h>
#include <ydb/library/workload/tpcc/init.h>
#include <ydb/library/workload/tpcc/runner.h>

#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>

#include <util/generic/serialized_enum.h>
#include <util/system/info.h>

namespace NYdb::NConsoleClient {

namespace {

//-----------------------------------------------------------------------------

class TCommandTPCCClean
    : public TYdbCommand
{
public:
    TCommandTPCCClean(std::shared_ptr<NTPCC::TRunConfig> runConfig);
    ~TCommandTPCCClean() = default;

    int Run(TConfig& config) override;

private:
    std::shared_ptr<NTPCC::TRunConfig> RunConfig;
};

TCommandTPCCClean::TCommandTPCCClean(std::shared_ptr<NTPCC::TRunConfig> runConfig)
    : TYdbCommand("clean", {}, "Drop tables created by benchmark")
    , RunConfig(std::move(runConfig))
{
}

int TCommandTPCCClean::Run(TConfig& connectionConfig) {
    RunConfig->SetFullPath(connectionConfig);
    try {
        NTPCC::CleanSync(connectionConfig, *RunConfig);
        return 0;
    } catch (const std::exception& e) {
        Cout << "Clean failed: " << e.what() << Endl;
        return 1;
    }
}

//-----------------------------------------------------------------------------

class TCommandTPCCInit
    : public TYdbCommand
{
public:
    TCommandTPCCInit(std::shared_ptr<NTPCC::TRunConfig> runConfig);
    ~TCommandTPCCInit() = default;

    void Config(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    std::shared_ptr<NTPCC::TRunConfig> RunConfig;
};

TCommandTPCCInit::TCommandTPCCInit(std::shared_ptr<NTPCC::TRunConfig> runConfig)
    : TYdbCommand("init", {}, "Create and initialize tables for benchmark")
    , RunConfig(std::move(runConfig))
{
}

void TCommandTPCCInit::Config(TConfig& config) {
    TYdbCommand::Config(config);

    config.Opts->AddLongOption(
        'w', "warehouses", TStringBuilder() << "Number of warehouses")
            .RequiredArgument("INT").StoreResult(&RunConfig->WarehouseCount).DefaultValue(RunConfig->WarehouseCount);

    config.Opts->AddLongOption(
        "log-level", TStringBuilder() << "Log level from 0 to 8, default is 6 (INFO)")
            .Optional().StoreMappedResult(&RunConfig->LogPriority, [](const TString& v) {
                return FromString<ELogPriority>(v);
            }).DefaultValue(RunConfig->LogPriority).Hidden();
}

int TCommandTPCCInit::Run(TConfig& connectionConfig) {
    RunConfig->SetFullPath(connectionConfig);
    try {
        NTPCC::InitSync(connectionConfig, *RunConfig);
        return 0;
    } catch (const std::exception& e) {
        Cout << "Init failed: " << e.what() << Endl;
        return 1;
    }
}

//-----------------------------------------------------------------------------

class TCommandTPCCImport
    : public TYdbCommand
{
public:
    TCommandTPCCImport(std::shared_ptr<NTPCC::TRunConfig> runConfig);
    ~TCommandTPCCImport() = default;

    void Config(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    std::shared_ptr<NTPCC::TRunConfig> RunConfig;
};

TCommandTPCCImport::TCommandTPCCImport(std::shared_ptr<NTPCC::TRunConfig> runConfig)
    : TYdbCommand("import", {}, "Fill tables with initial benchmark data")
    , RunConfig(std::move(runConfig))
{
}

void TCommandTPCCImport::Config(TConfig& config) {
    TYdbCommand::Config(config);

    config.Opts->AddLongOption(
        'w', "warehouses", TStringBuilder() << "Number of warehouses")
            .RequiredArgument("INT").StoreResult(&RunConfig->WarehouseCount).DefaultValue(RunConfig->WarehouseCount);

    // TODO: detect automatically
    config.Opts->AddLongOption(
        "threads", TStringBuilder() << "Number of threads loading the data")
            .RequiredArgument("INT").StoreResult(&RunConfig->LoadThreadCount).DefaultValue(RunConfig->LoadThreadCount);

    config.Opts->AddLongOption(
        "no-tui", TStringBuilder() << "Disable TUI, which is enabled by default in interactive mode")
            .Optional().StoreTrue(&RunConfig->NoTui);

    // advanced hidden options (mainly for developers)

    auto logLevelOpt = config.Opts->AddLongOption(
        "log-level", TStringBuilder() << "Log level from 0 to 8, default is 6 (INFO)")
            .Optional().StoreMappedResult(&RunConfig->LogPriority, [](const TString& v) {
                return FromString<ELogPriority>(v);
            }).DefaultValue(RunConfig->LogPriority);

    auto connectionsOpt = config.Opts->AddLongOption(
        "connections", TStringBuilder() << "Number of SDK driver/client instances (default: auto)")
            .RequiredArgument("INT").StoreResult(&RunConfig->DriverCount).DefaultValue(0);

    // for now. Later might be "config.HelpCommandVerbosiltyLevel <= 1" or advanced section
    if (true) {
        logLevelOpt.Hidden();
        connectionsOpt.Hidden();
    }
}

int TCommandTPCCImport::Run(TConfig& connectionConfig) {
    RunConfig->SetFullPath(connectionConfig);
    try {
        NTPCC::ImportSync(connectionConfig, *RunConfig);
        return 0;
    } catch (const std::exception& e) {
        Cout << "Import failed: " << e.what() << Endl;
        return 1;
    }
}

//-----------------------------------------------------------------------------

class TCommandTPCCRun
    : public TYdbCommand
{
public:
    TCommandTPCCRun(std::shared_ptr<NTPCC::TRunConfig> runConfig);
    ~TCommandTPCCRun() = default;

    void Config(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    std::shared_ptr<NTPCC::TRunConfig> RunConfig;
};

TCommandTPCCRun::TCommandTPCCRun(std::shared_ptr<NTPCC::TRunConfig> runConfig)
    : TYdbCommand("run", {}, "Run the benchmark")
    , RunConfig(std::move(runConfig))
{
}

void TCommandTPCCRun::Config(TConfig& config) {
    TYdbCommand::Config(config);

    config.Opts->AddLongOption(
        'w', "warehouses", TStringBuilder() << "Number of warehouses")
            .RequiredArgument("INT").StoreResult(&RunConfig->WarehouseCount).DefaultValue(RunConfig->WarehouseCount);

    // TODO: default value should be auto
    config.Opts->AddLongOption(
        "warmup", TStringBuilder() << "Warmup time. Example: 10s, 5m, 1h")
            .RequiredArgument("DURATION").StoreResult(&RunConfig->WarmupDuration).DefaultValue(RunConfig->WarmupDuration);

    config.Opts->AddLongOption(
        't', "time", TStringBuilder() << "Execution time. Example: 10s, 5m, 1h")
            .RequiredArgument("DURATION").StoreResult(&RunConfig->RunDuration).DefaultValue(RunConfig->RunDuration);

    // TODO: default value should be auto
    config.Opts->AddLongOption(
        'm', "max-sessions", TStringBuilder() << "Soft limit on number of DB sessions")
            .RequiredArgument("INT").StoreResult(&RunConfig->MaxInflight).DefaultValue(RunConfig->MaxInflight);

    // TODO: detect automatically
    config.Opts->AddLongOption(
        "threads", TStringBuilder() << "Number of threads executing queries (by default autodected)")
            .RequiredArgument("INT").StoreResult(&RunConfig->ThreadCount);

    config.Opts->AddLongOption(
        'f', "format", TStringBuilder() << "Output format: " << GetEnumAllNames<NTPCC::TRunConfig::EFormat>())
            .OptionalArgument("STRING").StoreResult(&RunConfig->Format).DefaultValue(RunConfig->Format);

    config.Opts->AddLongOption(
        "no-tui", TStringBuilder() << "Disable TUI, which is enabled by default in interactive mode")
            .Optional().StoreTrue(&RunConfig->NoTui);

    // advanced hidden options (mainly for developers)

    auto extendedStatsOpt = config.Opts->AddLongOption(
        "extended-stats", TStringBuilder() << "Print additional statistics")
            .Optional().StoreTrue(&RunConfig->ExtendedStats);

    auto logLevelOpt = config.Opts->AddLongOption(
        "log-level", TStringBuilder() << "Log level from 0 to 8, default is 6 (INFO)")
            .Optional().StoreMappedResult(&RunConfig->LogPriority, [](const TString& v) {
                return FromString<ELogPriority>(v);
            }).DefaultValue(RunConfig->LogPriority);

    auto connectionsOpt = config.Opts->AddLongOption(
        "connections", TStringBuilder() << "Number of SDK driver/client instances (default: auto)")
            .RequiredArgument("INT").StoreResult(&RunConfig->DriverCount).DefaultValue(0);

    auto noDelaysOpt = config.Opts->AddLongOption(
        "no-delays", TStringBuilder() << "Disable TPC-C keying/thinking delays")
            .Optional().StoreTrue(&RunConfig->NoDelays);

    auto simulateOpt = config.Opts->AddLongOption(
        "simulate", TStringBuilder() << "Simulate transaction execution (delay is simulated transaction latency ms)")
            .OptionalArgument("INT").StoreResult(&RunConfig->SimulateTransactionMs).DefaultValue(0);

    auto simulateSelect1Opt = config.Opts->AddLongOption(
        "simulate-select1", TStringBuilder() << "Instead of real queries, execute specified number of SELECT 1 queries")
            .OptionalArgument("INT").StoreResult(&RunConfig->SimulateTransactionSelect1Count).DefaultValue(0);

    // for now. Later might be "config.HelpCommandVerbosiltyLevel <= 1" or advanced section
    if (true) {
        extendedStatsOpt.Hidden();
        logLevelOpt.Hidden();
        connectionsOpt.Hidden();
        noDelaysOpt.Hidden();
        simulateOpt.Hidden();
        simulateSelect1Opt.Hidden();
    }
}

int TCommandTPCCRun::Run(TConfig& connectionConfig) {
    RunConfig->SetFullPath(connectionConfig);
    try {
        NTPCC::RunSync(connectionConfig, *RunConfig);
        return 0;
    } catch (const std::exception& e) {
        Cout << "Run failed: " << e.what() << Endl;
        return 1;
    }
}

//-----------------------------------------------------------------------------

class TCommandTPCCCheck
    : public TYdbCommand
{
public:
    TCommandTPCCCheck(std::shared_ptr<NTPCC::TRunConfig> runConfig);
    ~TCommandTPCCCheck() = default;

    void Config(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    std::shared_ptr<NTPCC::TRunConfig> RunConfig;
};

TCommandTPCCCheck::TCommandTPCCCheck(std::shared_ptr<NTPCC::TRunConfig> runConfig)
    : TYdbCommand("check", {}, "Check TPC-C data consistency")
    , RunConfig(std::move(runConfig))
{
}

void TCommandTPCCCheck::Config(TConfig& config) {
    TYdbCommand::Config(config);

    config.Opts->AddLongOption(
        'w', "warehouses", TStringBuilder() << "Number of warehouses")
            .RequiredArgument("INT").StoreResult(&RunConfig->WarehouseCount).DefaultValue(RunConfig->WarehouseCount);

    config.Opts->AddLongOption(
        "just-imported", TStringBuilder() << "Turns on additional checks. "
                << "Should be used only when data has been just imported and no runs have been done yet.")
            .Optional().StoreTrue(&RunConfig->JustImported);

    config.Opts->AddLongOption(
        "log-level", TStringBuilder() << "Log level from 0 to 8, default is 6 (INFO)")
            .Optional().StoreMappedResult(&RunConfig->LogPriority, [](const TString& v) {
                return FromString<ELogPriority>(v);
            }).DefaultValue(RunConfig->LogPriority).Hidden();
}

int TCommandTPCCCheck::Run(TConfig& connectionConfig) {
    RunConfig->SetFullPath(connectionConfig);
    try {
        NTPCC::CheckSync(connectionConfig, *RunConfig);
        return 0;
    } catch (const std::exception& e) {
        Cout << "Check failed: " << e.what() << Endl;
        return 1;
    }
}

} // anonymous

//-----------------------------------------------------------------------------

TCommandTPCC::TCommandTPCC()
    : TClientCommandTree("tpcc", {}, "YDB TPC-C benchmark")
    , RunConfig(std::make_shared<NTPCC::TRunConfig>())
{
    AddCommand(std::make_unique<TCommandTPCCRun>(RunConfig));
    AddCommand(std::make_unique<TCommandTPCCClean>(RunConfig));
    AddCommand(std::make_unique<TCommandTPCCInit>(RunConfig));
    AddCommand(std::make_unique<TCommandTPCCImport>(RunConfig));
    AddCommand(std::make_unique<TCommandTPCCCheck>(RunConfig));
}

void TCommandTPCC::Config(TConfig& config) {
    TClientCommandTree::Config(config);

    config.Opts->AddLongOption(
        'p', "path", TStringBuilder() << "Database path where benchmark tables are located")
            .RequiredArgument("STRING").StoreResult(&RunConfig->Path);
}

} // namespace NYdb::NConsoleClient
