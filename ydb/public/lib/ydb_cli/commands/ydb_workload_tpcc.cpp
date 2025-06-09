#include "ydb_workload_tpcc.h"

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

    virtual int Run(TConfig& config) override;

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
    NTPCC::CleanSync(connectionConfig, *RunConfig);
    return 0;
}

//-----------------------------------------------------------------------------

class TCommandTPCCInit
    : public TYdbCommand
{
public:
    TCommandTPCCInit(std::shared_ptr<NTPCC::TRunConfig> runConfig);
    ~TCommandTPCCInit() = default;

    virtual int Run(TConfig& config) override;

private:
    std::shared_ptr<NTPCC::TRunConfig> RunConfig;
};

TCommandTPCCInit::TCommandTPCCInit(std::shared_ptr<NTPCC::TRunConfig> runConfig)
    : TYdbCommand("init", {}, "Create and initialize tables for benchmark")
    , RunConfig(std::move(runConfig))
{
}

int TCommandTPCCInit::Run(TConfig& connectionConfig) {
    RunConfig->SetFullPath(connectionConfig);
    NTPCC::InitSync(connectionConfig, *RunConfig);
    return 0;
}

//-----------------------------------------------------------------------------

class TCommandTPCCImport
    : public TYdbCommand
{
public:
    TCommandTPCCImport(std::shared_ptr<NTPCC::TRunConfig> runConfig);
    ~TCommandTPCCImport() = default;

    virtual int Run(TConfig& config) override;

private:
    std::shared_ptr<NTPCC::TRunConfig> RunConfig;
};

TCommandTPCCImport::TCommandTPCCImport(std::shared_ptr<NTPCC::TRunConfig> runConfig)
    : TYdbCommand("import", {}, "Fill tables with initial benchmark data")
    , RunConfig(std::move(runConfig))
{
}

int TCommandTPCCImport::Run(TConfig& connectionConfig) {
    RunConfig->SetFullPath(connectionConfig);
    NTPCC::ImportSync(connectionConfig, *RunConfig);
    return 0;
}

//-----------------------------------------------------------------------------

class TCommandTPCCRun
    : public TYdbCommand
{
public:
    TCommandTPCCRun(std::shared_ptr<NTPCC::TRunConfig> runConfig);
    ~TCommandTPCCRun() = default;

    virtual void Config(TConfig& config) override;
    virtual int Run(TConfig& config) override;

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

    // TODO: default value should be auto
    config.Opts->AddLongOption(
        "warmup", TStringBuilder() << "Warmup time in minutes")
            .RequiredArgument("INT").StoreResult(&RunConfig->WarmupMinutes).DefaultValue(RunConfig->WarmupMinutes);
    config.Opts->AddLongOption(
        't', "time", TStringBuilder() << "Execution time in minutes")
            .RequiredArgument("INT").StoreResult(&RunConfig->RunMinutes).DefaultValue(RunConfig->RunMinutes);

    // TODO: default value should be auto
    config.Opts->AddLongOption(
        'm', "max-sessions", TStringBuilder() << "Soft limit on number of DB sessions")
            .RequiredArgument("INT").StoreResult(&RunConfig->MaxInflight).DefaultValue(RunConfig->MaxInflight);

    config.Opts->AddLongOption(
        "json-result-path", TStringBuilder() << "Store the result in JSON format at the specified path")
            .OptionalArgument("STRING").StoreResult(&RunConfig->JsonResultPath).DefaultValue("");

    // advanced options mainly for developers (all hidden)

    config.Opts->AddLongOption(
        "connections", TStringBuilder() << "Number of SDK driver/client instances (default: auto)")
            .RequiredArgument("INT").StoreResult(&RunConfig->DriverCount).DefaultValue(0)
            .Hidden();
    config.Opts->AddLongOption(
        "no-delays", TStringBuilder() << "Disable TPC-C keying/thinking delays")
            .Optional().StoreTrue(&RunConfig->NoDelays)
            .Hidden();
    config.Opts->AddLongOption(
        "simulate", TStringBuilder() << "Simulate transaction execution (delay is simulated transaction latency ms)")
            .OptionalArgument("INT").StoreResult(&RunConfig->SimulateTransactionMs).DefaultValue(0)
            .Hidden();
    config.Opts->AddLongOption(
        "simulate-select1", TStringBuilder() << "Instead of real queries, execute specified number of SELECT 1 queries")
            .OptionalArgument("INT").StoreResult(&RunConfig->SimulateTransactionSelect1Count).DefaultValue(0)
            .Hidden();
}

int TCommandTPCCRun::Run(TConfig& connectionConfig) {
    NTPCC::RunSync(connectionConfig, *RunConfig);
    return 0;
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
}

void TCommandTPCC::Config(TConfig& config) {
    TClientCommandTree::Config(config);

    const TString& availableMonitoringModes = GetEnumAllNames<NTPCC::TRunConfig::EDisplayMode>();

    config.Opts->AddLongOption(
        'p', "path", TStringBuilder() << "Database path where benchmark tables are located")
            .RequiredArgument("STRING").StoreResult(&RunConfig->Path);

    config.Opts->AddLongOption(
        'w', "warehouses", TStringBuilder() << "Number of warehouses")
            .OptionalArgument("INT").StoreResult(&RunConfig->WarehouseCount).DefaultValue(RunConfig->WarehouseCount);

    config.Opts->AddLongOption(
        "display-mode", TStringBuilder() << "Benchmark execution display mode: " << availableMonitoringModes)
            .OptionalArgument("STRING")
            .StoreResult(&RunConfig->DisplayMode).DefaultValue(NTPCC::TRunConfig::EDisplayMode::None);

    // advanced options mainly for developers (all hidden)

    // TODO: detect automatically
    config.Opts->AddLongOption(
        "threads", TStringBuilder() << "Number of threads executing queries (default: auto)")
            .RequiredArgument("INT").StoreResult(&RunConfig->ThreadCount).DefaultValue(0)
            .Hidden();

    config.Opts->AddLongOption(
        "extended-stats", TStringBuilder() << "Print additional statistics")
            .Optional().StoreTrue(&RunConfig->ExtendedStats)
            .Hidden();
    config.Opts->AddLongOption(
        "log-level", TStringBuilder() << "Log level from 0 to 8, default is 6 (INFO)")
            .Optional().StoreMappedResult(&RunConfig->LogPriority, [](const TString& v) {
                int intValue = FromString<int>(v);
                return static_cast<ELogPriority>(intValue);
            }).DefaultValue(RunConfig->LogPriority)
            .Hidden();
}

} // namespace NYdb::NConsoleClient
