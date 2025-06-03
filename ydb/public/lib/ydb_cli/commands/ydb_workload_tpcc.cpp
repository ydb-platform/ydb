#include "ydb_workload_tpcc.h"

#include <ydb/library/workload/tpcc/runner.h>

#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>

#include <util/system/info.h>

namespace NYdb::NConsoleClient {

namespace {

//-----------------------------------------------------------------------------

constexpr int DEFAULT_WAREHOUSE_COUNT = 1;
constexpr int DEFAULT_WARMUP_MINUTES = 1; // TODO
constexpr int DEFAULT_RUN_MINUTES = 2; // TODO
constexpr int DEFAULT_MAX_SESSIONS = 100; // TODO
constexpr int DEFAULT_LOG_LEVEL = 6; // TODO: properly use enum

//-----------------------------------------------------------------------------

class TCommandTPCCClean
    : public TYdbCommand
{
public:
    TCommandTPCCClean(std::shared_ptr<NTPCC::TRunConfig>& runConfig);
    ~TCommandTPCCClean() = default;

    virtual int Run(TConfig& config) override;

private:
    std::shared_ptr<NTPCC::TRunConfig> RunConfig;
};

TCommandTPCCClean::TCommandTPCCClean(std::shared_ptr<NTPCC::TRunConfig>& runConfig)
    : TYdbCommand("clean", {}, "Drop tables created in init phase")
    , RunConfig(runConfig)
{
}

int TCommandTPCCClean::Run(TConfig& config) {
    Y_UNUSED(config);
    Cout << "Not Implemented" << Endl;
    return 1;
}

//-----------------------------------------------------------------------------

class TCommandTPCCInit
    : public TYdbCommand
{
public:
    TCommandTPCCInit(std::shared_ptr<NTPCC::TRunConfig>& runConfig);
    ~TCommandTPCCInit() = default;

    virtual int Run(TConfig& config) override;

private:
    std::shared_ptr<NTPCC::TRunConfig> RunConfig;
};

TCommandTPCCInit::TCommandTPCCInit(std::shared_ptr<NTPCC::TRunConfig>& runConfig)
    : TYdbCommand("init", {}, "Create and initialize tables for workload")
    , RunConfig(runConfig)
{
}

int TCommandTPCCInit::Run(TConfig& config) {
    Y_UNUSED(config);
    Cout << "Not Implemented" << Endl;
    return 1;
}

//-----------------------------------------------------------------------------

class TCommandTPCCImport
    : public TYdbCommand
{
public:
    TCommandTPCCImport(std::shared_ptr<NTPCC::TRunConfig>& runConfig);
    ~TCommandTPCCImport() = default;

    virtual int Run(TConfig& config) override;

private:
    std::shared_ptr<NTPCC::TRunConfig> RunConfig;
};

TCommandTPCCImport::TCommandTPCCImport(std::shared_ptr<NTPCC::TRunConfig>& runConfig)
    : TYdbCommand("import", {}, "Fill tables for workload with data")
    , RunConfig(runConfig)
{
}

int TCommandTPCCImport::Run(TConfig& config) {
    Y_UNUSED(config);
    Cout << "Not Implemented" << Endl;
    return 1;
}

//-----------------------------------------------------------------------------

class TCommandTPCCRun
    : public TYdbCommand
{
public:
    TCommandTPCCRun(std::shared_ptr<NTPCC::TRunConfig>& runConfig);
    ~TCommandTPCCRun() = default;

    virtual void Config(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    std::shared_ptr<NTPCC::TRunConfig> RunConfig;
};

TCommandTPCCRun::TCommandTPCCRun(std::shared_ptr<NTPCC::TRunConfig>& runConfig)
    : TYdbCommand("run", {}, "Run benchmark (When needed initial TPC-C data is generated)")
    , RunConfig(runConfig)
{
}

void TCommandTPCCRun::Config(TConfig& config) {
    TYdbCommand::Config(config);

    config.Opts->AddLongOption(
        'p', "path", TStringBuilder() << "Path to TPC-C data")
            .RequiredArgument("STRING").StoreResult(&RunConfig->Path);

    config.Opts->AddLongOption(
        'w', "warehouses", TStringBuilder() << "Number of warehouses")
            .RequiredArgument("INT").StoreResult(&RunConfig->WarehouseCount).DefaultValue(DEFAULT_WAREHOUSE_COUNT);

    // TODO: default value should be auto
    config.Opts->AddLongOption(
        "warmup", TStringBuilder() << "Warmup time in minutes")
            .RequiredArgument("INT").StoreResult(&RunConfig->WarmupMinutes).DefaultValue(DEFAULT_WARMUP_MINUTES);
    config.Opts->AddLongOption(
        't', "time", TStringBuilder() << "Execution time in minutes")
            .RequiredArgument("INT").StoreResult(&RunConfig->RunMinutes).DefaultValue(DEFAULT_RUN_MINUTES);

    // TODO: default value should be auto
    config.Opts->AddLongOption(
        'm', "max-sessions", TStringBuilder() << "Soft limit on number of DB sessions (max inflight)")
            .RequiredArgument("INT").StoreResult(&RunConfig->MaxInflight).DefaultValue(DEFAULT_MAX_SESSIONS);

    // advanced options mainly for developers (all hidden)

    // TODO: hide this and detect automatically
    config.Opts->AddLongOption(
        "threads", TStringBuilder() << "TaskQueue threads (default: auto)")
            .RequiredArgument("INT").StoreResult(&RunConfig->ThreadCount).DefaultValue(0)
            .Hidden();
    config.Opts->AddLongOption(
        "connections", TStringBuilder() << "Number of client connections (default: auto)")
            .RequiredArgument("INT").StoreResult(&RunConfig->DriverCount).DefaultValue(0)
            .Hidden();
    config.Opts->AddLongOption(
        "no-delays", TStringBuilder() << "Disable keying/thinking delays")
            .Optional().StoreTrue(&RunConfig->NoDelays)
            .Hidden();
    config.Opts->AddLongOption(
        "simulate", TStringBuilder() << "Simulate transaction execution (delay is latency ms)")
            .OptionalArgument("INT").StoreResult(&RunConfig->SimulateTransactionMs).DefaultValue(0)
            .Hidden();
    config.Opts->AddLongOption(
        "simulate-select1", TStringBuilder() << "Instead of real queries, execute specified number of SELECT 1 queries")
            .OptionalArgument("INT").StoreResult(&RunConfig->SimulateTransactionSelect1Count).DefaultValue(0)
            .Hidden();
    config.Opts->AddLongOption(
        "log-level", TStringBuilder() << "Log level from 0 to 8, default is 6 (INFO)")
            .Optional().StoreMappedResult(&RunConfig->LogPriority, [](const TString& v) {
                int intValue = FromString<int>(v);
                return static_cast<ELogPriority>(intValue);
            }).DefaultValue(DEFAULT_LOG_LEVEL)
            .Hidden();
    config.Opts->AddLongOption(
        "developer", TStringBuilder() << "Developer mode")
            .Optional().StoreTrue(&RunConfig->Developer)
            .Hidden();
}

int TCommandTPCCRun::Run(TConfig& connectionConfig) {
    NTPCC::RunSync(connectionConfig, *RunConfig);
    return 0;
}

} // anonymous

//-----------------------------------------------------------------------------

TCommandTPCC::TCommandTPCC()
    : TClientCommandTree("tpcc", {}, "YDB TPC-C workload")
    , RunConfig(std::make_shared<NTPCC::TRunConfig>())
{
    AddCommand(std::make_unique<TCommandTPCCRun>(RunConfig));
    AddCommand(std::make_unique<TCommandTPCCClean>(RunConfig));
    AddHiddenCommand(std::make_unique<TCommandTPCCInit>(RunConfig));
    AddHiddenCommand(std::make_unique<TCommandTPCCImport>(RunConfig));
}

void TCommandTPCC::Config(TConfig& config) {
    TClientCommandTree::Config(config);

}

} // namespace NYdb::NConsoleClient
