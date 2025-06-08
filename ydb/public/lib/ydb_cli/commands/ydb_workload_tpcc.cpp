#include "ydb_workload_tpcc.h"

#include <ydb/library/workload/tpcc/runner.h>

#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>

#include <util/system/info.h>

namespace NYdb::NConsoleClient {

namespace {

//-----------------------------------------------------------------------------

constexpr int DEFAULT_WAREHOUSE_COUNT = 1;
constexpr int DEFAULT_WARMUP_SECONDS = 60; // TODO
constexpr int DEFAULT_RUN_SECONDS = 60; // TODO
constexpr int DEFAULT_MAX_INFLIGHT = 100; // TODO
constexpr int DEFAULT_LOG_LEVEL = 6; // TODO: properly use enum

//-----------------------------------------------------------------------------

class TCommandTPCCRun
    : public TYdbCommand
{
public:
    TCommandTPCCRun();
    ~TCommandTPCCRun();

    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    NTPCC::TRunConfig RunConfig;
};

TCommandTPCCRun::TCommandTPCCRun()
    : TYdbCommand("run", {}, "run benchmark")
{
}

TCommandTPCCRun::~TCommandTPCCRun() {
}

void TCommandTPCCRun::Config(TConfig& config) {
    config.Opts->AddLongOption(
        'w', "warehouses", TStringBuilder() << "Number of warehouses")
            .RequiredArgument("INT").StoreResult(&RunConfig.WarehouseCount).DefaultValue(DEFAULT_WAREHOUSE_COUNT);
    config.Opts->AddLongOption(
        "warmup", TStringBuilder() << "Warmup time in seconds")
            .RequiredArgument("INT").StoreResult(&RunConfig.WarmupSeconds).DefaultValue(DEFAULT_WARMUP_SECONDS);
    config.Opts->AddLongOption(
        't', "time", TStringBuilder() << "Execution time in seconds")
            .RequiredArgument("INT").StoreResult(&RunConfig.RunSeconds).DefaultValue(DEFAULT_RUN_SECONDS);
    config.Opts->AddLongOption(
        'm', "max-inflight", TStringBuilder() << "Max terminal inflight")
            .RequiredArgument("INT").StoreResult(&RunConfig.MaxInflight).DefaultValue(DEFAULT_MAX_INFLIGHT);
    config.Opts->AddLongOption(
        "path", TStringBuilder() << "Path to TPC-C data")
            .RequiredArgument("STRING").StoreResult(&RunConfig.Path);

    // TODO: hide dev options

    config.Opts->AddLongOption(
        "threads", TStringBuilder() << "TaskQueue threads (default: auto)")
            .RequiredArgument("INT").StoreResult(&RunConfig.ThreadCount).DefaultValue(0);
    config.Opts->AddLongOption(
        "connections", TStringBuilder() << "Number of client connections (default: auto)")
            .RequiredArgument("INT").StoreResult(&RunConfig.DriverCount).DefaultValue(0);
    config.Opts->AddLongOption(
        "log-level", TStringBuilder() << "Log level from 0 to 8, default is 6 (INFO)")
            .Optional().StoreMappedResult(&RunConfig.LogPriority, [](const TString& v) {
                int intValue = FromString<int>(v);
                return static_cast<ELogPriority>(intValue);
            }).DefaultValue(DEFAULT_LOG_LEVEL);
    config.Opts->AddLongOption(
        "no-sleep", TStringBuilder() << "Disable keying/thinking")
            .Optional().StoreTrue(&RunConfig.NoSleep);
    config.Opts->AddLongOption(
        "developer", TStringBuilder() << "Developer mode")
            .Optional().StoreTrue(&RunConfig.Developer);
}

void TCommandTPCCRun::Parse(TConfig& config) {
    TClientCommand::Parse(config);

    // TODO: validate config?
}

int TCommandTPCCRun::Run(TConfig& connectionConfig) {
    NTPCC::RunSync(connectionConfig, RunConfig);
    return 0;
}

} // anonymous

//-----------------------------------------------------------------------------

TCommandTPCC::TCommandTPCC()
    : TClientCommandTree("tpcc", {}, "YDB TPC-C workload")
{
    AddCommand(std::make_unique<TCommandTPCCRun>());
}

} // namespace NYdb::NConsoleClient
