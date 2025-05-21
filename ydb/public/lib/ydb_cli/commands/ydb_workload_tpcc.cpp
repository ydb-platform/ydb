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
    int WarehouseCount;
    int WarmupSeconds;
    int RunSeconds;
};

TCommandTPCCRun::TCommandTPCCRun()
    : TYdbCommand("run", {}, "run benchmark")
    , WarehouseCount(DEFAULT_WAREHOUSE_COUNT)
    , WarmupSeconds(DEFAULT_WARMUP_SECONDS)
    , RunSeconds(DEFAULT_RUN_SECONDS)
{
}

TCommandTPCCRun::~TCommandTPCCRun() {
}

void TCommandTPCCRun::Config(TConfig& config) {
    config.Opts->AddLongOption(
        'w', "warehouses", TStringBuilder() << "Number of warehouses")
            .RequiredArgument("INT").StoreResult(&WarehouseCount).DefaultValue(DEFAULT_WAREHOUSE_COUNT);
    config.Opts->AddLongOption(
        "warmup", TStringBuilder() << "Warmup time in seconds")
            .RequiredArgument("INT").StoreResult(&WarmupSeconds).DefaultValue(DEFAULT_WARMUP_SECONDS);
    config.Opts->AddLongOption(
        't', "time", TStringBuilder() << "Execution time in seconds")
            .RequiredArgument("INT").StoreResult(&RunSeconds).DefaultValue(DEFAULT_RUN_SECONDS);
}

void TCommandTPCCRun::Parse(TConfig& config) {
    TClientCommand::Parse(config);

    // TODO: validate config?
}

int TCommandTPCCRun::Run(TConfig& config) {
    NTPCC::TRunConfig tpccConfig(config);
    tpccConfig.WarehouseCount = WarehouseCount;
    tpccConfig.WarmupSeconds = WarmupSeconds;
    tpccConfig.RunSeconds = RunSeconds;

    NTPCC::RunSync(tpccConfig);

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
