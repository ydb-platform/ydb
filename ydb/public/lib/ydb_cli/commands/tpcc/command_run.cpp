#include "command_run.h"

#include "runner.h"

#include <util/system/info.h>

namespace NYdb::NConsoleClient {

namespace {
    constexpr int DEFAULT_WAREHOUSE_COUNT = 1;
    constexpr int DEFAULT_WARMUP_SECONDS = 60; // TODO
    constexpr int DEFAULT_RUN_SECONDS = 60; // TODO

} // anonymous

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

} // namespace NYdb::NConsoleClient
