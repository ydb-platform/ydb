#include "tpcc_workload.h"

#include <ydb/library/workload/tpcc/tpcc_workload.h>

namespace NYdb {
namespace NConsoleClient {

TCommandTPCCWorkload::TCommandTPCCWorkload() 
    : TClientCommandTree("tpcc", {}, "YDB TPCC workload")
{
    AddCommand(std::make_unique<TTPCCInitCommand>());
    AddCommand(std::make_unique<TTPCCCleanCommand>());
}

TTPCCWorkloadCommand::TTPCCWorkloadCommand(const TString& name, const std::initializer_list<TString>& aliases, const TString& description)
    : TYdbCommand(name, aliases, description)
    , ClientTimeoutMs(0)
    , OperationTimeoutMs(0)
    , CancelAfterTimeoutMs(0)
    , WindowDurationSec(0)
    , Quiet(false)
    , PrintTimestamp(false)
    , WindowHist(60000, 2) // highestTrackableValue 60000ms = 60s, precision 2
    , TotalHist(60000, 2)
    , TotalRetries(0)
    , WindowRetryCount(0)
    , TotalErrors(0)
    , WindowErrors(0) {}

struct TTPCCWorkloadStats {
    ui64 OpsCount = 0;
    ui64 Percentile50 = 0;
    ui64 Percentile95 = 0;
    ui64 Percentile99 = 0;
    ui64 Percentile100 = 0;
};

void TTPCCWorkloadCommand::Config(TConfig& config) {
    TYdbCommand::Config(config);
    
    config.Opts->AddLongOption("quiet", "Quiet mode. Doesn't print statistics each second.")
        .StoreTrue(&Quiet);
    config.Opts->AddLongOption("print-timestamp", "Print timestamp each second with statistics.")
        .StoreTrue(&PrintTimestamp);
    config.Opts->AddLongOption("client-timeout", "Client timeout in ms.")
        .DefaultValue(1000).StoreResult(&ClientTimeoutMs);
    config.Opts->AddLongOption("operation-timeout", "Operation timeout in ms.")
        .DefaultValue(800).StoreResult(&OperationTimeoutMs);
    config.Opts->AddLongOption("cancel-after", "Cancel after timeout in ms.")
        .DefaultValue(800).StoreResult(&CancelAfterTimeoutMs);
    config.Opts->AddLongOption("window", "Window duration in seconds.")
        .DefaultValue(1).StoreResult(&WindowDurationSec);
}

void TTPCCWorkloadCommand::PrepareForRun(TConfig& config) {
    auto driverConfig = TDriverConfig()
        .SetEndpoint(config.Address)
        .SetDatabase(config.Database)
        .SetBalancingPolicy(EBalancingPolicy::UseAllNodes)
        .SetCredentialsProviderFactory(config.CredentialsGetter(config));
    Params.DbPath = config.Database;

    if (config.EnableSsl) {
        driverConfig.UseSecureConnection(config.CaCerts);
    }
    Driver = std::make_shared<NYdb::TDriver>(NYdb::TDriver(driverConfig));
    Workload = std::unique_ptr<TTPCCWorkload>(new TTPCCWorkload(Driver, Params));
}

TTPCCInitCommand::TTPCCInitCommand() 
    : TTPCCWorkloadCommand("init", {}, "Create and initialize tables for TPCC workload") {}

void TTPCCInitCommand::Config(TConfig& config) {
    TTPCCWorkloadCommand::Config(config);

    config.SetFreeArgsNum(0);

    config.Opts->AddLongOption('w', "warehouses", "The number of TPC-C warehouses.")
        .DefaultValue(static_cast<int>(ETPCCWorkloadConstants::TPCC_WAREHOUSES)).StoreResult(&Params.Warehouses);
    config.Opts->AddLongOption('t', "threads", "Number of parallel threads in workload.")
        .DefaultValue(static_cast<int>(ETPCCWorkloadConstants::TPCC_THREADS)).StoreResult(&Params.Threads);
    config.Opts->AddLongOption("load-batch-size", "The size of the batch of queries with which data is loaded.")
        .DefaultValue(static_cast<int>(ETPCCWorkloadConstants::TPCC_LOAD_BATCH_SIZE)).StoreResult(&Params.LoadBatchSize);
    config.Opts->AddLongOption("min-partitions", "Minimum partitioning of tables.")
        .DefaultValue(static_cast<int>(ETPCCWorkloadConstants::TPCC_MIN_PARTITIONS)).StoreResult(&Params.MinPartitions);
    config.Opts->AddLongOption("max-partitions", "Maximum partitioning of tables.")
        .DefaultValue(static_cast<int>(ETPCCWorkloadConstants::TPCC_MAX_PARTITIONS)).StoreResult(&Params.MaxPartitions);
    config.Opts->AddLongOption("auto-partitioning", "Enable auto partitioning by load.")
        .DefaultValue(static_cast<int>(ETPCCWorkloadConstants::TPCC_AUTO_PARTITIONING)).StoreResult(&Params.AutoPartitioning, true);
}

int TTPCCInitCommand::Run(TConfig& config) {
    PrepareForRun(config);
    Cout << "INIT COMMAND\n";
    Cout << "Warehouses: " << Params.Warehouses << "\nThreads: " << Params.Threads << "\n";
    return Workload->InitTables();
}

void TTPCCInitCommand::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

TTPCCCleanCommand::TTPCCCleanCommand() 
    : TTPCCWorkloadCommand("clean", {}, "Drop the TPCC tables.") {}

void TTPCCCleanCommand::Config(TConfig& config) {
    TTPCCWorkloadCommand::Config(config);
    config.SetFreeArgsNum(0);
}

int TTPCCCleanCommand::Run(TConfig& config) {
    PrepareForRun(config);
    return Workload->CleanTables();
}

void TTPCCCleanCommand::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

}
}
