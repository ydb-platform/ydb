#include "tpcc_workload.h"

#include <ydb/library/workload/tpcc/workload.h>

namespace NYdb {
namespace NConsoleClient {

TCommandTPCCWorkload::TCommandTPCCWorkload() 
    : TClientCommandTree("tpcc", {}, "YDB TPCC workload")
{
    AddCommand(std::make_unique<TTPCCInitCommand>());
    AddCommand(std::make_unique<TTPCCCleanCommand>());
    AddCommand(std::make_unique<TTPCCRunCommand>());
}


TTPCCWorkloadCommand::TTPCCWorkloadCommand(const TString& name, const std::initializer_list<TString>& aliases, const TString& description)
    : TYdbCommand(name, aliases, description) {}

void TTPCCWorkloadCommand::Config(TConfig& config) {
    TYdbCommand::Config(config);
}

void TTPCCWorkloadCommand::PrepareForRun(TConfig& config, i32 threadsNum) {
    auto driverConfig = TDriverConfig()
        .SetEndpoint(config.Address)
        .SetDatabase(config.Database)
        .SetNetworkThreadsNum(threadsNum)
        .SetBalancingPolicy(EBalancingPolicy::UseAllNodes)
        .SetCredentialsProviderFactory(config.CredentialsGetter(config));
    LoadParams.DbPath = config.Database;

    if (config.EnableSsl) {
        driverConfig.UseSecureConnection(config.CaCerts);
    }
    Driver = std::make_shared<NYdb::TDriver>(NYdb::TDriver(driverConfig));
    Workload = std::unique_ptr<TTPCCWorkload>(new TTPCCWorkload(Driver));
}


TTPCCInitCommand::TTPCCInitCommand() 
    : TTPCCWorkloadCommand("init", {}, "Create and initialize tables for TPCC workload") {}

void TTPCCInitCommand::Config(TConfig& config) {
    TTPCCWorkloadCommand::Config(config);

    config.SetFreeArgsNum(0);

    config.Opts->AddLongOption('w', "warehouses", "The number of TPC-C warehouses.")
        .DefaultValue(static_cast<int>(EWorkloadConstants::TPCC_WAREHOUSES)).StoreResult(&LoadParams.Warehouses);
    config.Opts->AddLongOption('t', "threads", "Number of parallel threads in workload.")
        .DefaultValue(static_cast<int>(EWorkloadConstants::TPCC_THREADS)).StoreResult(&LoadParams.Threads);
    config.Opts->AddLongOption("network-threads", "Number of network threads in workload.")
        .DefaultValue(static_cast<int>(EWorkloadConstants::TPCC_NETWORK_THREADS)).StoreResult(&LoadParams.NetworkThreads);
    config.Opts->AddLongOption("load-batch-size", "The size of the batch of queries with which data is loaded.")
        .DefaultValue(static_cast<int>(EWorkloadConstants::TPCC_LOAD_BATCH_SIZE)).StoreResult(&LoadParams.LoadBatchSize);
    config.Opts->AddLongOption("max-partitions", "Maximum partitioning of tables.")
        .DefaultValue(static_cast<int>(EWorkloadConstants::TPCC_MAX_PARTITIONS)).StoreResult(&LoadParams.MaxPartitions);
    config.Opts->AddLongOption("disable-auto-partitioning", "Disable auto partitioning by load.")
        .StoreTrue(&LoadParams.DisableAutoPartitioning);
    config.Opts->AddLongOption("only-configuring", "Only configuring partitioning stage.")
        .StoreTrue(&LoadParams.OnlyConfiguring);
    config.Opts->AddLongOption("only-creating-indices", "Only creating indices stage.")
        .StoreTrue(&LoadParams.OnlyCreatingIndices);
    config.Opts->AddLongOption("debug", "More logs.")
        .StoreTrue(&LoadParams.DebugMode);
    config.Opts->AddLongOption( "seed", "Seed of the launch.")
        .DefaultValue(-1).StoreResult(&LoadParams.Seed);
}

int TTPCCInitCommand::Run(TConfig& config) {
    PrepareForRun(config, LoadParams.NetworkThreads);
    Workload->SetLoadParams(LoadParams);
    
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
    PrepareForRun(config, 0);
    return Workload->CleanTables();
}

void TTPCCCleanCommand::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}


TTPCCRunCommand::TTPCCRunCommand() 
    : TTPCCWorkloadCommand("run", {}, "Run TPC-C benchmark.") {}

void TTPCCRunCommand::Config(TConfig& config) {
    TTPCCWorkloadCommand::Config(config);

    config.SetFreeArgsNum(0);

    config.Opts->AddLongOption('w', "warehouses", "The number of TPC-C warehouses.")
        .DefaultValue(static_cast<int>(EWorkloadConstants::TPCC_WAREHOUSES)).StoreResult(&RunParams.Warehouses);
    config.Opts->AddLongOption('s', "seconds", "Seconds to run workload.")
        .DefaultValue(static_cast<int>(EWorkloadConstants::TPCC_WORKLOAD_DURATION_SECONDS)).StoreResult(&RunParams.DurationS);
    config.Opts->AddLongOption('t', "threads", "Number of parallel threads in workload.")
        .DefaultValue(static_cast<int>(EWorkloadConstants::TPCC_THREADS)).StoreResult(&RunParams.Threads);
    config.Opts->AddLongOption("network-threads", "Number of network threads in workload.")
        .DefaultValue(static_cast<int>(EWorkloadConstants::TPCC_NETWORK_THREADS)).StoreResult(&RunParams.NetworkThreads);
    config.Opts->AddLongOption("warmup", "Seconds to warm up before run workload.")
        .DefaultValue(static_cast<int>(EWorkloadConstants::TPCC_WORKLOAD_WARMUP_SECONDS)).StoreResult(&RunParams.WarmupDurationS);
    config.Opts->AddLongOption("scheduler-threads", "Number of parallel threads in scheduler.")
        .DefaultValue(static_cast<int>(EWorkloadConstants::TPCC_SCHEDULER_THREADS)).StoreResult(&RunParams.SchedulerThreads);
    config.Opts->AddLongOption("start-id", "TPC-C will be launched in warehouses: start-id...start-id+warehouses-1. This parameter is needed to run the TPC-C on multiple machines.")
        .DefaultValue(1).StoreResult(&RunParams.WarehouseStartId);
    config.Opts->AddLongOption("seed", "Seed of the launch.")
        .StoreResult(&RunParams.Seed);
    config.Opts->AddLongOption("debug", "More logs. It is strongly recommended to turn on with a small number of warehouses or threads.")
        .StoreTrue(&RunParams.DebugMode);
    config.Opts->AddLongOption("with-errors", "Error logs. It is strongly recommended to turn on with a small number of warehouses or threads.")
        .StoreTrue(&RunParams.ErrorMode);
    config.Opts->AddLongOption("one-type", "Enable only one type of transaction. Write the number corresponding to the transaction(NewOrder=0, Payment=1, Delivery=2, OrderStatus=3, StockLevel=4)")
        .StoreResult(&RunParams.OnlyOneType);
    config.Opts->AddLongOption("max-active-sessions", "Max number of active session")
        .DefaultValue(static_cast<int>(EWorkloadConstants::TPCC_MAX_ACTIVE_SESSIONS)).StoreResult(&RunParams.MaxActiveSessions);
}

int TTPCCRunCommand::Run(TConfig& config) {
    if (RunParams.OnlyOneType < -1 || RunParams.OnlyOneType >= EProcedureType::COUNT) {
        Cerr << "There is no transaction with this number";
        return EXIT_FAILURE;
    }

    PrepareForRun(config, RunParams.NetworkThreads);
    Workload->SetRunParams(RunParams);

    return Workload->RunWorkload();
}

void TTPCCRunCommand::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

}
}
