#include "kv_workload.h"

#include <ydb/library/workload/kv_workload.h>
#include <ydb/library/workload/workload_factory.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_common.h>

namespace NYdb::NConsoleClient {

TCommandKv::TCommandKv()
    : TClientCommandTree("kv", {}, "YDB kv workload")
{
    AddCommand(std::make_unique<TCommandKvInit>());
    AddCommand(std::make_unique<TCommandKvClean>());
    AddCommand(std::make_unique<TCommandKvRun>());
}

TCommandKvInit::TCommandKvInit()
    : TWorkloadCommand("init", {}, "Create and initialize tables for workload")
    , InitRowCount(1000)
    , MinPartitions(1)
    , MaxFirstKey(5000)
    , StringLen(8)
    , ColumnsCnt(2)
    , PartitionsByLoad(true)
{}

void TCommandKvInit::Config(TConfig& config) {
    TYdbCommand::Config(config);

    config.SetFreeArgsNum(0);

    config.Opts->AddLongOption("rows-cnt", "count of rows need to Insert while table initialization")
        .DefaultValue(1000).StoreResult(&InitRowCount);
    config.Opts->AddLongOption("min-partitions", "Minimum partitions for tables.")
        .DefaultValue(40).StoreResult(&MinPartitions);
    config.Opts->AddLongOption("auto-partition", "Enable auto partitioning by load.")
        .DefaultValue(true).StoreResult(&PartitionsByLoad);
    config.Opts->AddLongOption("max-first-key", "maximum value of first primary key")
        .DefaultValue(5000).StoreResult(&MaxFirstKey);
    config.Opts->AddLongOption("len", "String len")
        .DefaultValue(8).StoreResult(&StringLen);
    config.Opts->AddLongOption("cols", "Number of columns")
        .DefaultValue(2).StoreResult(&ColumnsCnt);
}

void TCommandKvInit::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandKvInit::Run(TConfig& config) {
    Driver = std::make_unique<NYdb::TDriver>(CreateDriver(config));
    TableClient = std::make_unique<NTable::TTableClient>(*Driver);
    NYdbWorkload::TKvWorkloadParams params;
    params.DbPath = config.Database;
    params.InitRowCount = InitRowCount;
    params.MinPartitions = MinPartitions;
    params.PartitionsByLoad = PartitionsByLoad;
    params.MaxFirstKey = MaxFirstKey;
    params.StringLen = StringLen;
    params.ColumnsCnt = ColumnsCnt;

    NYdbWorkload::TWorkloadFactory factory;
    auto workloadGen = factory.GetWorkloadQueryGenerator(NYdbWorkload::EWorkload::KV, &params);

    return InitTables(workloadGen);
}


TCommandKvClean::TCommandKvClean()
    : TWorkloadCommand("clean", {}, "drop tables created in init phase") {}

void TCommandKvClean::Config(TConfig& config) {
    TWorkloadCommand::Config(config);
    config.SetFreeArgsNum(0);
}

void TCommandKvClean::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandKvClean::Run(TConfig& config) {
    Driver = std::make_unique<NYdb::TDriver>(CreateDriver(config));
    TableClient = std::make_unique<NTable::TTableClient>(*Driver);
    NYdbWorkload::TKvWorkloadParams params;
    params.DbPath = config.Database;

    NYdbWorkload::TWorkloadFactory factory;
    auto workloadGen = factory.GetWorkloadQueryGenerator(NYdbWorkload::EWorkload::KV, &params);

    return CleanTables(workloadGen);
}

TCommandKvRun::TCommandKvRun()
    : TClientCommandTree("run", {}, "Run YDB KV workload")
{
    AddCommand(std::make_unique<TCommandKvRunUpsertRandom>());
    AddCommand(std::make_unique<TCommandKvRunSelectRandom>());
}

TCommandKvRunUpsertRandom::TCommandKvRunUpsertRandom()
    : TWorkloadCommand("upsert", {}, "upsert random pairs (a, b) into table")
{}

void TCommandKvRunUpsertRandom::Config(TConfig& config) {
    TWorkloadCommand::Config(config);
    config.SetFreeArgsNum(0);

    config.Opts->AddLongOption("max-first-key", "maximum value of first primary key")
        .DefaultValue(5000).StoreResult(&MaxFirstKey);
    config.Opts->AddLongOption("len", "String len")
        .DefaultValue(8).StoreResult(&StringLen);
    config.Opts->AddLongOption("cols", "Number of columns")
        .DefaultValue(2).StoreResult(&ColumnsCnt);
}

void TCommandKvRunUpsertRandom::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandKvRunUpsertRandom::Run(TConfig& config) {
    PrepareForRun(config);

    NYdbWorkload::TKvWorkloadParams params;
    params.DbPath = config.Database;
    params.MaxFirstKey = MaxFirstKey;
    params.StringLen = StringLen;
    params.ColumnsCnt = ColumnsCnt;

    NYdbWorkload::TWorkloadFactory factory;
    auto workloadGen = factory.GetWorkloadQueryGenerator(NYdbWorkload::EWorkload::KV, &params);

    return RunWorkload(workloadGen, static_cast<int>(NYdbWorkload::TKvWorkloadGenerator::EType::UpsertRandom));
}

TCommandKvRunSelectRandom::TCommandKvRunSelectRandom()
    : TWorkloadCommand("select", {}, "select row by exactly matching of a")
{}

void TCommandKvRunSelectRandom::Config(TConfig& config) {
    TWorkloadCommand::Config(config);
    config.SetFreeArgsNum(0);

    config.Opts->AddLongOption("max-first-key", "maximum value of first primary key")
        .DefaultValue(5000).StoreResult(&MaxFirstKey);
    config.Opts->AddLongOption("cols", "Number of columns")
        .DefaultValue(2).StoreResult(&ColumnsCnt);
}

void TCommandKvRunSelectRandom::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandKvRunSelectRandom::Run(TConfig& config) {
    PrepareForRun(config);

    NYdbWorkload::TKvWorkloadParams params;
    params.DbPath = config.Database;
    params.MaxFirstKey = MaxFirstKey;
    params.ColumnsCnt = ColumnsCnt;

    NYdbWorkload::TWorkloadFactory factory;
    auto workloadGen = factory.GetWorkloadQueryGenerator(NYdbWorkload::EWorkload::KV, &params);

    return RunWorkload(workloadGen, static_cast<int>(NYdbWorkload::TKvWorkloadGenerator::EType::SelectRandom));
}

} // namespace NYdb::NConsoleClient {
