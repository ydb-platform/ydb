#include "kv_workload.h"

#include <ydb/library/workload/kv_workload.h>
#include <ydb/library/workload/workload_factory.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_common.h>

namespace NYdb::NConsoleClient {

TCommandKv::TCommandKv()
    : TClientCommandTree("kv", {}, "YDB Key-Value workload")
{
    AddCommand(std::make_unique<TCommandKvInit>());
    AddCommand(std::make_unique<TCommandKvClean>());
    AddCommand(std::make_unique<TCommandKvRun>());
}

TCommandKvInit::TCommandKvInit()
    : TWorkloadCommand("init", {}, "Create and initialize a table for workload")
    , InitRowCount(NYdbWorkload::KvWorkloadConstants::INIT_ROW_COUNT)
    , MinPartitions(NYdbWorkload::KvWorkloadConstants::MIN_PARTITIONS)
    , MaxFirstKey(NYdbWorkload::KvWorkloadConstants::MAX_FIRST_KEY)
    , StringLen(NYdbWorkload::KvWorkloadConstants::STRING_LEN)
    , ColumnsCnt(NYdbWorkload::KvWorkloadConstants::COLUMNS_CNT)
    , IntColumnsCnt(NYdbWorkload::KvWorkloadConstants::INT_COLUMNS_CNT)
    , KeyColumnsCnt(NYdbWorkload::KvWorkloadConstants::KEY_COLUMNS_CNT)
    , RowsCnt(NYdbWorkload::KvWorkloadConstants::ROWS_CNT)
    , PartitionsByLoad(NYdbWorkload::KvWorkloadConstants::PARTITIONS_BY_LOAD)
{}

void TCommandKvInit::Config(TConfig& config) {
    TYdbCommand::Config(config);

    config.SetFreeArgsNum(0);

    config.Opts->AddLongOption("init-upserts", "count of upserts need to create while table initialization")
        .DefaultValue(NYdbWorkload::KvWorkloadConstants::INIT_ROW_COUNT).StoreResult(&InitRowCount);
    config.Opts->AddLongOption("min-partitions", "Minimum partitions for tables.")
        .DefaultValue(NYdbWorkload::KvWorkloadConstants::MIN_PARTITIONS).StoreResult(&MinPartitions);
    config.Opts->AddLongOption("auto-partition", "Enable auto partitioning by load.")
        .DefaultValue(NYdbWorkload::KvWorkloadConstants::PARTITIONS_BY_LOAD).StoreResult(&PartitionsByLoad);
    config.Opts->AddLongOption("max-first-key", "Maximum value of a first primary key")
        .DefaultValue(NYdbWorkload::KvWorkloadConstants::MAX_FIRST_KEY).StoreResult(&MaxFirstKey);
    config.Opts->AddLongOption("len", "String len")
        .DefaultValue(NYdbWorkload::KvWorkloadConstants::STRING_LEN).StoreResult(&StringLen);
    config.Opts->AddLongOption("cols", "Number of columns")
        .DefaultValue(NYdbWorkload::KvWorkloadConstants::COLUMNS_CNT).StoreResult(&ColumnsCnt);
    config.Opts->AddLongOption("int-cols", "Number of int columns")
        .DefaultValue(NYdbWorkload::KvWorkloadConstants::INT_COLUMNS_CNT).StoreResult(&IntColumnsCnt);
    config.Opts->AddLongOption("key-cols", "Number of key columns")
        .DefaultValue(NYdbWorkload::KvWorkloadConstants::KEY_COLUMNS_CNT).StoreResult(&KeyColumnsCnt);
    config.Opts->AddLongOption("rows", "Number of rows")
        .DefaultValue(NYdbWorkload::KvWorkloadConstants::ROWS_CNT).StoreResult(&RowsCnt);
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
    params.IntColumnsCnt = IntColumnsCnt;
    params.KeyColumnsCnt = KeyColumnsCnt;
    params.RowsCnt = RowsCnt;

    NYdbWorkload::TWorkloadFactory factory;
    auto workloadGen = factory.GetWorkloadQueryGenerator(NYdbWorkload::EWorkload::KV, &params);

    return InitTables(workloadGen);
}


TCommandKvClean::TCommandKvClean()
    : TWorkloadCommand("clean", {}, "Drop table created in init phase") {}

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
    : TClientCommandTree("run", {}, "Run YDB Key-Value workload")
{
    AddCommand(std::make_unique<TCommandKvRunUpsertRandom>());
    AddCommand(std::make_unique<TCommandKvRunInsertRandom>());
    AddCommand(std::make_unique<TCommandKvRunSelectRandom>());
    AddCommand(std::make_unique<TCommandKvRunReadRowsRandom>());
}

TCommandKvRunUpsertRandom::TCommandKvRunUpsertRandom()
    : TWorkloadCommand("upsert", {}, "Upsert random rows into table")
{}

void TCommandKvRunUpsertRandom::Config(TConfig& config) {
    TWorkloadCommand::Config(config);
    config.SetFreeArgsNum(0);

    config.Opts->AddLongOption("max-first-key", "Maximum value of a first primary key")
        .DefaultValue(NYdbWorkload::KvWorkloadConstants::MAX_FIRST_KEY).StoreResult(&MaxFirstKey);
    config.Opts->AddLongOption("len", "String len")
        .DefaultValue(NYdbWorkload::KvWorkloadConstants::STRING_LEN).StoreResult(&StringLen);
    config.Opts->AddLongOption("cols", "Number of columns to upsert")
        .DefaultValue(NYdbWorkload::KvWorkloadConstants::COLUMNS_CNT).StoreResult(&ColumnsCnt);
    config.Opts->AddLongOption("int-cols", "Number of int columns")
        .DefaultValue(NYdbWorkload::KvWorkloadConstants::INT_COLUMNS_CNT).StoreResult(&IntColumnsCnt);
    config.Opts->AddLongOption("key-cols", "Number of key columns")
        .DefaultValue(NYdbWorkload::KvWorkloadConstants::KEY_COLUMNS_CNT).StoreResult(&KeyColumnsCnt);
    config.Opts->AddLongOption("rows", "Number of rows to upsert")
        .DefaultValue(NYdbWorkload::KvWorkloadConstants::ROWS_CNT).StoreResult(&RowsCnt);
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
    params.IntColumnsCnt = IntColumnsCnt;
    params.KeyColumnsCnt = KeyColumnsCnt;
    params.RowsCnt = RowsCnt;

    NYdbWorkload::TWorkloadFactory factory;
    auto workloadGen = factory.GetWorkloadQueryGenerator(NYdbWorkload::EWorkload::KV, &params);

    return RunWorkload(workloadGen, static_cast<int>(NYdbWorkload::TKvWorkloadGenerator::EType::UpsertRandom));
}

TCommandKvRunInsertRandom::TCommandKvRunInsertRandom()
    : TWorkloadCommand("insert", {}, "Insert random rows into table")
{}

void TCommandKvRunInsertRandom::Config(TConfig& config) {
    TWorkloadCommand::Config(config);
    config.SetFreeArgsNum(0);

    config.Opts->AddLongOption("max-first-key", "Maximum value of a first primary key")
        .DefaultValue(NYdbWorkload::KvWorkloadConstants::MAX_FIRST_KEY).StoreResult(&MaxFirstKey);
    config.Opts->AddLongOption("len", "String len")
        .DefaultValue(NYdbWorkload::KvWorkloadConstants::STRING_LEN).StoreResult(&StringLen);
    config.Opts->AddLongOption("cols", "Number of columns insert")
        .DefaultValue(NYdbWorkload::KvWorkloadConstants::COLUMNS_CNT).StoreResult(&ColumnsCnt);
    config.Opts->AddLongOption("int-cols", "Number of int columns")
        .DefaultValue(NYdbWorkload::KvWorkloadConstants::INT_COLUMNS_CNT).StoreResult(&IntColumnsCnt);
    config.Opts->AddLongOption("key-cols", "Number of key columns")
        .DefaultValue(NYdbWorkload::KvWorkloadConstants::KEY_COLUMNS_CNT).StoreResult(&KeyColumnsCnt);
    config.Opts->AddLongOption("rows", "Number of rows to insert")
        .DefaultValue(NYdbWorkload::KvWorkloadConstants::ROWS_CNT).StoreResult(&RowsCnt);
}

void TCommandKvRunInsertRandom::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandKvRunInsertRandom::Run(TConfig& config) {
    PrepareForRun(config);

    NYdbWorkload::TKvWorkloadParams params;
    params.DbPath = config.Database;
    params.MaxFirstKey = MaxFirstKey;
    params.StringLen = StringLen;
    params.ColumnsCnt = ColumnsCnt;
    params.IntColumnsCnt = IntColumnsCnt;
    params.KeyColumnsCnt = KeyColumnsCnt;
    params.RowsCnt = RowsCnt;

    NYdbWorkload::TWorkloadFactory factory;
    auto workloadGen = factory.GetWorkloadQueryGenerator(NYdbWorkload::EWorkload::KV, &params);

    return RunWorkload(workloadGen, static_cast<int>(NYdbWorkload::TKvWorkloadGenerator::EType::InsertRandom));
}

TCommandKvRunSelectRandom::TCommandKvRunSelectRandom()
    : TWorkloadCommand("select", {}, "Select rows matching primary key(s)")
{}

void TCommandKvRunSelectRandom::Config(TConfig& config) {
    TWorkloadCommand::Config(config);
    config.SetFreeArgsNum(0);

    config.Opts->AddLongOption("max-first-key", "Maximum value of a first primary key")
        .DefaultValue(NYdbWorkload::KvWorkloadConstants::MAX_FIRST_KEY).StoreResult(&MaxFirstKey);
    config.Opts->AddLongOption("cols", "Number of columns to select for a single query")
        .DefaultValue(NYdbWorkload::KvWorkloadConstants::COLUMNS_CNT).StoreResult(&ColumnsCnt);
    config.Opts->AddLongOption("int-cols", "Number of int columns")
        .DefaultValue(NYdbWorkload::KvWorkloadConstants::INT_COLUMNS_CNT).StoreResult(&IntColumnsCnt);
    config.Opts->AddLongOption("key-cols", "Number of key columns")
        .DefaultValue(NYdbWorkload::KvWorkloadConstants::KEY_COLUMNS_CNT).StoreResult(&KeyColumnsCnt);
    config.Opts->AddLongOption("rows", "Number of rows to select for a single query")
        .DefaultValue(NYdbWorkload::KvWorkloadConstants::ROWS_CNT).StoreResult(&RowsCnt);
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
    params.IntColumnsCnt = IntColumnsCnt;
    params.KeyColumnsCnt = KeyColumnsCnt;
    params.RowsCnt = RowsCnt;

    NYdbWorkload::TWorkloadFactory factory;
    auto workloadGen = factory.GetWorkloadQueryGenerator(NYdbWorkload::EWorkload::KV, &params);

    return RunWorkload(workloadGen, static_cast<int>(NYdbWorkload::TKvWorkloadGenerator::EType::SelectRandom));
}

TCommandKvRunReadRowsRandom::TCommandKvRunReadRowsRandom()
    : TWorkloadCommand("read-rows", {}, "ReadRows rows matching primary key(s)")
{}

void TCommandKvRunReadRowsRandom::Config(TConfig& config) {
    TWorkloadCommand::Config(config);
    config.SetFreeArgsNum(0);

    config.Opts->AddLongOption("max-first-key", "Maximum value of a first primary key")
        .DefaultValue(NYdbWorkload::KvWorkloadConstants::MAX_FIRST_KEY).StoreResult(&MaxFirstKey);
    config.Opts->AddLongOption("cols", "Number of columns to select for a single query")
        .DefaultValue(NYdbWorkload::KvWorkloadConstants::COLUMNS_CNT).StoreResult(&ColumnsCnt);
    config.Opts->AddLongOption("int-cols", "Number of int columns")
        .DefaultValue(NYdbWorkload::KvWorkloadConstants::INT_COLUMNS_CNT).StoreResult(&IntColumnsCnt);
    config.Opts->AddLongOption("key-cols", "Number of key columns")
        .DefaultValue(NYdbWorkload::KvWorkloadConstants::KEY_COLUMNS_CNT).StoreResult(&KeyColumnsCnt);
    config.Opts->AddLongOption("rows", "Number of rows to select for a single query")
        .DefaultValue(NYdbWorkload::KvWorkloadConstants::ROWS_CNT).StoreResult(&RowsCnt);
}

void TCommandKvRunReadRowsRandom::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandKvRunReadRowsRandom::Run(TConfig& config) {
    PrepareForRun(config);

    NYdbWorkload::TKvWorkloadParams params;
    params.DbPath = config.Database;
    params.MaxFirstKey = MaxFirstKey;
    params.ColumnsCnt = ColumnsCnt;
    params.IntColumnsCnt = IntColumnsCnt;
    params.KeyColumnsCnt = KeyColumnsCnt;
    params.RowsCnt = RowsCnt;

    NYdbWorkload::TWorkloadFactory factory;
    auto workloadGen = factory.GetWorkloadQueryGenerator(NYdbWorkload::EWorkload::KV, &params);

    return RunWorkload(workloadGen, static_cast<int>(NYdbWorkload::TKvWorkloadGenerator::EType::ReadRowsRandom));
}

} // namespace NYdb::NConsoleClient
