#include "cmd_prepare.h"

#include <ydb/core/kqp/tests/tpch/lib/tpch_runner.h>


namespace NYdb::NTpch {

TCommandPrepare::TCommandPrepare()
    : TTpchCommandBase("prepare", {"p"}, "Prepare YDB database")
{}

void TCommandPrepare::Config(TConfig& config) {
    config.Opts->AddLongOption('s', "source", "Path to directory with source tables (*.tbl files)")
        .Required()
        .StoreResult(&SrcDataPath);
    config.Opts->AddLongOption('p', "partition-size", "Partition size (in MB)")
        .Required()
        .StoreResult(&PartitionSize);
    config.Opts->AddLongOption('b', "batch", "Uploading batch size (default 1000)")
        .Optional()
        .StoreResult(&BatchSize);
    config.SetFreeArgsNum(0);
}

int TCommandPrepare::Run(TConfig& config) {
    auto driver = CreateDriver(config);
    TTpchRunner tpch{driver, Path};
    tpch.UploadData(SrcDataPath, PartitionSize, BatchSize);
    driver.Stop(true);
    return 0;
}

} // namespace NYdb::NTpch
