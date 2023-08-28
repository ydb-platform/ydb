#pragma once

#include "tpcc_config.h"

#include <ydb/library/workload/tpcc/load_data/load_thread_pool.h>

#include <ydb/public/lib/ydb_cli/common/command.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>

#include <library/cpp/logger/log.h>


namespace NYdbWorkload {
namespace NTPCC {

using namespace NYdb;
using namespace NConsoleClient;

class TTPCCWorkload {
public:
    TTPCCWorkload(std::shared_ptr<TDriver>& driver, TTPCCWorkloadParams& params);

    ~TTPCCWorkload() = default;
    
    int InitTables();

    int CleanTables();

    int RunWorkload();

private:
    void ConfigurePartitioning();

    TString CreateTablesQuery();
    TString CleanTablesQuery();

    TString PartitionByKeys(i32 keysCount, i32 partsCount);

    TTPCCWorkloadParams Params;
    std::unique_ptr<TLoadThreadPool> LoadThreadPool;

    std::shared_ptr<TDriver> Driver;
    
    TLog Log;
    ui64 Seed;
    TFastRng64 Rng;
};

}
}
