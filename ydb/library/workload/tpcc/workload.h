#pragma once

#include <ydb/library/workload/tpcc/config.h>
#include <ydb/library/workload/tpcc/terminal.h>

#include <ydb/public/lib/ydb_cli/common/command.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>

#include <library/cpp/logger/log.h>
#include <library/cpp/threading/task_scheduler/task_scheduler.h>

#include <util/random/fast.h>
#include <util/thread/pool.h>


using namespace NYdb;

namespace NYdbWorkload {
namespace NTPCC {

class TTPCCWorkload {
public:
    TTPCCWorkload(std::shared_ptr<TDriver>& driver);

    ~TTPCCWorkload() = default;
    
    int InitTables();

    int CleanTables();

    int RunWorkload();

    void SetLoadParams(const TLoadParams& params);

    void SetRunParams(const TRunParams& params);

private:
    int ConfigurePartitioning();
    
    int CreateIndices();

    void CollectAndPrintResult();

    TString CreateTablesQuery();
    TString CleanTablesQuery();

    TString PartitionByWarehouses(i32 keysCount, i32 partsCount);

    TLoadParams LoadParams;
    TRunParams RunParams;

    std::shared_ptr<TThreadPool> ThreadPool;
    std::shared_ptr<TTaskScheduler> Scheduler;

    std::shared_ptr<TDriver> Driver;

    std::vector<std::shared_ptr<TTerminal>> Terminals;

    TLog Log;
    ui64 Seed;
    TMaybe<TFastRng64> Rng;
};


}
}
