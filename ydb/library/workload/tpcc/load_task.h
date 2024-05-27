#pragma once

#include <ydb/library/workload/tpcc/load_data/query_generator.h>

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <util/thread/pool.h>
#include <library/cpp/logger/log.h>


namespace NYdbWorkload {
namespace NTPCC {

class TLoadTask : public IObjectInQueue {
public:
    TLoadTask(std::unique_ptr<TLoadQueryGenerator>&& queryGen, 
              std::shared_ptr<NYdb::NTable::TTableClient> tableClient,
              std::atomic_int32_t& numberOfFinished,
              std::atomic_bool& failed,
              TLog& log);

    void Process(void* threadResource) override;

private:
    void PrintAfterFinish(int32_t numberOfFinished, ETablesType type, i32 threads);

    bool LoadTableFinished(int32_t numberOfFinished, ETablesType type, i32 threads);

    std::unique_ptr<TLoadQueryGenerator> QueryGen;
    
    std::shared_ptr<NYdb::NTable::TTableClient> TableClient;

    std::atomic_int32_t& NumberOfFinished;
    std::atomic_bool& Failed;

    TLog& Log;
};

}
}
