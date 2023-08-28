#pragma once

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/library/workload/tpcc/load_data/query_generator.h>

#include <util/thread/pool.h>
#include <library/cpp/logger/log.h>

namespace NYdbWorkload {
namespace NTPCC {

using namespace NYdb;

class TLoadThreadPool : public TThreadPool {
public:
    TLoadThreadPool(std::shared_ptr<TDriver>& driver);

protected:
    void* CreateThreadSpecificResource() override;

    void DestroyThreadSpecificResource(void* resource) override;

private:
    std::shared_ptr<TDriver> Driver;
};


class TLoadTask : public IObjectInQueue {
public:
    TLoadTask(std::unique_ptr<TLoadQueryGenerator>&& queryGen);

    void Process(void* threadResource) override;

private:
    std::unique_ptr<TLoadQueryGenerator> QueryGen;
};

}
}
