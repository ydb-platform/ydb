#include "load_thread_pool.h"

#include <ydb/library/workload/tpcc/tpcc_thread_resource.h>

#include <ydb/public/api/protos/ydb_value.pb.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_common.h>

namespace NYdbWorkload {
namespace NTPCC {

TLoadThreadPool::TLoadThreadPool(std::shared_ptr<TDriver>& driver)
    : TThreadPool(TThreadPool::TParams().SetBlocking(true).SetCatching(true))
    , Driver(driver) {}

void* TLoadThreadPool::CreateThreadSpecificResource() {
    auto tableClientSettings = NTable::TClientSettings()
                        .SessionPoolSettings(
                            NTable::TSessionPoolSettings()
                                .MaxActiveSessions(1));
    return new TThreadResource(std::make_shared<NTable::TTableClient>(*Driver, tableClientSettings));
}

void TLoadThreadPool::DestroyThreadSpecificResource(void* resource) {
    delete static_cast<TThreadResource*>(resource);
}

TLoadTask::TLoadTask(std::unique_ptr<TLoadQueryGenerator>&& queryGen) 
    : QueryGen(std::move(queryGen)) {}

void TLoadTask::Process(void* threadResource) {
    try {
        int retryCount = 0;
        auto query = QueryGen->GetNextBatchLoadDDL();
        auto retrySettings = NYdb::NTable::TRetryOperationSettings()
                                        .MaxRetries(QueryGen->GetParams().RetryCount);
        auto& db = static_cast<TThreadResource*>(threadResource)->Client;

        auto upsertQuery = [this, &query, &retryCount] (NTable::TTableClient& tableClient) {
            TValue queryCopy(query);

            auto future = tableClient.BulkUpsert(
                QueryGen->GetParams().DbPath,
                std::move(queryCopy)
            );

            return future.Apply([&retryCount](const NYdb::NTable::TAsyncBulkUpsertResult& bulkUpsertResult) {
                NYdb::TStatus status = bulkUpsertResult.GetValueSync();
                if (!status.IsSuccess()) {
                    ++retryCount;
                } else if (status.GetIssues()) {
                    Cerr << status.GetIssues().ToString() << "\n";
                }
                return NThreading::MakeFuture(status);
            });
        };

        while (true) {
            auto statusF = db->RetryOperation(upsertQuery, retrySettings);

            // We want to prepare the request while the previous one is running
            if (!QueryGen->Finished()) {
                query = QueryGen->GetNextBatchLoadDDL();
            } else {
                NConsoleClient::ThrowOnError(statusF.GetValueSync());
                break;
            }
            NConsoleClient::ThrowOnError(statusF.GetValueSync());
            
            // TODO: Retry monitoring
            retryCount = 0;
        }

    } catch(NConsoleClient::TYdbErrorException ex) {
        Cerr << ex;
        throw;
    } catch(...) {
        throw;
    }
}


}
}
