#include "load_task.h"

#include <ydb/library/workload/tpcc/error.h>

using namespace NYdb;

namespace NYdbWorkload {
namespace NTPCC {

TLoadTask::TLoadTask(std::unique_ptr<TLoadQueryGenerator>&& queryGen,
                     std::shared_ptr<NTable::TTableClient> tableClient,
                     std::atomic_int32_t& numberOfFinished,
                     std::atomic_bool& failed,
                     TLog& log) 
    : QueryGen(std::move(queryGen))
    , TableClient(tableClient)
    , NumberOfFinished(numberOfFinished)
    , Failed(failed)
    , Log(log)
{
}

void TLoadTask::Process(void*) {
    try {
        TValue query = QueryGen->GetNextBatchLoadDDL();
        TLoadParams params = QueryGen->GetParams();
        
        auto retrySettings = NTable::TRetryOperationSettings()
            .MaxRetries(params.RetryCount);

        std::function<NTable::TAsyncBulkUpsertResult(NTable::TTableClient& tableClient)> bulkUpsertOp =
            [this, &params, &query] (NTable::TTableClient& db) {
                return db.BulkUpsert(
                    params.DbPath + "/" + ToString(QueryGen->GetType()),
                    TValue(query)
                ).Apply([this, &params](const NTable::TAsyncBulkUpsertResult& future) {
                    if (params.DebugMode && (future.HasException() || !future.GetValue().IsSuccess())) {
                        Log.Write(TLOG_DEBUG, ToString(QueryGen->GetType()) + ": Retrying...");
                    }

                    return future;
                });
            };

        while (true) {
            auto statusF = TableClient->RetryOperation(bulkUpsertOp, retrySettings);
            
            // We want to prepare the request while the previous one is running
            if (!QueryGen->Finished()) {
                TValue nextQuery = QueryGen->GetNextBatchLoadDDL();

                ThrowOnError(statusF.GetValueSync(), Log, __func__);

                query = std::move(nextQuery);
            } else {
                ThrowOnError(statusF.GetValueSync(), Log, __func__);
                
                NumberOfFinished++;
                PrintAfterFinish(NumberOfFinished.load(), QueryGen->GetType(), params.Threads);
                
                break;
            }
        }
    } catch(const TErrorException& ex) {
        Failed.store(true);
        TStringStream stream;
        stream << QueryGen->GetType() << ": " << ex;
        Log.Write(TLOG_ERR, stream.Str());
    } catch(const std::exception& ex) {
        Failed.store(true);
        Log.Write(TLOG_ERR, ToString(QueryGen->GetType()) + ": " + ex.what());
    } catch(...) {
        Failed.store(true);
        Log.Write(TLOG_ERR, ToString(QueryGen->GetType()) + ": Failed with an unknown error");
    }
}

void TLoadTask::PrintAfterFinish(int32_t numberOfFinished, ETablesType type, i32 threads) {
    if (!LoadTableFinished(numberOfFinished, type, threads)) {
        return;
    }

    TStringStream stream;
    stream << "Loading data into the table `" << type << "` is completed";
    Log.Write(stream.Str().c_str(), stream.Size());
}

bool TLoadTask::LoadTableFinished(int32_t numberOfFinished, ETablesType type, i32 threads) {
    switch (type) {
        // Heavy Tables
        case ETablesType::customer:
        case ETablesType::history:
        case ETablesType::new_order:
        case ETablesType::oorder:
        case ETablesType::order_line:
        case ETablesType::stock:
            if (numberOfFinished == threads) {
                return true;
            }
            return false;
        // Light Tables
        case ETablesType::warehouse:
        case ETablesType::item:
        case ETablesType::district:
            if (numberOfFinished == 1) {
                return true;
            }
            return false;
    }
}

}
}
