#include "query_uploader.h"
#include "util.h"

namespace NYdb::NBackup {

static const char DOC_API_REQUEST_TYPE[] = "_document_api_request";

////////////////////////////////////////////////////////////////////////////////
// TUploader
////////////////////////////////////////////////////////////////////////////////

ui32 TUploader::TOptions::GetRps() const {
    return Rate * TDuration::Seconds(1).MilliSeconds() / Interval.MilliSeconds();
}

TUploader::TUploader(
        const TUploader::TOptions& opts,
        const TString& query,
        NYdb::NTable::TTableClient* tableClient,
        NYdb::NQuery::TQueryClient* queryClient)
    : Opts(opts)
    , Query(query)
    , ShouldStop(0)
    , RequestLimiter(opts.GetRps(), opts.GetRps())
    , TableClient(tableClient)
    , QueryClient(queryClient)
{
    TasksQueue = MakeSimpleShared<TThreadPool>(TThreadPool::TParams().SetBlocking(true).SetCatching(true));
    TasksQueue->Start(opts.InFly, opts.InFly + 1);
}

TUploader::TUploader(const TUploader::TOptions& opts, NYdb::NTable::TTableClient& tableClient, const TString& query)
    : TUploader(opts, query, &tableClient, nullptr)
{
}

TUploader::TUploader(const TUploader::TOptions& opts, NYdb::NQuery::TQueryClient& queryClient, const TString& query)
    : TUploader(opts, query, nullptr, &queryClient)
{
}

bool TUploader::Push(const TString& path, TValue&& value) {
    Y_ENSURE(TableClient, "Bulk upsert requires a TableClient-backed TUploader");

    if (IsStopped()) {
        return false;
    }

    auto task = [this, taskValue = std::move(value), &path] () mutable {
        while (true) {
            while (!RequestLimiter.IsAvail()) {
                Sleep(Min(TDuration::MicroSeconds(RequestLimiter.GetWaitTime()), Opts.ReactionTime));
                if (IsStopped()) {
                    return;
                }
            }

            if (IsStopped()) {
                return;
            }

            RequestLimiter.Use(1);

            auto upsert = [&] (NYdb::NTable::TSession) -> TStatus {
                auto settings = NTable::TBulkUpsertSettings()
                    .RequestType(DOC_API_REQUEST_TYPE)
                    .OperationTimeout(TDuration::Seconds(30))
                    .ClientTimeout(TDuration::Seconds(35));

                // Make copy of taskValue to save initial data for case of error
                return TableClient->BulkUpsert(path, TValue(taskValue), settings).GetValueSync();
            };
            auto settings = NYdb::NTable::TRetryOperationSettings()
                .MaxRetries(Opts.RetryOperationMaxRetries)
                .Idempotent(true);
            auto status = TableClient->RetryOperationSync(upsert, settings);

            if (status.IsSuccess()) {
                if (status.GetIssues()) {
                    LOG_W("Bulk upsert was completed with issues: " << status.GetIssues().ToOneLineString());
                }
                return;
            } else {
                LOG_E("Bulk upsert failed: " << status.GetIssues().ToOneLineString());
                PleaseStop();
                return;
            }
        }
    };

    return TasksQueue->AddFunc(task);
}

void TUploader::ReportWriteTxResult(const NYdb::TStatus& status) {
    if (status.IsSuccess()) {
        if (status.GetIssues()) {
            LOG_W("Write tx was completed with issues: " << status.GetIssues().ToOneLineString());
        }
        return;
    }

    LOG_E("Write tx failed: " << status.GetIssues().ToOneLineString());
    PleaseStop();
}

bool TUploader::WaitForRequestSlot() {
    while (!RequestLimiter.IsAvail()) {
        Sleep(Min(TDuration::MilliSeconds(RequestLimiter.GetWaitTime()), Opts.ReactionTime));
        if (IsStopped()) {
            return false;
        }
    }

    if (IsStopped()) {
        return false;
    }

    RequestLimiter.Use(1);
    return true;
}

bool TUploader::Push(TParams params) {
    Y_ENSURE(TableClient || QueryClient);

    if (IsStopped()) {
        return false;
    }

    auto task = [this, params] () {
        if (!WaitForRequestSlot()) {
            return;
        }

        auto retrySettings = NRetry::TRetryOperationSettings()
            .MaxRetries(Opts.RetryOperationMaxRetries)
            .FastBackoffSettings(NRetry::TBackoffSettings().SlotDuration(TDuration::MilliSeconds(10)).Ceiling(10))
            .SlowBackoffSettings(NRetry::TBackoffSettings().SlotDuration(TDuration::Seconds(2)).Ceiling(6))
            .Idempotent(true);

        if (QueryClient) {
            auto upload = [this, params] (NYdb::NQuery::TSession session) -> NYdb::TStatus {
                auto transaction = NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::SerializableRW()).CommitTx();
                auto settings = NYdb::NQuery::TExecuteQuerySettings()
                    .Syntax(NYdb::NQuery::ESyntax::YqlV1)
                    .StatsMode(NYdb::NQuery::EStatsMode::None)
                    .RequestType(DOC_API_REQUEST_TYPE)
                    .ClientTimeout(TDuration::Seconds(120));
                return session.ExecuteQuery(Query, transaction, std::move(params), settings).GetValueSync();
            };
            ReportWriteTxResult(QueryClient->RetryQuerySync(upload, retrySettings));
            return;
        }

        auto upload = [this, params] (NYdb::NTable::TSession session) -> NYdb::TStatus {
            auto transaction = NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx();
            auto settings = NTable::TExecDataQuerySettings()
                .KeepInQueryCache(true)
                .RequestType(DOC_API_REQUEST_TYPE)
                .OperationTimeout(TDuration::Seconds(100))
                .ClientTimeout(TDuration::Seconds(120));
            return session.ExecuteDataQuery(Query, transaction, std::move(params), settings).GetValueSync();
        };
        ReportWriteTxResult(TableClient->RetryOperationSync(upload, retrySettings));
    };

    return TasksQueue->AddFunc(task);
}

} // NYdb::NBackup
