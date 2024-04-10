#include <util/datetime/cputimer.h>

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

TUploader::TUploader(const TUploader::TOptions &opts, NYdb::NTable::TTableClient& client, const TString &query)
    : Opts(opts)
    , Query(query)
    , ShouldStop(0)
    , RequestLimiter(opts.GetRps(), opts.GetRps())
    , Client(client)
{
    TasksQueue = MakeSimpleShared<TThreadPool>(TThreadPool::TParams().SetBlocking(true).SetCatching(true));
    TasksQueue->Start(opts.InFly, opts.InFly + 1);
}

bool TUploader::Push(const TString& path, TValue&& value) {
    if (IsStopped()) {
        return false;
    }

    auto task = [this, taskValue = std::move(value), &path, retrySleep = BulkUpsertRetryDuration] () mutable {
        ui32 retry = 0;
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
                return Client.BulkUpsert(path, TValue(taskValue), settings).GetValueSync();
            };
            auto settings = NYdb::NTable::TRetryOperationSettings()
                .MaxRetries(Opts.RetryOperaionMaxRetries);
            auto status = Client.RetryOperationSync(upsert, settings);

            if (status.IsSuccess()) {
                if (status.GetIssues()) {
                    LOG_ERR("BulkUpsert has finished successfull, but has issues# {"
                            << status.GetIssues().ToString() << "}");
                }
                return;
            // Since upsert of data is an idempotent operation it is possible to retry transport errors
            } else if (status.IsTransportError() && retry < Opts.TransportErrorsMaxRetries) {
                LOG_DEBUG("Notice: transport error in BulkUpsert, issues# {" << status.GetIssues().ToString() << "}"
                        << " current Retry is " << retry
                        << " < MaxRetries# " << Opts.TransportErrorsMaxRetries
                        << ", so sleep for " << retrySleep.Seconds() << "s"
                        << " and try again");
                ++retry;
                TInstant deadline = retrySleep.ToDeadLine();
                while (TInstant::Now() < deadline) {
                    if (IsStopped()) {
                        return;
                    }
                    Sleep(TDuration::Seconds(1));
                }
                retrySleep *= 2;
                continue;
            } else {
                LOG_ERR("Error in BulkUpsert, so stop working. Issues# {" << status.GetIssues().ToString() << "}"
                        << " IsTransportError# " << (status.IsTransportError() ? "true" : "false")
                        << " retries done# " << retry);
                PleaseStop();
                return;
            }
        }
    };

    return TasksQueue->AddFunc(task);
}

bool TUploader::Push(TParams params) {
    if (IsStopped()) {
        return false;
    }

    auto upload = [this, params] (NYdb::NTable::TSession session) -> NYdb::TStatus {
        auto prepareSettings = NTable::TPrepareDataQuerySettings()
            .RequestType(DOC_API_REQUEST_TYPE);
        auto prepareResult = session.PrepareDataQuery(Query, prepareSettings).GetValueSync();
        if (!prepareResult.IsSuccess()) {
            return prepareResult;
        }

        auto dataQuery = prepareResult.GetQuery();
        auto transaction = NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx();
        auto settings = NTable::TExecDataQuerySettings()
            .RequestType(DOC_API_REQUEST_TYPE)
            .OperationTimeout(TDuration::Seconds(100))
            .ClientTimeout(TDuration::Seconds(120));
        return dataQuery.Execute(transaction, std::move(params), settings).GetValueSync();
    };

    auto task = [this, upload] () {
        while (!RequestLimiter.IsAvail()) {
            Sleep(Min(TDuration::MilliSeconds(RequestLimiter.GetWaitTime()), Opts.ReactionTime));
            if (IsStopped()) {
                return;
            }
        }

        if (IsStopped()) {
            return;
        }

        RequestLimiter.Use(1);

        auto settings = NYdb::NTable::TRetryOperationSettings()
            .MaxRetries(Opts.RetryOperaionMaxRetries)
            .FastBackoffSettings(NRetry::TBackoffSettings().SlotDuration(TDuration::MilliSeconds(10)).Ceiling(10))
            .SlowBackoffSettings(NRetry::TBackoffSettings().SlotDuration(TDuration::Seconds(2)).Ceiling(6))
            .Idempotent(true);

        auto status = Client.RetryOperationSync(upload, settings);


        if (status.IsSuccess()) {
            if (status.GetIssues()) {
                LOG_ERR("Upload tx has finished successfull, but has issues# {"
                    << status.GetIssues().ToString() << "}");
            }
        } else {
            LOG_ERR("Error in upload tx, issues# {" << status.GetIssues().ToString() << "}");
            PleaseStop();
            return;
        }
    };

    return TasksQueue->AddFunc(task);
}

} // NYdb::NBackup
