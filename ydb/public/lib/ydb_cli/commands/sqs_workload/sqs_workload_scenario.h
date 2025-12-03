#pragma once

#include <ydb/public/lib/ydb_cli/common/command.h>
#include <aws/core/Aws.h>
#include <library/cpp/logger/log.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/sqs/SQSClient.h>

namespace NYdb::NConsoleClient {

struct TSqsWorkloadScenario {
    TSqsWorkloadScenario();
    ~TSqsWorkloadScenario();

    TDuration TotalSec = TDuration::Seconds(60);
    TDuration WindowSec = TDuration::Seconds(1);
    bool Quiet = false;
    bool PrintTimestamp = false;
    ui32 Percentile = 50;
    std::shared_ptr<Aws::SQS::SQSClient> SqsClient;
    std::shared_ptr<TLog> Log;
    std::shared_ptr<std::atomic_bool> ErrorFlag;
    TString Token;
    TString QueueName;
    TString EndPoint;
    TString Account;
    ui32 BatchSize;
    ui32 MessageSize;
    ui32 SleepTimeMs;
    ui32 Concurrency = 1;

    void InitSqsClient();
    void DestroySqsClient();
private:
    Aws::SDKOptions AwsOptions;

protected:
    bool AnyErrors() const;
    bool AnyIncomingMessages() const;

    std::shared_ptr<std::mutex> Mutex;
    std::shared_ptr<std::condition_variable> FinishedCond;
    std::shared_ptr<size_t> StartedCount;
};

} // namespace NYdb::NConsoleClient
