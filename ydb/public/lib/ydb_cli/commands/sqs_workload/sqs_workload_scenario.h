#pragma once

#include "sqs_workload_stats_collector.h"

#include <ydb/public/lib/ydb_cli/common/command.h>
#include <aws/core/Aws.h>
#include <library/cpp/logger/log.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/sqs/SQSClient.h>

namespace NYdb::NConsoleClient {

struct TSqsWorkloadScenario {
    TSqsWorkloadScenario();
    ~TSqsWorkloadScenario();

    TDuration TotalSec;
    TDuration WindowSec;
    TDuration WarmupSec;
    bool Quiet;
    bool PrintTimestamp;
    ui32 Percentile;
    std::shared_ptr<Aws::SQS::SQSClient> SqsClient;
    std::shared_ptr<TLog> Log;
    std::shared_ptr<std::atomic_bool> ErrorFlag;
    std::shared_ptr<TSqsWorkloadStatsCollector> StatsCollector;
    TString Token;
    TString QueueName;
    TString EndPoint;
    TString Account;
    ui32 BatchSize;
    ui32 MessageSize;
    ui32 SleepTimeMs;
    ui32 GroupsAmount;
    ui32 Concurrency;

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
