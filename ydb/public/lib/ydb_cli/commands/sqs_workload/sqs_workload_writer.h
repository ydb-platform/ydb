#pragma once

#include "sqs_workload_stats_collector.h"

#include <aws/core/utils/threading/Executor.h>
#include <aws/sqs/SQSClient.h>
#include <library/cpp/logger/log.h>
#include <ydb/public/lib/ydb_cli/common/command.h>

namespace NYdb::NConsoleClient {

    struct TSqsWorkloadWriterParams {
        TDuration TotalSec;
        TString QueueUrl;
        TMaybe<TString> AwsAccessKeyId;
        TMaybe<TString> AwsSessionToken;
        TMaybe<TString> AwsSecretKey;
        std::shared_ptr<TLog> Log;
        std::shared_ptr<std::mutex> Mutex;
        std::shared_ptr<std::condition_variable> FinishedCond;
        std::shared_ptr<size_t> StartedCount;
        std::shared_ptr<std::atomic_bool> ErrorFlag;
        std::shared_ptr<Aws::SQS::SQSClient> SqsClient;
        std::shared_ptr<TSqsWorkloadStatsCollector> StatsCollector;
        ui64 MaxUniqueMessages;
        ui32 BatchSize;
        ui32 WorkersCount;
        ui32 GroupsAmount;
        ui32 MessageSize;
    };

    class TSqsWorkloadWriter {
    public:
        static void RunLoop(const TSqsWorkloadWriterParams& params,
                            TInstant endTime);

    private:
        static void OnMessageSent(
            const TSqsWorkloadWriterParams& params,
            const Aws::SQS::SQSClient* sqsClient,
            const Aws::SQS::Model::SendMessageBatchRequest& sendMessageBatchRequest,
            const Aws::SQS::Model::SendMessageBatchOutcome& outcome,
            const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context);
    };

} // namespace NYdb::NConsoleClient
