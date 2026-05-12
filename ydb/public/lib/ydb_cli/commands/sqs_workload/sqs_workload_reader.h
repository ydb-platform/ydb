#pragma once

#include "sqs_workload_stats_collector.h"
#include <aws/core/utils/threading/Executor.h>
#include <aws/sqs/SQSClient.h>
#include <library/cpp/logger/log.h>
#include <ydb/public/lib/ydb_cli/common/command.h>

namespace NYdb::NConsoleClient {

    struct TSqsWorkloadReaderParams {
        TDuration TotalSec;
        TString QueueUrl;
        TMaybe<TString> AwsAccessKeyId;
        TMaybe<TString> AwsSessionToken;
        std::shared_ptr<TLog> Log;
        std::shared_ptr<std::atomic_bool> ErrorFlag;
        std::shared_ptr<Aws::SQS::SQSClient> SqsClient;
        std::shared_ptr<std::mutex> Mutex;
        std::shared_ptr<std::condition_variable> FinishedCond;
        std::shared_ptr<size_t> StartedCount;
        ui32 WorkersCount;
        ui32 BatchSize;
        TMaybe<ui32> ErrorMessagesRate;
        TString ErrorMessagesPolicy;
        TDuration HandleMessageDelay;
        TDuration VisibilityTimeout;
        bool ValidateMessagesOrder;
        std::shared_ptr<std::mutex> HashMapMutex;
        std::shared_ptr<THashMap<TString, ui64>> LastReceivedMessageInGroup;
        std::shared_ptr<TSqsWorkloadStatsCollector> StatsCollector;
    };

    class TSqsWorkloadReader {
    public:
        static void RunLoop(const TSqsWorkloadReaderParams& params,
                            TInstant endTime);

    private:
        static void
        OnMessageReceived(const TSqsWorkloadReaderParams& params,
                          const Aws::SQS::SQSClient* client,
                          const Aws::SQS::Model::ReceiveMessageRequest& request,
                          const Aws::SQS::Model::ReceiveMessageOutcome& outcome);

        static bool ShouldFail(const TSqsWorkloadReaderParams& params, ui64 batchIndex, ui64 sendMessageTime);
        static bool ValidateFifo(const TSqsWorkloadReaderParams& params,
                                 const Aws::SQS::Model::Message& message,
                                 ui64 sendTimestamp);
    };

} // namespace NYdb::NConsoleClient
