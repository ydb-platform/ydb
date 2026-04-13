#pragma once

#include "sqs_workload_stats_collector.h"

#include <aws/core/Aws.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/sqs/SQSClient.h>
#include <library/cpp/logger/log.h>
#include <ydb/public/lib/ydb_cli/common/command.h>

namespace NYdb::NConsoleClient {

    struct TSqsWorkloadScenario {
        TSqsWorkloadScenario();
        ~TSqsWorkloadScenario();

        TDuration TotalSec;
        TDuration WindowSec;
        TDuration WarmupSec;
        bool Quiet;
        bool PrintTimestamp;
        double Percentile;
        std::shared_ptr<Aws::SQS::SQSClient> SqsClient;
        std::shared_ptr<TLog> Log;
        std::shared_ptr<std::atomic_bool> ErrorFlag;
        std::shared_ptr<TSqsWorkloadStatsCollector> StatsCollector;
        TMaybe<TString> AwsSessionToken;
        TMaybe<TString> AwsSecretKey;
        TString Topic;
        TString Consumer;
        TMaybe<TString> QueueName;
        TMaybe<TString> AwsAccessKeyId;
        TMaybe<TString> AwsRegion;
        TString Endpoint;
        ui64 MaxUniqueMessages;
        ui32 BatchSize;
        ui32 MessageSize;
        ui32 GroupsAmount;
        ui32 WorkersCount;
        ui32 RequestTimeoutMs;
        bool UseXmlAPI;
        bool ValidateMessagesOrder;

        void InitAwsSdk();
        void DestroyAwsSdk();
        void InitStatsCollector(size_t writerCount, size_t readerCount);
        void InitSqsClient(const TClientCommand::TConfig& config);
        void DestroySqsClient();
        TString GetQueueUrl(TString topic, TString consumer, TMaybe<TString> queueName = Nothing()) const;

    private:
        Aws::SDKOptions AwsOptions;

        TString GetQueueEndpointFromUrl(const TString& queueUrl) const;
        TString BuildQueueName(TString topic, TString consumer, TMaybe<TString> queueName = Nothing()) const;

    protected:
        bool AnyErrors() const;
        bool AnyIncomingMessages() const;

        std::shared_ptr<std::mutex> Mutex;
        std::shared_ptr<std::condition_variable> FinishedCond;
        std::shared_ptr<size_t> StartedCount;
    };

} // namespace NYdb::NConsoleClient
