#pragma once

#include "sqs_workload_stats_collector.h"

#include <aws/core/Aws.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/utils/logging/LogLevel.h>
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
        bool Quiet = false;
        bool PrintTimestamp = false;
        double Percentile = 80.0;
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
        ui64 MaxUniqueMessages = 0;
        ui32 BatchSize = 1;
        ui32 MessageSize = 900;
        ui32 GroupsAmount = 0;
        ui32 WorkersCount = 1;
        ui32 RequestTimeoutMs = 2000;
        bool AwsSdkLog = false;
        bool UseXmlAPI = false;
        bool ValidateMessagesOrder = false;

        void InitAwsSdk();
        void DestroyAwsSdk();
        Aws::Utils::Logging::LogLevel GetAwsSdkLogLevel() const;
        Aws::Client::ClientConfiguration CreateSqsClientConfiguration() const;
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
