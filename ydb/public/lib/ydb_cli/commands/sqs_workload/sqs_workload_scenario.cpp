#include "sqs_workload_scenario.h"
#include "sqs_client_wrapper.h"

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/utils/ratelimiter/RateLimiterInterface.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/sqs/model/GetQueueUrlRequest.h>
#include <library/cpp/logger/log.h>
#include <library/cpp/uri/http_url.h>
#include <util/string/builder.h>
#include <ydb/public/lib/ydb_cli/commands/sqs_workload/sqs_json/sqs_json_client.h>
#include <ydb/public/lib/ydb_cli/common/command.h>

#include <atomic>
#include <mutex>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/src/client/impl/internal/logger/log.h>
#undef INCLUDE_YDB_INTERNAL_H

namespace NYdb::NConsoleClient {

    TSqsWorkloadScenario::TSqsWorkloadScenario()
        : Log(std::make_shared<TLog>(CreateLogBackend(
              "cerr", ELogPriority::TLOG_DEBUG, true))),
        ErrorFlag(std::make_shared<std::atomic_bool>(false)),
        AwsOptions(),
        Mutex(std::make_shared<std::mutex>()),
        FinishedCond(std::make_shared<std::condition_variable>()),
        StartedCount(std::make_shared<size_t>(0))
    {
        Log->SetFormatter(GetPrefixLogFormatter(""));
    }

    TSqsWorkloadScenario::~TSqsWorkloadScenario() {}

    void TSqsWorkloadScenario::InitAwsSdk() {
        AwsOptions.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Debug;
        Aws::InitAPI(AwsOptions);
    }

    void TSqsWorkloadScenario::DestroyAwsSdk() {
        Aws::ShutdownAPI(AwsOptions);
    }

    void TSqsWorkloadScenario::InitStatsCollector(size_t writerCount, size_t readerCount) {
        StatsCollector = std::make_shared<TSqsWorkloadStatsCollector>(
            writerCount, readerCount, Quiet, PrintTimestamp, WindowSec.Seconds(),
            TotalSec.Seconds(), 0, Percentile, ErrorFlag);
    }

    void TSqsWorkloadScenario::InitSqsClient(const TClientCommand::TConfig& config) {
        Aws::Client::ClientConfiguration sqsClientConfiguration;

        sqsClientConfiguration.endpointOverride =
            Aws::String(Endpoint.c_str(), Endpoint.size());
        sqsClientConfiguration.scheme = Aws::Http::Scheme::HTTP;
        sqsClientConfiguration.httpRequestTimeoutMs = RequestTimeoutMs;
        sqsClientConfiguration.executor =
            Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>(
                "pooled-thread-executor", WorkersCount);

        if (AwsRegion.Defined()) {
            sqsClientConfiguration.region = Aws::String(AwsRegion->c_str(), AwsRegion->size());
        }

        Aws::Auth::AWSCredentials credentials;
        if (AwsAccessKeyId.Defined()) {
            Aws::String accessKeyIdStr(AwsAccessKeyId->c_str(), AwsAccessKeyId->size());
            credentials.SetAWSAccessKeyId(accessKeyIdStr.c_str());
        }

        if (AwsSessionToken.Defined()) {
            Aws::String sessionTokenStr(AwsSessionToken->c_str(), AwsSessionToken->size());
            credentials.SetSessionToken(sessionTokenStr.c_str());
        }

        if (AwsSecretKey.Defined()) {
            Aws::String secretKeyStr(AwsSecretKey->c_str(), AwsSecretKey->size());
            credentials.SetAWSSecretKey(secretKeyStr.c_str());
        }

        if (UseXmlAPI) {
            SqsClient = Aws::MakeShared<TSQSClientWrapper>(
                "sqs-client-wrapper", credentials, sqsClientConfiguration, Aws::MakeShared<Aws::SQS::SQSClient>(
                    "sqs-client", credentials, sqsClientConfiguration), StatsCollector, ValidateMessagesOrder);
        } else {
            auto jsonSqsClient = Aws::MakeShared<TSQSJsonClient>(
                "json-sqs-client",
                credentials,
                sqsClientConfiguration,
                config.UseIamAuth ? Aws::String(config.SecurityToken.c_str(), config.SecurityToken.size()) : Aws::String());
            SqsClient = Aws::MakeShared<TSQSClientWrapper>(
                "sqs-client-wrapper", credentials, sqsClientConfiguration, jsonSqsClient, StatsCollector, ValidateMessagesOrder);
        }
    }

    TString TSqsWorkloadScenario::BuildQueueName(TString topic, TString consumer, TMaybe<TString> queueName) const {
        if (queueName.Defined()) {
            return queueName.GetRef();
        }
        return topic + "@" + consumer;
    }

    TString TSqsWorkloadScenario::GetQueueUrl(TString topic, TString consumer, TMaybe<TString> queueName) const {
        Aws::SQS::Model::GetQueueUrlRequest request;
        request.SetQueueName(BuildQueueName(topic, consumer, queueName));
        auto outcome = SqsClient->GetQueueUrl(request);
        if (outcome.IsSuccess()) {
            return outcome.GetResult().GetQueueUrl().c_str();
        } else {
            Log->Write(ELogPriority::TLOG_ERR, "got error: " + outcome.GetError().GetMessage());
            return "";
        }
    }

    void TSqsWorkloadScenario::DestroySqsClient() {
        SqsClient.reset();
    }

    bool TSqsWorkloadScenario::AnyErrors() const {
        if (!ErrorFlag->load()) {
            return false;
        }

        Log->Write(ELogPriority::TLOG_EMERG,
                   "Problems occured while processing messages.");
        return true;
    }

    bool TSqsWorkloadScenario::AnyIncomingMessages() const {
        return false;
    }

} // namespace NYdb::NConsoleClient
