#include "sqs_workload_scenario.h"
#include "sqs_client_wrapper.h"

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/utils/ratelimiter/RateLimiterInterface.h>
#include <aws/core/utils/threading/Executor.h>
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

    void TSqsWorkloadScenario::InitSqsClient() {
        Aws::Client::ClientConfiguration sqsClientConfiguration;

        if (EndpointOverride.Defined()) {
            sqsClientConfiguration.endpointOverride =
                Aws::String(EndpointOverride->c_str(), EndpointOverride->size());
        }

        sqsClientConfiguration.scheme = Aws::Http::Scheme::HTTP;
        sqsClientConfiguration.httpRequestTimeoutMs = RequestTimeoutMs;
        sqsClientConfiguration.executor =
            Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>(
                "pooled-thread-executor", Concurrency);

        if (Region.Defined()) {
            sqsClientConfiguration.region = Aws::String(Region->c_str(), Region->size());
        }

        Aws::Auth::AWSCredentials credentials;
        Aws::String accountStr(Account.c_str(), Account.size());
        Aws::String tokenStr(Token.c_str(), Token.size());

        credentials.SetAWSAccessKeyId(accountStr.c_str());
        credentials.SetAWSSecretKey("unused");
        credentials.SetSessionToken(tokenStr.c_str());

        if (UseJsonAPI) {
            auto jsonSqsClient = Aws::MakeShared<TSQSJsonClient>(
                "json-sqs-client", credentials, sqsClientConfiguration);
            SqsClient = Aws::MakeShared<TSQSClientWrapper>(
                "sqs-client-wrapper", credentials, sqsClientConfiguration, jsonSqsClient, StatsCollector, ValidateFifo);
        } else {
            SqsClient = Aws::MakeShared<TSQSClientWrapper>(
                "sqs-client-wrapper", credentials, sqsClientConfiguration, Aws::MakeShared<Aws::SQS::SQSClient>(
                    "sqs-client", credentials, sqsClientConfiguration), StatsCollector, ValidateFifo);
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
