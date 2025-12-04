#include "sqs_workload_scenario.h"
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/utils/threading/Executor.h>
#include <library/cpp/uri/http_url.h>
#include <ydb/public/lib/ydb_cli/common/command.h>
#include <library/cpp/logger/log.h>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/src/client/impl/internal/logger/log.h>
#undef INCLUDE_YDB_INTERNAL_H

namespace NYdb::NConsoleClient {

TSqsWorkloadScenario::TSqsWorkloadScenario()
    : Log(std::make_shared<TLog>(CreateLogBackend("cerr", ELogPriority::TLOG_DEBUG, true))),
      ErrorFlag(std::make_shared<std::atomic_bool>(false)),
      AwsOptions(),
      Mutex(std::make_shared<std::mutex>()),
      FinishedCond(std::make_shared<std::condition_variable>()),
      StartedCount(std::make_shared<size_t>(0)) {
    Log->SetFormatter(GetPrefixLogFormatter(""));
    AwsOptions.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Debug;
    Aws::InitAPI(AwsOptions);
}

TSqsWorkloadScenario::~TSqsWorkloadScenario() { Aws::ShutdownAPI(AwsOptions); }

void TSqsWorkloadScenario::InitSqsClient() {
    Aws::Client::ClientConfiguration sqsClientConfiguration;
    auto endpoint = GetQueueEndpointFromUrl(QueueUrl);
    sqsClientConfiguration.endpointOverride = Aws::String(endpoint.c_str(), endpoint.size());
    sqsClientConfiguration.scheme = Aws::Http::Scheme::HTTP;
    sqsClientConfiguration.maxConnections = Concurrency;
    sqsClientConfiguration.executor =
        Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>("pooled-thread-executor", Concurrency);

    Aws::Auth::AWSCredentials credentials;
    Aws::String accountStr(Account.c_str(), Account.size());
    Aws::String tokenStr(Token.c_str(), Token.size());
    credentials.SetAWSAccessKeyId(accountStr.c_str());
    credentials.SetAWSSecretKey("unused");
    credentials.SetSessionToken(tokenStr.c_str());

    SqsClient = Aws::MakeShared<Aws::SQS::SQSClient>("sqs-client", credentials, sqsClientConfiguration);
}

TString TSqsWorkloadScenario::GetQueueEndpointFromUrl(const TString& queueUrl) const {
    auto endpoint = queueUrl;
    if (queueUrl.StartsWith("http://")) {
        endpoint = queueUrl.substr(7);
        endpoint = endpoint.substr(0, endpoint.find('/'));
        return endpoint;
    }

    if (queueUrl.StartsWith("https://")) {
        endpoint = queueUrl.substr(8);
        endpoint = endpoint.substr(0, endpoint.find('/'));
        return endpoint;
    }

    if (queueUrl.StartsWith("grpc://")) {
        endpoint = queueUrl.substr(7);
        endpoint = endpoint.substr(0, endpoint.find('/'));
        return endpoint;
    }

    if (queueUrl.StartsWith("grpcs://")) {
        endpoint = queueUrl.substr(8);
        endpoint = endpoint.substr(0, endpoint.find('/'));
        return endpoint;
    }

    return queueUrl;
}

void TSqsWorkloadScenario::DestroySqsClient() { SqsClient.reset(); }

bool TSqsWorkloadScenario::AnyErrors() const {
    if (!ErrorFlag->load()) {
        return false;
    }

    Log->Write(ELogPriority::TLOG_EMERG, "Problems occured while processing messages.");

    return true;
}

bool TSqsWorkloadScenario::AnyIncomingMessages() const { return false; }

}  // namespace NYdb::NConsoleClient
