#include "sqs_workload_reader.h"
#include <ydb/public/lib/ydb_cli/commands/topic_workload/topic_workload_defines.h>
#include <ydb/public/lib/ydb_cli/common/command.h>
#include <library/cpp/logger/log.h>

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/sqs/SQSClient.h>
#include <aws/sqs/model/DeleteMessageBatchRequest.h>
#include <aws/sqs/model/DeleteMessageBatchRequestEntry.h>
#include <aws/sqs/model/ReceiveMessageRequest.h>

#include <fmt/format.h>
#include <chrono>
#include <thread>

namespace NYdb::NConsoleClient {

namespace {

constexpr auto kWaitTimeSeconds = 15;
constexpr auto kErrorMessagesDestinyFatal = "fatal";

}  // namespace

void TSqsWorkloadReader::OnMessageReceived(
    const TSqsWorkloadReaderParams& params,
    const Aws::SQS::SQSClient* client,
    const Aws::SQS::Model::ReceiveMessageRequest&,
    const Aws::SQS::Model::ReceiveMessageOutcome& outcome
) {        
    if (!outcome.IsSuccess()) {
        params.Log->Write(
            ELogPriority::TLOG_ERR, TStringBuilder() << "Error receiving message: " << outcome.GetError().GetMessage()
        );
        return;
    }

    const auto& messages = outcome.GetResult().GetMessages();

    Aws::Vector<Aws::SQS::Model::DeleteMessageBatchRequestEntry> deleteMessageBatchRequestEntries;
    for (const auto& message : messages) {
        auto startHandlingTime = TInstant::Now();

        try {
            const auto& body = message.GetBody();
            std::string send_time;
            for (size_t i = 0; i < body.size(); ++i) {
                if (body[i] == 'a') {
                    break;
                }

                send_time.push_back(body[i]);
            }

            [[maybe_unused]] auto endToEnd = (TInstant::Now().MilliSeconds() - std::stoll(send_time));
            // params.Log->Write(ELogPriority::TLOG_INFO, TStringBuilder() << "End-to-end latency: " << endToEnd << "
            // ms");
        } catch (const std::exception& e) {
            params.Log->Write(ELogPriority::TLOG_ERR, TStringBuilder() << "Error parsing message body: " << e.what());
        }

        if (ShouldFail(params, message)) {
            continue;
        }

        if (params.HandleMessageDelay.MilliSeconds() > 0 &&
            (TInstant::Now() - startHandlingTime).MilliSeconds() < params.HandleMessageDelay.MilliSeconds()) {
            auto toSleep =
                params.HandleMessageDelay.MilliSeconds() - (TInstant::Now() - startHandlingTime).MilliSeconds();
            std::this_thread::sleep_for(std::chrono::milliseconds(toSleep));
        }

        deleteMessageBatchRequestEntries.push_back(
            Aws::SQS::Model::DeleteMessageBatchRequestEntry().WithReceiptHandle(message.GetReceiptHandle()).WithId(message.GetMessageId())
        );
    }

    if (!deleteMessageBatchRequestEntries.empty()) {
        auto deleteMessageBatchOutcome = client->DeleteMessageBatch(
            Aws::SQS::Model::DeleteMessageBatchRequest()
                .WithQueueUrl(fmt::format("http://{}/{}", params.EndPoint, params.QueueName).c_str())
                .WithEntries(deleteMessageBatchRequestEntries)
        );

        if (!deleteMessageBatchOutcome.IsSuccess()) {
            params.Log->Write(
                ELogPriority::TLOG_ERR,
                TStringBuilder() << "Error deleting message: " << deleteMessageBatchOutcome.GetError().GetMessage()
            );
        }
    }
}

bool TSqsWorkloadReader::ShouldFail(const TSqsWorkloadReaderParams& params, const Aws::SQS::Model::Message& message) {
    if (!params.ErrorMessagesRate || *params.ErrorMessagesRate == 0) {
        return false;
    }

    auto shouldFail = (std::hash<std::string>{}(message.GetMessageId()) % *params.ErrorMessagesRate) == 0;
    if (!shouldFail) {
        return false;
    }

    return (params.ErrorMessagesDestiny == kErrorMessagesDestinyFatal) || (std::rand() % 2 == 0);
}

void TSqsWorkloadReader::RunLoop(const TSqsWorkloadReaderParams& params, TInstant endTime) {
    auto visibilityTimeout = params.VisibilityTimeout.Seconds();
    auto queueUrl = fmt::format("http://{}/{}", params.EndPoint, params.QueueName);

    while (Now() < endTime && !params.ErrorFlag->load()) {
        Aws::SQS::Model::ReceiveMessageRequest receiveMessageRequest;
        receiveMessageRequest.SetQueueUrl(queueUrl.c_str());
        receiveMessageRequest.SetWaitTimeSeconds(kWaitTimeSeconds);
        receiveMessageRequest.SetVisibilityTimeout(visibilityTimeout);
        receiveMessageRequest.SetMaxNumberOfMessages(params.BatchSize);

        {
            std::unique_lock<std::mutex> locker(*params.Mutex);
            ++(*params.StartedCount);
        }

        params.SqsClient->ReceiveMessageAsync(
            receiveMessageRequest,
            [&params](const Aws::SQS::SQSClient* sqsClient, const Aws::SQS::Model::ReceiveMessageRequest& request, const Aws::SQS::Model::ReceiveMessageOutcome& outcome, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) {
                OnMessageReceived(params, sqsClient, request, outcome);
            }
        );
    }
}

}  // namespace NYdb::NConsoleClient
