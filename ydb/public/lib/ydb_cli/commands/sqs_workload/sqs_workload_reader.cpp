#include "sqs_workload_reader.h"
#include "sqs_workload_stats.h"
#include "consts.h"
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

void DecrementStartedCountAndNotify(const TSqsWorkloadReaderParams& params) {
    std::unique_lock<std::mutex> locker(*params.Mutex);
    --(*params.StartedCount);
    params.FinishedCond->notify_one();
}

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

        params.StatsCollector->AddReceiveRequestErrorEvent(TSqsWorkloadStats::ReceiveRequestErrorEvent());
        DecrementStartedCountAndNotify(params);
        return;
    }

    const auto& messages = outcome.GetResult().GetMessages();
    if (!messages.empty()) {
        try {
            const auto& message = messages[0];
            const auto& body = message.GetBody();
            std::string startTime;
            for (size_t i = 0; i < body.size(); ++i) {
                if (body[i] == kSQSMessageStartTimeSeparator) {
                    break;
                }

                startTime.push_back(body[i]);
            }

            auto endToEndLatency = Now().MilliSeconds() - std::stoll(startTime);
            TSqsWorkloadStats::GotMessageEvent gotMessageEvent{body.size() * messages.size(), endToEndLatency, messages.size()};
            params.StatsCollector->AddGotMessageEvent(gotMessageEvent);
        } catch (const std::exception& e) {
            params.Log->Write(ELogPriority::TLOG_ERR, TStringBuilder() << "Error parsing message body: " << e.what());
        }
    }

    Aws::Vector<Aws::SQS::Model::DeleteMessageBatchRequestEntry> deleteMessageBatchRequestEntries;
    for (const auto& message : messages) {
        auto startHandlingTime = TInstant::Now();
        TInstant timestamp = TInstant::Now();
        if (params.ValidateFifo && !ValidateFifo(params, message, timestamp)) {
            params.Log->Write(ELogPriority::TLOG_ERR, TStringBuilder() << "FIFO validation failed for message: " << message.GetMessageId());
            params.ErrorFlag->store(true);
            DecrementStartedCountAndNotify(params);
            return;
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
        Aws::SQS::Model::DeleteMessageBatchRequest deleteMessageBatchRequest;
        deleteMessageBatchRequest.SetQueueUrl(params.QueueUrl.c_str());
        deleteMessageBatchRequest.SetEntries(deleteMessageBatchRequestEntries);
        deleteMessageBatchRequest.SetAdditionalCustomHeaderValue(kSQSWorkloadActionHeader, kSQSWorkloadActionDelete);
        
        auto deleteMessageBatchOutcome = client->DeleteMessageBatch(deleteMessageBatchRequest);

        if (!deleteMessageBatchOutcome.IsSuccess()) {
            params.Log->Write(
                ELogPriority::TLOG_ERR,
                TStringBuilder() << "Error deleting message: " << deleteMessageBatchOutcome.GetError().GetMessage()
            );
            params.StatsCollector->AddDeleteRequestErrorEvent(TSqsWorkloadStats::DeleteRequestErrorEvent());
        } else {
            params.StatsCollector->AddDeletedMessagesEvent(TSqsWorkloadStats::DeletedMessagesEvent{deleteMessageBatchRequestEntries.size()});
        }
    }

    DecrementStartedCountAndNotify(params);
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

bool TSqsWorkloadReader::ValidateFifo(const TSqsWorkloadReaderParams& params, const Aws::SQS::Model::Message& message, TInstant timestamp) {
    std::unique_lock<std::mutex> locker(*params.HashMapMutex);
    const auto& attributes = message.GetAttributes();
    const auto& messageGroupId = attributes.find(Aws::SQS::Model::MessageSystemAttributeName::MessageGroupId);
    if (messageGroupId == attributes.end()) {
        return false;
    }  

    auto lastReceivedMessageInGroup = params.LastReceivedMessageInGroup->find(messageGroupId->second);
    if (lastReceivedMessageInGroup == params.LastReceivedMessageInGroup->end()) {
        (*params.LastReceivedMessageInGroup)[messageGroupId->second] = timestamp;
        return true;
    }

    if (lastReceivedMessageInGroup->second > timestamp) {
        return false;
    }

    (*params.LastReceivedMessageInGroup)[messageGroupId->second] = timestamp;
    return true;
}

void TSqsWorkloadReader::RunLoop(const TSqsWorkloadReaderParams& params, TInstant endTime) {
    auto visibilityTimeout = params.VisibilityTimeout.Seconds();

    while (Now() < endTime && !params.ErrorFlag->load()) {
        Aws::SQS::Model::ReceiveMessageRequest receiveMessageRequest;
        receiveMessageRequest.SetQueueUrl(params.QueueUrl.c_str());
        receiveMessageRequest.SetWaitTimeSeconds(kWaitTimeSeconds);
        receiveMessageRequest.SetVisibilityTimeout(visibilityTimeout);
        receiveMessageRequest.SetMaxNumberOfMessages(params.BatchSize);
        receiveMessageRequest.SetAdditionalCustomHeaderValue(kSQSWorkloadActionHeader, kSQSWorkloadActionReceive);

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

        if (params.SleepTimeMs > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(params.SleepTimeMs));
        }
    }
}

}  // namespace NYdb::NConsoleClient
