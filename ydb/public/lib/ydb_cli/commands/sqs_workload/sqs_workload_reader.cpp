#include "sqs_workload_reader.h"
#include "consts.h"
#include "sqs_workload_stats.h"
#include <library/cpp/logger/log.h>
#include <ydb/public/lib/ydb_cli/commands/topic_workload/topic_workload_defines.h>
#include <ydb/public/lib/ydb_cli/common/command.h>

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/sqs/SQSClient.h>
#include <aws/sqs/model/DeleteMessageBatchRequest.h>
#include <aws/sqs/model/DeleteMessageBatchRequestEntry.h>
#include <aws/sqs/model/ReceiveMessageRequest.h>

#include <chrono>
#include <fmt/format.h>
#include <thread>

namespace NYdb::NConsoleClient {

    namespace {

        constexpr auto kWaitTimeSeconds = 15;
        constexpr auto kErrorMessagesDestinyFatal = "fatal";

        void DecrementStartedCountAndNotify(const TSqsWorkloadReaderParams& params) {
            std::unique_lock locker(*params.Mutex);
            --(*params.StartedCount);
            params.FinishedCond->notify_all();
        }

    } // namespace

    void TSqsWorkloadReader::OnMessageReceived(
        const TSqsWorkloadReaderParams& params, const Aws::SQS::SQSClient* client,
        const Aws::SQS::Model::ReceiveMessageRequest&,
        const Aws::SQS::Model::ReceiveMessageOutcome& outcome) {
        if (!outcome.IsSuccess()) {
            params.Log->Write(ELogPriority::TLOG_ERR,
                              TStringBuilder() << "Error receiving message: "
                                               << outcome.GetError().GetMessage());

            params.StatsCollector->AddReceiveRequestErrorEvent(
                TSqsWorkloadStats::ReceiveRequestErrorEvent());
            DecrementStartedCountAndNotify(params);
            return;
        }

        ui64 sendMessageTime = Now().MilliSeconds();
        const auto& messages = outcome.GetResult().GetMessages();
        if (!messages.empty()) {
            const auto& body = messages[0].GetBody();
            auto sepIndex = body.find(kSQSMessageStartTimeSeparator);

            if (sepIndex != std::string::npos) {
                try {
                    sendMessageTime = std::stoll(body.substr(0, sepIndex));
                    auto endToEndLatency = Now().MilliSeconds() - sendMessageTime;
                    TSqsWorkloadStats::GotMessageEvent gotMessageEvent{
                        body.size() * messages.size(), endToEndLatency,
                        messages.size()};
                    params.StatsCollector->AddGotMessageEvent(gotMessageEvent);
                } catch (const std::exception& e) {
                    params.Log->Write(ELogPriority::TLOG_ERR,
                                      TStringBuilder() << "Can not parse send message time: " << e.what());
                }
            }
        }

        Aws::Vector<Aws::SQS::Model::DeleteMessageBatchRequestEntry>
            deleteMessageBatchRequestEntries;
        for (const auto& message : messages) {
            if (params.ValidateFifo && !ValidateFifo(params, message, sendMessageTime)) {
                params.Log->Write(ELogPriority::TLOG_ERR,
                                  TStringBuilder()
                                      << "FIFO validation failed for message: "
                                      << message.GetMessageId());
                params.ErrorFlag->store(true);
                DecrementStartedCountAndNotify(params);
                params.StatsCollector->AddFinishProcessMessagesEvent(
                    TSqsWorkloadStats::FinishProcessMessagesEvent{messages.size()});
                return;
            }

            if (ShouldFail(params, message, sendMessageTime)) {
                continue;
            }

            auto messageDelayMs = params.HandleMessageDelay.MilliSeconds();
            if (messageDelayMs > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(messageDelayMs));
            }

            deleteMessageBatchRequestEntries.push_back(
                Aws::SQS::Model::DeleteMessageBatchRequestEntry()
                    .WithReceiptHandle(message.GetReceiptHandle())
                    .WithId(message.GetMessageId()));
        }

        if (!deleteMessageBatchRequestEntries.empty()) {
            Aws::SQS::Model::DeleteMessageBatchRequest deleteMessageBatchRequest;
            deleteMessageBatchRequest.SetQueueUrl(params.QueueUrl.c_str());
            deleteMessageBatchRequest.SetEntries(deleteMessageBatchRequestEntries);
            deleteMessageBatchRequest.SetAdditionalCustomHeaderValue(
                kAmzTargetHeader, kSQSTargetDeleteMessageBatch);

            if (params.SetSubjectToken) {
                deleteMessageBatchRequest.SetAdditionalCustomHeaderValue(
                    kYacloudSubjectTokenHeader, params.Token.c_str());
            }

            auto deleteMessageBatchOutcome =
                client->DeleteMessageBatch(deleteMessageBatchRequest);

            if (!deleteMessageBatchOutcome.IsSuccess()) {
                params.Log->Write(
                    ELogPriority::TLOG_ERR,
                    TStringBuilder()
                        << "Error deleting message: "
                        << deleteMessageBatchOutcome.GetError().GetMessage());
                params.StatsCollector->AddDeleteRequestErrorEvent(
                    TSqsWorkloadStats::DeleteRequestErrorEvent());
            } else {
                params.StatsCollector->AddDeletedMessagesEvent(
                    TSqsWorkloadStats::DeletedMessagesEvent{deleteMessageBatchOutcome.GetResult().GetSuccessful().size()});
            }
        }

        params.StatsCollector->AddFinishProcessMessagesEvent(
            TSqsWorkloadStats::FinishProcessMessagesEvent{messages.size()});
        DecrementStartedCountAndNotify(params);
    }

    bool TSqsWorkloadReader::ShouldFail(const TSqsWorkloadReaderParams& params,
                                        const Aws::SQS::Model::Message& message, ui64 sendMessageTime) {
        if (!params.ErrorMessagesRate || *params.ErrorMessagesRate == 0) {
            return false;
        }

        auto shouldFail = (sendMessageTime %
                           *params.ErrorMessagesRate) == 0;
        if (!shouldFail) {
            return false;
        }

        params.Log->Write(ELogPriority::TLOG_ERR,
                          TStringBuilder() << "Message failed to process: "
                                           << message.GetMessageId());

        return (params.ErrorMessagesDestiny == kErrorMessagesDestinyFatal) ||
               (std::rand() % 3 == 0);
    }

    bool TSqsWorkloadReader::ValidateFifo(const TSqsWorkloadReaderParams& params,
                                          const Aws::SQS::Model::Message& message,
                                          ui64 sendTimestamp) {
        std::unique_lock locker(*params.HashMapMutex);
        const auto& attributes = message.GetAttributes();
        auto messageGroupId = attributes.find(
            Aws::SQS::Model::MessageSystemAttributeName::MessageGroupId);
        if (messageGroupId == attributes.end()) {
            return false;
        }

        auto lastReceivedMessageInGroup =
            params.LastReceivedMessageInGroup->find(messageGroupId->second);
        if (lastReceivedMessageInGroup ==
            params.LastReceivedMessageInGroup->end()) {
            (*params.LastReceivedMessageInGroup)[messageGroupId->second] =
                sendTimestamp;
            return true;
        }

        if (lastReceivedMessageInGroup->second > sendTimestamp) {
            return false;
        }

        (*params.LastReceivedMessageInGroup)[messageGroupId->second] = sendTimestamp;
        return true;
    }

    void TSqsWorkloadReader::RunLoop(const TSqsWorkloadReaderParams& params,
                                     TInstant endTime) {
        auto visibilityTimeout = params.VisibilityTimeout.Seconds();

        while (Now() < endTime && !params.ErrorFlag->load()) {
            Aws::SQS::Model::ReceiveMessageRequest receiveMessageRequest;
            receiveMessageRequest.SetQueueUrl(params.QueueUrl.c_str());
            receiveMessageRequest.SetWaitTimeSeconds(kWaitTimeSeconds);
            receiveMessageRequest.SetVisibilityTimeout(visibilityTimeout);
            receiveMessageRequest.SetMaxNumberOfMessages(params.BatchSize);
            receiveMessageRequest.SetAdditionalCustomHeaderValue(
                kAmzTargetHeader, kSQSTargetReceiveMessage);

            if (params.SetSubjectToken) {
                receiveMessageRequest.SetAdditionalCustomHeaderValue(
                    kYacloudSubjectTokenHeader, params.Token.c_str());
            }

            {
                std::unique_lock locker(*params.Mutex);
                // Concurrency tasks are running in parallel and also Concurrency tasks are waiting in executor queue
                while (*params.StartedCount >= params.Concurrency * 2) {
                    params.FinishedCond->wait(locker);
                }

                ++(*params.StartedCount);
            }

            params.StatsCollector->AddAddAsyncRequestTaskToQueueEvent(
                TSqsWorkloadStats::AddAsyncRequestTaskToQueueEvent());
            params.SqsClient->ReceiveMessageAsync(
                receiveMessageRequest,
                [&params](
                    const Aws::SQS::SQSClient* sqsClient,
                    const Aws::SQS::Model::ReceiveMessageRequest& request,
                    const Aws::SQS::Model::ReceiveMessageOutcome& outcome,
                    const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) {
                    OnMessageReceived(params, sqsClient, request, outcome);
                });
        }
    }

} // namespace NYdb::NConsoleClient
