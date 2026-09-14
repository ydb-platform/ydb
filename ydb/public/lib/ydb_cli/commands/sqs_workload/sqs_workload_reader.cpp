#include "sqs_workload_reader.h"
#include "consts.h"
#include "utils.h"
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

        constexpr auto WAIT_TIME_SECONDS = 15;
        constexpr auto ERROR_MESSAGES_DESTINY_FATAL = "fatal";

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

        const auto& messages = outcome.GetResult().GetMessages();
        Aws::Vector<Aws::SQS::Model::DeleteMessageBatchRequestEntry> deleteMessageBatchRequestEntries;
        for (size_t i = 0; i < messages.size(); ++i) {
            auto sendTimestamp = ExtractSendTimestamp(messages[i].GetBody());
            if (ShouldFail(params, i, sendTimestamp)) {
                params.StatsCollector->AddErrorWhileProcessingMessagesEvent(TSqsWorkloadStats::ErrorWhileProcessingMessagesEvent());

                continue;
            }

            auto messageDelayMs = params.HandleMessageDelay.MilliSeconds();
            if (messageDelayMs > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(messageDelayMs));
            }

            deleteMessageBatchRequestEntries.push_back(
                Aws::SQS::Model::DeleteMessageBatchRequestEntry()
                    .WithReceiptHandle(messages[i].GetReceiptHandle())
                    .WithId(messages[i].GetMessageId()));
        }

        if (!deleteMessageBatchRequestEntries.empty()) {
            Aws::SQS::Model::DeleteMessageBatchRequest deleteMessageBatchRequest;
            deleteMessageBatchRequest.SetQueueUrl(params.QueueUrl.c_str());
            deleteMessageBatchRequest.SetEntries(deleteMessageBatchRequestEntries);
            deleteMessageBatchRequest.SetAdditionalCustomHeaderValue(
                AMZ_TARGET_HEADER, SQS_TARGET_DELETE_MESSAGE_BATCH);

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

    bool TSqsWorkloadReader::ShouldFail(const TSqsWorkloadReaderParams& params, ui64 batchIndex, ui64 sendMessageTime) {
        if (!params.ErrorMessagesRate || *params.ErrorMessagesRate == 0) {
            return false;
        }

        auto shouldFail = ((sendMessageTime + batchIndex) %
                           *params.ErrorMessagesRate) == 0;
        if (!shouldFail) {
            return false;
        }

        return (params.ErrorMessagesPolicy == ERROR_MESSAGES_DESTINY_FATAL) ||
               (std::rand() % 3 == 0);
    }

    bool TSqsWorkloadReader::ValidateFifo(const TSqsWorkloadReaderParams& params,
                                          const Aws::SQS::Model::Message& message,
                                          ui64 sendTimestamp) {
        const auto& attributes = message.GetAttributes();
        auto messageGroupId = attributes.find(
            Aws::SQS::Model::MessageSystemAttributeName::MessageGroupId);
        if (messageGroupId == attributes.end()) {
            return true;
        }

        std::unique_lock locker(*params.HashMapMutex);
        auto [it, newGroup] = params.LastReceivedMessageInGroup->try_emplace(messageGroupId->second, sendTimestamp);
        if (!newGroup) {
            params.Log->Write(ELogPriority::TLOG_ERR,
                              TStringBuilder() << "Message group already exists: " << messageGroupId->second << " with send timestamp: " << it->second << " and current send timestamp: " << sendTimestamp);
            return false;
        }

        if (!newGroup && it->second > sendTimestamp) {
            return false;
        }

        it->second = sendTimestamp;
        return true;
    }

    void TSqsWorkloadReader::RunLoop(const TSqsWorkloadReaderParams& params,
                                     TInstant endTime) {
        auto visibilityTimeout = params.VisibilityTimeout.Seconds();

        while (Now() < endTime && !params.ErrorFlag->load()) {
            Aws::SQS::Model::ReceiveMessageRequest receiveMessageRequest;
            receiveMessageRequest.SetQueueUrl(params.QueueUrl.c_str());
            receiveMessageRequest.SetWaitTimeSeconds(WAIT_TIME_SECONDS);
            receiveMessageRequest.SetVisibilityTimeout(visibilityTimeout);
            receiveMessageRequest.SetMaxNumberOfMessages(params.BatchSize);
            receiveMessageRequest.SetAdditionalCustomHeaderValue(
                AMZ_TARGET_HEADER, SQS_TARGET_RECEIVE_MESSAGE);

            {
                std::unique_lock locker(*params.Mutex);
                // WorkersCount tasks are running in parallel and also WorkersCount tasks are waiting in executor queue
                while (*params.StartedCount >= params.WorkersCount * 2) {
                    params.FinishedCond->wait(locker);
                }

                ++(*params.StartedCount);
            }

            params.StatsCollector->AddPushAsyncRequestTaskToQueueEvent(
                TSqsWorkloadStats::PushAsyncRequestTaskToQueueEvent());
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
