#include "sqs_workload_writer.h"
#include "consts.h"
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/utils/UUID.h>
#include <aws/sqs/SQSClient.h>
#include <aws/sqs/model/SendMessageBatchRequest.h>
#include <aws/sqs/model/SendMessageRequest.h>
#include <ydb/public/lib/ydb_cli/common/command.h>

#include <fmt/format.h>

namespace NYdb::NConsoleClient {

    namespace {

        void DecrementStartedCountAndNotify(const TSqsWorkloadWriterParams& params) {
            std::unique_lock locker(*params.Mutex);
            --(*params.StartedCount);
            params.FinishedCond->notify_all();
        }

        Aws::Vector<Aws::SQS::Model::SendMessageBatchRequestEntry>
        CreateSendMessageBatchRequestEntries(ui64 now, ui32 batchSize, ui32 messageSize,
                                             ui32 messageGroups, ui32& messageGroupID) {
            Aws::Vector<Aws::SQS::Model::SendMessageBatchRequestEntry> entries;
            for (ui32 i = 0; i < batchSize; ++i) {
                auto messageBody =
                    fmt::format("{}{}", now, kSQSMessageStartTimeSeparator);
                while (messageBody.size() < messageSize) {
                    messageBody.push_back('a');
                }

                Aws::SQS::Model::SendMessageBatchRequestEntry entry;
                entry.WithMessageBody(messageBody).WithId(fmt::format("{}", i));
                if (messageGroups > 0) {
                    entry.WithMessageGroupId(fmt::format("{}", messageGroupID));
                }

                entries.push_back(std::move(entry));
                if (messageGroups > 0) {
                    messageGroupID = (messageGroupID + 1) % messageGroups;
                }
            }
            return entries;
        }

    } // namespace

    void TSqsWorkloadWriter::OnMessageSent(
        const TSqsWorkloadWriterParams& params, const Aws::SQS::SQSClient*,
        const Aws::SQS::Model::SendMessageBatchRequest&,
        const Aws::SQS::Model::SendMessageBatchOutcome& outcome,
        const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) {
        auto failedCount = outcome.GetResult().GetFailed().size();
        if (!outcome.IsSuccess() || failedCount > 0) {
            params.Log->Write(
                ELogPriority::TLOG_ERR,
                TStringBuilder()
                    << "Error sending message: " << outcome.GetError().GetMessage()
                    << " failed: " << failedCount);
            params.StatsCollector->AddSendRequestErrorEvent(
                TSqsWorkloadStats::SendRequestErrorEvent());
        }

        auto successCount = outcome.GetResult().GetSuccessful().size();
        params.StatsCollector->AddSentMessagesEvent(
            TSqsWorkloadStats::SentMessagesEvent{successCount * params.MessageSize,
                                                 successCount});
        DecrementStartedCountAndNotify(params);
    }

    void TSqsWorkloadWriter::RunLoop(const TSqsWorkloadWriterParams& params,
                                     TInstant endTime) {
        ui32 messageGroupID = 0;

        while (Now() < endTime && !params.ErrorFlag->load()) {
            auto now = Now().MilliSeconds();

            Aws::SQS::Model::SendMessageBatchRequest sendMessageBatchRequest;
            sendMessageBatchRequest.SetQueueUrl(params.QueueUrl.c_str());
            sendMessageBatchRequest.SetEntries(CreateSendMessageBatchRequestEntries(
                now, params.BatchSize, params.MessageSize, params.GroupsAmount,
                messageGroupID));
            sendMessageBatchRequest.SetAdditionalCustomHeaderValue(
                kAmzTargetHeader, kSQSTargetSendMessageBatch);

            if (params.SetSubjectToken) {
                sendMessageBatchRequest.SetAdditionalCustomHeaderValue(
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
            params.SqsClient->SendMessageBatchAsync(
                sendMessageBatchRequest,
                [&params](
                    const Aws::SQS::SQSClient* sqsClient,
                    const Aws::SQS::Model::SendMessageBatchRequest& sendMessageBatchRequest,
                    const Aws::SQS::Model::SendMessageBatchOutcome& outcome,
                    const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context) {
                    OnMessageSent(params, sqsClient, sendMessageBatchRequest,
                                  outcome, context);
                });
        }
    }

} // namespace NYdb::NConsoleClient
