#include "sqs_workload_writer.h"
#include "consts.h"
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/AsyncCallerContext.h>
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
        CreateSendMessageBatchRequestEntries(const TSqsWorkloadWriterParams& params) {
            Aws::Vector<Aws::SQS::Model::SendMessageBatchRequestEntry> entries;
            for (ui32 i = 0; i < params.BatchSize; ++i) {
                Aws::String messageBody(params.MessageSize, 'a');
                Aws::SQS::Model::SendMessageBatchRequestEntry entry;
                entry.WithMessageBody(messageBody).WithId(fmt::format("{}", i));
                if (params.GroupsAmount > 0) {
                    auto messageGroupID = std::rand() % params.GroupsAmount;
                    entry.WithMessageGroupId(fmt::format("{}", messageGroupID));
                }

                if (params.MaxUniqueMessages > 0) {
                    auto messageDeduplicationID = std::rand() % params.MaxUniqueMessages;
                    entry.WithMessageDeduplicationId(std::format("{}", messageDeduplicationID));
                }

                entries.push_back(std::move(entry));
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
        while (Now() < endTime && !params.ErrorFlag->load()) {
            Aws::SQS::Model::SendMessageBatchRequest sendMessageBatchRequest;
            sendMessageBatchRequest.SetQueueUrl(params.QueueUrl.c_str());
            sendMessageBatchRequest.SetEntries(CreateSendMessageBatchRequestEntries(params));
            sendMessageBatchRequest.SetAdditionalCustomHeaderValue(
                AMZ_TARGET_HEADER, SQS_TARGET_SEND_MESSAGE_BATCH);

            if (params.SetSubjectToken) {
                sendMessageBatchRequest.SetAdditionalCustomHeaderValue(
                    YACLOUD_SUBJECT_TOKEN_HEADER, params.Token.c_str());
            }

            {
                std::unique_lock locker(*params.Mutex);
                // Concurrency tasks are running in parallel and also Concurrency tasks are waiting in executor queue
                while (*params.StartedCount >= params.Concurrency * 2) {
                    params.FinishedCond->wait(locker);
                }

                ++(*params.StartedCount);
            }

            params.StatsCollector->AddPushAsyncRequestTaskToQueueEvent(
                TSqsWorkloadStats::PushAsyncRequestTaskToQueueEvent());
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
