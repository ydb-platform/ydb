#include "sqs_workload_writer.h"
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/sqs/SQSClient.h>
#include <aws/sqs/model/SendMessageBatchRequest.h>
#include <aws/sqs/model/SendMessageRequest.h>
#include <ydb/public/lib/ydb_cli/common/command.h>

#include <fmt/format.h>

namespace NYdb::NConsoleClient {

namespace {

Aws::Vector<Aws::SQS::Model::SendMessageBatchRequestEntry>
CreateSendMessageBatchRequestEntries(ui32 batchSize, ui32 messageSize) {
    Aws::Vector<Aws::SQS::Model::SendMessageBatchRequestEntry> entries;
    auto now = Now().MilliSeconds();
    for (ui32 i = 0; i < batchSize; ++i) {
        auto messageBody = fmt::format("{}", now);
        while (messageBody.size() < messageSize) {
            messageBody.push_back('a');
        }

        entries.push_back(
            Aws::SQS::Model::SendMessageBatchRequestEntry().WithMessageBody(messageBody).WithId(fmt::format("{}", i))
        );
    }
    return entries;
}

}  // namespace

void TSqsWorkloadWriter::
    OnMessageSent(const TSqsWorkloadWriterParams& params, const Aws::SQS::SQSClient*, const Aws::SQS::Model::SendMessageBatchRequest&, const Aws::SQS::Model::SendMessageBatchOutcome& outcome, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) {
    if (!outcome.IsSuccess()) {
        // params.Log->Write(
        //     ELogPriority::TLOG_ERR, TStringBuilder() << "Error sending message: " << outcome.GetError().GetMessage()
        // );
        // params.ErrorFlag->store(true);
    }

    std::unique_lock<std::mutex> locker(*params.Mutex);
    --(*params.StartedCount);
    params.FinishedCond->notify_one();
}

void TSqsWorkloadWriter::RunLoop(const TSqsWorkloadWriterParams& params, TInstant endTime) {
    while (Now() < endTime && !params.ErrorFlag->load()) {
        Aws::SQS::Model::SendMessageBatchRequest sendMessageBatchRequest;
        sendMessageBatchRequest.SetQueueUrl(fmt::format("http://{}/{}", params.EndPoint, params.QueueName).c_str());
        sendMessageBatchRequest.SetEntries(CreateSendMessageBatchRequestEntries(params.BatchSize, params.MessageSize));

        {
            std::unique_lock<std::mutex> locker(*params.Mutex);
            ++(*params.StartedCount);
        }

        params.SqsClient->SendMessageBatchAsync(
            sendMessageBatchRequest,
            [&params](
                const Aws::SQS::SQSClient* sqsClient,
                const Aws::SQS::Model::SendMessageBatchRequest& sendMessageBatchRequest,
                const Aws::SQS::Model::SendMessageBatchOutcome& outcome,
                const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context
            ) { OnMessageSent(params, sqsClient, sendMessageBatchRequest, outcome, context); }
        );

        std::this_thread::sleep_for(std::chrono::milliseconds(params.SleepTimeMs));
    }
}

}  // namespace NYdb::NConsoleClient
