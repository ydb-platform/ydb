#include "sqs_workload_read_scenario.h"
#include <aws/core/utils/threading/Executor.h>
#include <aws/sqs/model/SetQueueAttributesRequest.h>
#include <ydb/public/lib/ydb_cli/common/command.h>
#include "sqs_workload_reader.h"

#include <fmt/format.h>

namespace NYdb::NConsoleClient {

int TSqsWorkloadReadScenario::Run(const TClientCommand::TConfig&) {
    InitSqsClient();

    if (DlqEndPoint && DlqQueueName) {
        Aws::SQS::Model::SetQueueAttributesRequest setQueueAttributesRequest;
        setQueueAttributesRequest.SetQueueUrl(fmt::format("http://{}/{}", *DlqEndPoint, *DlqQueueName).c_str());
        // TODO: DLQ
        // Aws::String redrivePolicy = fmt::format("{{ \"deadLetterTargetArn\": \"arn:aws:sqs:{}:{}:{}\" }}", Region,
        // Account, *DlqQueueName); setQueueAttributesRequest.SetAttributes({
        //     { Aws::SQS::Model::QueueAttributeName::RedrivePolicy, redrivePolicy }
        // });
        auto setQueueAttributesOutcome = SqsClient->SetQueueAttributes(setQueueAttributesRequest);
        if (!setQueueAttributesOutcome.IsSuccess()) {
            Log->Write(
                ELogPriority::TLOG_ERR,
                TStringBuilder() << "Error setting queue attributes: "
                                 << setQueueAttributesOutcome.GetError().GetMessage()
            );
            return EXIT_FAILURE;
        }
    }

    TSqsWorkloadReaderParams params{
        .TotalSec = TotalSec,
        .QueueName = QueueName,
        .EndPoint = EndPoint,
        .Account = Account,
        .Log = Log,
        .ErrorFlag = ErrorFlag,
        .SqsClient = SqsClient,
        .Mutex = Mutex,
        .FinishedCond = FinishedCond,
        .StartedCount = StartedCount,
        .Concurrency = Concurrency,
        .BatchSize = BatchSize,
        .ErrorMessagesRate = ErrorMessagesRate,
        .HandleMessageDelay = TDuration::MilliSeconds(HandleMessageDelayMs),
        .VisibilityTimeout = TDuration::MilliSeconds(VisibilityTimeoutMs),
    };

    TSqsWorkloadReader::RunLoop(params, Now() + params.TotalSec);

    {
        std::unique_lock<std::mutex> lock(*Mutex);
        while (*StartedCount > 0) {
            FinishedCond->wait(lock);
        }
    }

    DestroySqsClient();

    if (AnyErrors()) {
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

}  // namespace NYdb::NConsoleClient
