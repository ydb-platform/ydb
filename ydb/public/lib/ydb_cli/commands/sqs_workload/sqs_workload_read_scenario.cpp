#include "sqs_workload_read_scenario.h"
#include <aws/core/utils/threading/Executor.h>
#include <aws/sqs/model/SetQueueAttributesRequest.h>
#include <ydb/public/lib/ydb_cli/common/command.h>
#include "sqs_workload_reader.h"

#include <fmt/format.h>

namespace NYdb::NConsoleClient {

int TSqsWorkloadReadScenario::Run(const TClientCommand::TConfig&) {
    InitSqsClient();

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
        .ErrorMessagesDestiny = ErrorMessagesDestiny,
        .HandleMessageDelay = TDuration::MilliSeconds(HandleMessageDelayMs),
        .VisibilityTimeout = TDuration::MilliSeconds(VisibilityTimeoutMs),
        .ValidateFifo = ValidateFifo,
        .HashMapMutex = std::make_shared<std::mutex>(),
        .LastReceivedMessageInGroup = std::make_shared<THashMap<TString, TInstant>>(),
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
