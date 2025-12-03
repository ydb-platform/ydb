#include "sqs_workload_write_scenario.h"
#include "sqs_workload_writer.h"

namespace NYdb::NConsoleClient {

int TSqsWorkloadWriteScenario::Run(const TClientCommand::TConfig&) {
    InitSqsClient();

    TSqsWorkloadWriterParams params{
        .TotalSec = TotalSec,
        .QueueName = QueueName,
        .EndPoint = EndPoint,
        .Account = Account,
        .Log = Log,
        .Mutex = Mutex,
        .FinishedCond = FinishedCond,
        .StartedCount = StartedCount,
        .ErrorFlag = ErrorFlag,
        .SqsClient = SqsClient,
        .BatchSize = BatchSize,
        .MessageSize = MessageSize,
        .SleepTimeMs = SleepTimeMs,
    };

    TSqsWorkloadWriter::RunLoop(params, Now() + params.TotalSec);
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
