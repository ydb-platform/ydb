#include "sqs_workload_write_scenario.h"
#include "sqs_workload_writer.h"
#include "http_client.h"

namespace NYdb::NConsoleClient {

int TSqsWorkloadWriteScenario::Run(const TClientCommand::TConfig&) {
    auto statsCollector = std::make_shared<TSqsWorkloadStatsCollector>(Concurrency, 0, Quiet, PrintTimestamp, WindowSec.Seconds(), TotalSec.Seconds(), 0, Percentile, ErrorFlag);
    InitMeasuringHttpClient(statsCollector);
    InitSqsClient();

    TSqsWorkloadWriterParams params{
        .TotalSec = TotalSec,
        .QueueUrl = QueueUrl,
        .Account = Account,
        .Log = Log,
        .Mutex = Mutex,
        .FinishedCond = FinishedCond,
        .StartedCount = StartedCount,
        .ErrorFlag = ErrorFlag,
        .SqsClient = SqsClient,
        .StatsCollector = statsCollector,
        .BatchSize = BatchSize,
        .GroupsAmount = GroupsAmount,
        .MessageSize = MessageSize,
        .SleepTimeMs = SleepTimeMs,
    };

    auto f = std::async([&params]() {
        params.StatsCollector->PrintWindowStatsLoop();
    });

    TSqsWorkloadWriter::RunLoop(params, Now() + params.TotalSec);
    {
        std::unique_lock<std::mutex> lock(*Mutex);
        while (*StartedCount > 0) {
            FinishedCond->wait(lock);
        }
    }

    f.wait();

    DestroySqsClient();
    DestroyMeasuringHttpClient();

    if (AnyErrors()) {
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

}  // namespace NYdb::NConsoleClient
