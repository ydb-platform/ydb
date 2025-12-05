#include "sqs_workload_read_scenario.h"
#include "sqs_workload_stats_collector.h"
#include <aws/core/utils/threading/Executor.h>
#include <aws/sqs/model/SetQueueAttributesRequest.h>
#include <ydb/public/lib/ydb_cli/common/command.h>
#include "sqs_workload_reader.h"
#include "http_client.h"

#include <fmt/format.h>

namespace NYdb::NConsoleClient {

int TSqsWorkloadReadScenario::Run(const TClientCommand::TConfig&) {
    auto statsCollector = std::make_shared<TSqsWorkloadStatsCollector>(0, Concurrency, Quiet, PrintTimestamp, WindowSec.Seconds(), TotalSec.Seconds(), WarmupSec.Seconds(), Percentile, ErrorFlag);
    InitMeasuringHttpClient(statsCollector);
    InitSqsClient();

    auto finishedFlag = std::make_shared<std::atomic_bool>(false);

    TSqsWorkloadReaderParams params{
        .TotalSec = TotalSec,
        .QueueUrl = QueueUrl,
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
        .StatsCollector = statsCollector,
        .SleepTimeMs = SleepTimeMs,
    };

    auto f = std::async([&params, finishedFlag]() {
        params.StatsCollector->PrintWindowStatsLoop(finishedFlag);
    });

    TSqsWorkloadReader::RunLoop(params, Now() + params.TotalSec);
    {
        std::unique_lock<std::mutex> lock(*Mutex);
        while (*StartedCount > 0) {
            FinishedCond->wait(lock);
        }
    }

    finishedFlag->store(true);
    f.wait();

    DestroySqsClient();
    DestroyMeasuringHttpClient();

    if (AnyErrors()) {
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

}  // namespace NYdb::NConsoleClient
