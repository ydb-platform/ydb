#include "sqs_workload_write_scenario.h"
#include "http_client.h"
#include "sqs_workload_writer.h"

namespace NYdb::NConsoleClient {

    int TSqsWorkloadWriteScenario::Run(const TClientCommand::TConfig&) {
        InitAwsSdk();
        auto result = RunScenario();
        DestroyAwsSdk();
        return result;
    }

    int TSqsWorkloadWriteScenario::RunScenario() {
        InitStatsCollector(Concurrency, 0);
        InitMeasuringHttpClient(StatsCollector);
        InitSqsClient();

        auto finishedFlag = std::make_shared<std::atomic_bool>(false);

        TSqsWorkloadWriterParams params{
            .TotalSec = TotalSec,
            .QueueUrl = QueueUrl,
            .Account = Account,
            .Token = Token,
            .Log = Log,
            .Mutex = Mutex,
            .FinishedCond = FinishedCond,
            .StartedCount = StartedCount,
            .ErrorFlag = ErrorFlag,
            .SqsClient = SqsClient,
            .StatsCollector = StatsCollector,
            .MaxUniqueMessages = MaxUniqueMessages,
            .BatchSize = BatchSize,
            .Concurrency = Concurrency,
            .GroupsAmount = GroupsAmount,
            .MessageSize = MessageSize,
            .SetSubjectToken = SetSubjectToken,
        };

        auto f = std::async([&params, finishedFlag]() {
            params.StatsCollector->PrintWindowStatsLoop(finishedFlag);
        });

        TSqsWorkloadWriter::RunLoop(params, Now() + params.TotalSec);
        {
            std::unique_lock lock(*Mutex);
            while (*StartedCount > 0) {
                FinishedCond->wait(lock);
            }
        }

        finishedFlag->store(true);
        f.wait();

        DestroySqsClient();

        if (AnyErrors()) {
            return EXIT_FAILURE;
        }

        return EXIT_SUCCESS;
    }

} // namespace NYdb::NConsoleClient
