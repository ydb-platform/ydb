#include "sqs_workload_write_scenario.h"
#include "http_client.h"
#include "sqs_workload_writer.h"

namespace NYdb::NConsoleClient {

    int TSqsWorkloadWriteScenario::Run(const TClientCommand::TConfig& config) {
        InitAwsSdk();
        auto result = RunScenario(config);
        DestroyAwsSdk();
        return result;
    }

    int TSqsWorkloadWriteScenario::RunScenario(const TClientCommand::TConfig& config) {
        InitStatsCollector(WorkersCount, 0);
        InitMeasuringHttpClient(StatsCollector);
        InitSqsClient(config);

        auto finishedFlag = std::make_shared<std::atomic_bool>(false);
        auto queueUrl = GetQueueUrl(Topic, Consumer, QueueName);
        if (queueUrl.empty()) {
            DestroySqsClient();
            return EXIT_FAILURE;
        }

        TSqsWorkloadWriterParams params{
            .TotalSec = TotalSec,
            .QueueUrl = queueUrl,
            .AwsAccessKeyId = AwsAccessKeyId,
            .AwsSessionToken = AwsSessionToken,
            .AwsSecretKey = AwsSecretKey,
            .Log = Log,
            .Mutex = Mutex,
            .FinishedCond = FinishedCond,
            .StartedCount = StartedCount,
            .ErrorFlag = ErrorFlag,
            .SqsClient = SqsClient,
            .StatsCollector = StatsCollector,
            .MaxUniqueMessages = MaxUniqueMessages,
            .BatchSize = BatchSize,
            .WorkersCount = WorkersCount,
            .GroupsAmount = GroupsAmount,
            .MessageSize = MessageSize,
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

        if (AnyErrors() || params.StatsCollector->GetTotalWriteMessages() == 0) {
            return EXIT_FAILURE;
        }

        return EXIT_SUCCESS;
    }

} // namespace NYdb::NConsoleClient
