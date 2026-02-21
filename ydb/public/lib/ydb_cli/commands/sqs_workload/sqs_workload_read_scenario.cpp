#include "sqs_workload_read_scenario.h"
#include "http_client.h"
#include "sqs_workload_reader.h"
#include <aws/core/utils/threading/Executor.h>
#include <aws/sqs/model/SetQueueAttributesRequest.h>
#include <ydb/public/lib/ydb_cli/common/command.h>

#include <fmt/format.h>

namespace NYdb::NConsoleClient {

    int TSqsWorkloadReadScenario::Run(const TClientCommand::TConfig& config) {
        InitAwsSdk();
        auto result = RunScenario(config);
        DestroyAwsSdk();
        return result;
    }

    int TSqsWorkloadReadScenario::RunScenario(const TClientCommand::TConfig& config) {
        InitStatsCollector(0, WorkersCount);
        InitMeasuringHttpClient(StatsCollector);
        InitSqsClient(config);

        auto finishedFlag = std::make_shared<std::atomic_bool>(false);
        auto queueUrl = GetQueueUrl(Topic, Consumer, QueueName);
        if (queueUrl.empty()) {
            DestroySqsClient();
            return EXIT_FAILURE;
        }

        TSqsWorkloadReaderParams params{
            .TotalSec = TotalSec,
            .QueueUrl = queueUrl,
            .AwsAccessKeyId = AwsAccessKeyId,
            .AwsSessionToken = AwsSessionToken,
            .Log = Log,
            .ErrorFlag = ErrorFlag,
            .SqsClient = SqsClient,
            .Mutex = Mutex,
            .FinishedCond = FinishedCond,
            .StartedCount = StartedCount,
            .WorkersCount = WorkersCount,
            .BatchSize = BatchSize,
            .ErrorMessagesRate = ErrorMessagesRate,
            .ErrorMessagesPolicy = ErrorMessagesPolicy,
            .HandleMessageDelay = TDuration::MilliSeconds(HandleMessageDelayMs),
            .VisibilityTimeout = TDuration::MilliSeconds(VisibilityTimeoutMs),
            .ValidateMessagesOrder = ValidateMessagesOrder,
            .HashMapMutex = std::make_shared<std::mutex>(),
            .LastReceivedMessageInGroup =
                std::make_shared<THashMap<TString, ui64>>(),
            .StatsCollector = StatsCollector,
        };

        auto f = std::async([&params, finishedFlag]() {
            params.StatsCollector->PrintWindowStatsLoop(finishedFlag);
        });

        TSqsWorkloadReader::RunLoop(params, Now() + params.TotalSec);
        {
            std::unique_lock locker(*Mutex);
            while (*StartedCount > 0) {
                FinishedCond->wait(locker);
            }
        }

        finishedFlag->store(true);
        f.wait();

        DestroySqsClient();

        if (AnyErrors() || params.StatsCollector->GetTotalReadMessages() == 0) {
            return EXIT_FAILURE;
        }

        return EXIT_SUCCESS;
    }

} // namespace NYdb::NConsoleClient
