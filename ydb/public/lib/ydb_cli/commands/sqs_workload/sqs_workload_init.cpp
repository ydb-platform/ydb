#include "sqs_workload_init.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_common.h>

namespace NYdb::NConsoleClient {

    TCommandWorkloadSqsInit::TCommandWorkloadSqsInit()
        : TWorkloadCommand("init", {}, "Init SQS workload")
    {
    }

    void TCommandWorkloadSqsInit::Config(TConfig& config) {
        TYdbCommand::Config(config);

        config.SetFreeArgsNum(0);

        config.Opts->AddLongOption("topic-path", "YDB topic path.")
            .DefaultValue("sqs-workload-topic")
            .StoreResult(&Scenario.TopicPath);
        config.Opts->AddLongOption('c', "consumer", "SQS consumer name.")
            .DefaultValue("sqs-workload-consumer")
            .StoreResult(&Scenario.Consumer);
        config.Opts->AddLongOption("topic-partition-count", "YDB topic partition count.")
            .DefaultValue(1)
            .StoreResult(&Scenario.TopicPartitionCount);
        config.Opts->AddLongOption("keep-messages-order", "Keep messages order.")
            .DefaultValue(false)
            .StoreTrue(&Scenario.KeepMessagesOrder);
        config.Opts
            ->AddLongOption("default-processing-timeout",
                            "Default processing timeout.")
            .Optional()
            .StoreMappedResult(&Scenario.DefaultProcessingTimeout, ParseDuration);
        config.Opts->AddLongOption("dlq-queue-name", "SQS DLQ queue name.")
            .StoreResult(&Scenario.DlqQueueName);
        config.Opts->AddLongOption("max-receive-count", "SQS max receive count.")
            .DefaultValue(0)
            .StoreResult(&Scenario.MaxReceiveCount);
    }

    void TCommandWorkloadSqsInit::Parse(TConfig& config) {
        TClientCommand::Parse(config);
    }

    int TCommandWorkloadSqsInit::Run(TConfig& config) {
        return Scenario.Run(config);
    }

} // namespace NYdb::NConsoleClient
