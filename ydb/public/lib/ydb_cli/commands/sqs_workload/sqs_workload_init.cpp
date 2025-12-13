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
            .Required()
            .Hidden()
            .StoreResult(&Scenario.TopicPath);
        config.Opts->AddLongOption('n', "queue-name", "SQS queue name.")
            .Required()
            .Hidden()
            .StoreResult(&Scenario.QueueName);
        config.Opts->AddLongOption("keep-messages-order", "Keep messages order.")
            .DefaultValue(false)
            .Hidden()
            .StoreTrue(&Scenario.KeepMessagesOrder);
        config.Opts
            ->AddLongOption("default-processing-timeout",
                            "Default processing timeout.")
            .Optional()
            .Hidden()
            .StoreMappedResult(&Scenario.DefaultProcessingTimeout, ParseDuration);
        config.Opts->AddLongOption("deduplication-on", "SQS deduplication on.")
            .DefaultValue(false)
            .Hidden()
            .StoreTrue(&Scenario.DeduplicationOn);
        config.Opts->AddLongOption("dlq-queue-name", "SQS DLQ queue name.")
            .Optional()
            .Hidden()
            .StoreResult(&Scenario.DlqQueueName);
        config.Opts->AddLongOption("max-receive-count", "SQS max receive count.")
            .DefaultValue(0)
            .Hidden()
            .StoreResult(&Scenario.MaxReceiveCount);
    }

    void TCommandWorkloadSqsInit::Parse(TConfig& config) {
        TClientCommand::Parse(config);
    }

    int TCommandWorkloadSqsInit::Run(TConfig& config) {
        return Scenario.Run(config);
    }

} // namespace NYdb::NConsoleClient
