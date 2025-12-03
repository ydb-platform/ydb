#include "sqs_workload_init.h"

namespace NYdb::NConsoleClient {

TCommandWorkloadSqsInit::TCommandWorkloadSqsInit() : TWorkloadCommand("init", {}, "Init SQS workload") {
}

void TCommandWorkloadSqsInit::Config(TConfig& config) {
    TYdbCommand::Config(config);

    config.SetFreeArgsNum(0);

    config.Opts->AddLongOption('n', "queue-name", "SQS queue name.").
        Required().
        StoreResult(&Scenario.QueueName);
    config.Opts->AddLongOption("fifo", "SQS FIFO queue.").
        DefaultValue(false).
        StoreTrue(&Scenario.Fifo);
    config.Opts->AddLongOption("deduplication-on", "SQS deduplication on.").
        DefaultValue(false).
        StoreTrue(&Scenario.DeduplicationOn);
    config.Opts->AddLongOption("dlq-queue-name", "SQS DLQ queue name.").
        Optional().
        StoreResult(&Scenario.DlqQueueName);
    config.Opts->AddLongOption('a', "account", "AWS account ID.").
        Required().
        StoreResult(&Scenario.Account);
    config.Opts->AddLongOption('t', "token", "AWS token.").
        Required().
        StoreResult(&Scenario.Token);
    config.Opts->AddLongOption('e', "endpoint", "AWS queue endpoint.").
        DefaultValue("sqs.yandex.net:8771").
        StoreResult(&Scenario.EndPoint);
    config.Opts->AddLongOption("max-receive-count", "SQS max receive count.").
        DefaultValue(5).
        StoreResult(&Scenario.MaxReceiveCount);
}

void TCommandWorkloadSqsInit::Parse(TConfig& config) { TClientCommand::Parse(config); }

int TCommandWorkloadSqsInit::Run(TConfig& config) { return Scenario.Run(config); }

}