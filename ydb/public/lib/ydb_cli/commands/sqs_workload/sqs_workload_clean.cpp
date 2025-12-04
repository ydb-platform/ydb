#include "sqs_workload_clean.h"

namespace NYdb::NConsoleClient {

TCommandWorkloadSqsClean::TCommandWorkloadSqsClean() : TWorkloadCommand("clean", {}, "Clean SQS workload queue") {
}

void TCommandWorkloadSqsClean::Config(TConfig& config) {
    TYdbCommand::Config(config);

    config.SetFreeArgsNum(0);

    config.Opts->AddLongOption('n', "queue-name", "SQS queue name.").
        Required().
        StoreResult(&Scenario.QueueName);
    config.Opts->AddLongOption("topic-path", "YDB topic path.").
        Required().
        StoreResult(&Scenario.TopicPath);
}

void TCommandWorkloadSqsClean::Parse(TConfig& config) { TClientCommand::Parse(config); }

int TCommandWorkloadSqsClean::Run(TConfig& config) { return Scenario.Run(config); }

}

