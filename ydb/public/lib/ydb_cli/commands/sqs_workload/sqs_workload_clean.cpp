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
    config.Opts->AddLongOption('a', "account", "AWS account ID.").
        Required().
        StoreResult(&Scenario.Account);
    config.Opts->AddLongOption('t', "token", "AWS token.").
        Required().
        StoreResult(&Scenario.Token);
    config.Opts->AddLongOption('e', "endpoint", "AWS queue endpoint.").
        DefaultValue("sqs.yandex.net:8771").
        StoreResult(&Scenario.EndPoint);
}

void TCommandWorkloadSqsClean::Parse(TConfig& config) { TClientCommand::Parse(config); }

int TCommandWorkloadSqsClean::Run(TConfig& config) { return Scenario.Run(config); }

}

