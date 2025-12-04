#include "sqs_workload_run_write.h"

#include <ydb/library/backup/util.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_service_topic.h>
#include <util/stream/format.h>

namespace NYdb::NConsoleClient {

TCommandWorkloadSqsRunWrite::TCommandWorkloadSqsRunWrite() : TWorkloadCommand("write", {}, "Write workload") {}

void TCommandWorkloadSqsRunWrite::Config(TConfig& config) {
    TYdbCommand::Config(config);

    config.SetFreeArgsNum(0);

    // Common params
    config.Opts->AddLongOption('s', "seconds", "Seconds to run workload.")
        .DefaultValue(60)
        .StoreResult(&Scenario.TotalSec);
    config.Opts->AddLongOption('w', "window", "Output window duration in seconds.")
        .DefaultValue(1)
        .StoreResult(&Scenario.WindowSec);
    config.Opts->AddLongOption("warmup", "Warm-up time in seconds.")
        .DefaultValue(5)
        .StoreResult(&Scenario.WarmupSec);
    config.Opts->AddLongOption('q', "quiet", "Quiet mode. Doesn't print statistics each second.")
        .StoreTrue(&Scenario.Quiet);
    config.Opts->AddLongOption("print-timestamp", "Print timestamp each second with statistics.")
        .StoreTrue(&Scenario.PrintTimestamp);
    config.Opts->AddLongOption('c', "concurrent", "Number of concurrent readers.")
        .DefaultValue(1)
        .StoreResult(&Scenario.Concurrency);
    config.Opts->AddLongOption('a', "account", "AWS account ID.").Required().StoreResult(&Scenario.Account);
    config.Opts->AddLongOption('n', "queue-name", "AWS queue name.").Required().StoreResult(&Scenario.QueueName);
    config.Opts->AddLongOption('t', "token", "AWS token.").Required().StoreResult(&Scenario.Token);
    config.Opts->AddLongOption('e', "endpoint", "AWS queue endpoint.")
        .DefaultValue("sqs.yandex.net:8771")
        .StoreResult(&Scenario.EndPoint);
    config.Opts->AddLongOption('b', "batch-size", "AWS batch size.").DefaultValue(1).StoreResult(&Scenario.BatchSize);
    config.Opts->AddLongOption('m', "message-size", "AWS message size.")
        .DefaultValue(900)
        .StoreResult(&Scenario.MessageSize);
    config.Opts->AddLongOption('z', "sleep-time", "Sleep time in milliseconds.")
        .DefaultValue(10)
        .StoreResult(&Scenario.SleepTimeMs);
    config.Opts->AddLongOption('g', "groups-amount", "Groups amount.")
        .DefaultValue(0)
        .StoreResult(&Scenario.GroupsAmount);
}

void TCommandWorkloadSqsRunWrite::Parse(TConfig& config) { TClientCommand::Parse(config); }

int TCommandWorkloadSqsRunWrite::Run(TConfig& config) { return Scenario.Run(config); }

}  // namespace NYdb::NConsoleClient
