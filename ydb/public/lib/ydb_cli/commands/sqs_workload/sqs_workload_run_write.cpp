#include "sqs_workload_run_write.h"

#include <util/stream/format.h>
#include <ydb/library/backup/util.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_service_topic.h>

namespace NYdb::NConsoleClient {

    TCommandWorkloadSqsRunWrite::TCommandWorkloadSqsRunWrite()
        : TWorkloadCommand("write", {}, "Write workload")
    {
    }

    void TCommandWorkloadSqsRunWrite::Config(TConfig& config) {
        TYdbCommand::Config(config);

        config.SetFreeArgsNum(0);

        // Common params
        config.Opts->AddLongOption("queue-url", "AWS queue URL.")
            .Required()
            .Hidden()
            .StoreResult(&Scenario.QueueUrl);
        config.Opts->AddLongOption("endpoint-override", "AWS queue endpoint.")
            .Optional()
            .Hidden()
            .StoreResult(&Scenario.EndpointOverride);
        config.Opts->AddLongOption('s', "seconds", "Seconds to run workload.")
            .DefaultValue(60)
            .Hidden()
            .StoreResult(&Scenario.TotalSec);
        config.Opts
            ->AddLongOption('w', "window", "Output window duration in seconds.")
            .DefaultValue(1)
            .Hidden()
            .StoreResult(&Scenario.WindowSec);
        config.Opts->AddLongOption("warmup", "Warm-up time in seconds.")
            .DefaultValue(5)
            .Hidden()
            .StoreResult(&Scenario.WarmupSec);
        config.Opts
            ->AddLongOption('q', "quiet",
                            "Quiet mode. Doesn't print statistics each second.")
            .Hidden()
            .StoreTrue(&Scenario.Quiet);
        config.Opts
            ->AddLongOption("print-timestamp",
                            "Print timestamp each second with statistics.")
            .Hidden()
            .StoreTrue(&Scenario.PrintTimestamp);
        config.Opts
            ->AddLongOption('c', "concurrent", "Number of concurrent readers.")
            .DefaultValue(1)
            .Hidden()
            .StoreResult(&Scenario.Concurrency);
        config.Opts->AddLongOption('a', "account", "AWS account ID.")
            .Required()
            .Hidden()
            .StoreResult(&Scenario.Account);
        config.Opts->AddLongOption('t', "token", "AWS token.")
            .Required()
            .Hidden()
            .StoreResult(&Scenario.Token);
        config.Opts->AddLongOption('b', "batch-size", "AWS batch size.")
            .DefaultValue(1)
            .Hidden()
            .StoreResult(&Scenario.BatchSize);
        config.Opts->AddLongOption('m', "message-size", "AWS message size.")
            .DefaultValue(900)
            .Hidden()
            .StoreResult(&Scenario.MessageSize);
        config.Opts->AddLongOption('g', "groups-amount", "Groups amount.")
            .DefaultValue(0)
            .Hidden()
            .StoreResult(&Scenario.GroupsAmount);
        config.Opts
            ->AddLongOption('p', "percentile", "Percentile for output statistics.")
            .DefaultValue(80.0)
            .Hidden()
            .StoreResult(&Scenario.Percentile);
        config.Opts->AddLongOption("use-json-api", "Use JSON API.")
            .DefaultValue(false)
            .Hidden()
            .StoreTrue(&Scenario.UseJsonAPI);
        config.Opts
            ->AddLongOption("request-timeout", "Request timeout in milliseconds.")
            .DefaultValue(2000)
            .Hidden()
            .StoreResult(&Scenario.RequestTimeoutMs);
        config.Opts->AddLongOption("region", "AWS region.")
            .Optional()
            .Hidden()
            .StoreResult(&Scenario.Region);
        config.Opts->AddLongOption("set-subject-token", "Set subject token.")
            .DefaultValue(false)
            .Hidden()
            .StoreTrue(&Scenario.SetSubjectToken);
    }

    void TCommandWorkloadSqsRunWrite::Parse(TConfig& config) {
        TClientCommand::Parse(config);
    }

    int TCommandWorkloadSqsRunWrite::Run(TConfig& config) {
        return Scenario.Run(config);
    }

} // namespace NYdb::NConsoleClient
