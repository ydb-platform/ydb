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
        config.Opts->AddLongOption("sqs-endpoint", "SQS HTTP endpoint of the queue.")
            .Required()
            .StoreResult(&Scenario.Endpoint);
        config.Opts->AddLongOption("topic", "YDB topic name.")
            .DefaultValue("sqs-workload-topic")
            .StoreResult(&Scenario.Topic);
        config.Opts->AddLongOption("consumer", "YDB consumer name.")
            .DefaultValue("sqs-workload-consumer")
            .StoreResult(&Scenario.Consumer);
        config.Opts->AddLongOption("queue-name", "AWS queue name.")
            .Hidden()
            .StoreResult(&Scenario.QueueName);
        config.Opts->AddLongOption('s', "seconds", "Seconds to run workload.")
            .DefaultValue(60)
            .StoreResult(&Scenario.TotalSec);
        config.Opts
            ->AddLongOption('w', "window", "Output window duration in seconds.")
            .DefaultValue(1)
            .StoreResult(&Scenario.WindowSec);
        config.Opts->AddLongOption("warmup", "Warm-up time in seconds.")
            .DefaultValue(5)
            .StoreResult(&Scenario.WarmupSec);
        config.Opts
            ->AddLongOption('q', "quiet",
                            "Quiet mode. Doesn't print statistics each second.")
            .StoreTrue(&Scenario.Quiet);
        config.Opts
            ->AddLongOption("print-timestamp",
                            "Print timestamp each second with statistics.")
            .StoreTrue(&Scenario.PrintTimestamp);
        config.Opts
            ->AddLongOption("workers", "Number of concurrent workers.")
            .DefaultValue(1)
            .StoreResult(&Scenario.WorkersCount);
        config.Opts->AddLongOption("aws-access-key-id", "AWS access key id.")
            .StoreResult(&Scenario.AwsAccessKeyId);
        config.Opts->AddLongOption("aws-session-token", "AWS session token.")
            .StoreResult(&Scenario.AwsSessionToken);
        config.Opts->AddLongOption("aws-secret-key", "AWS secret access key.")
            .StoreResult(&Scenario.AwsSecretKey);
        config.Opts->AddLongOption('b', "batch-size", "AWS batch size.")
            .DefaultValue(1)
            .StoreResult(&Scenario.BatchSize);
        config.Opts->AddLongOption('m', "message-size", "AWS message size.")
            .DefaultValue(900)
            .StoreResult(&Scenario.MessageSize);
        config.Opts->AddLongOption('g', "message-groups-amount", "Message groups amount.")
            .DefaultValue(0)
            .StoreResult(&Scenario.GroupsAmount);
        config.Opts
            ->AddLongOption('p', "percentile", "Percentile for output statistics.")
            .DefaultValue(80.0)
            .StoreResult(&Scenario.Percentile);
        config.Opts->AddLongOption("use-xml-api", "Use XML API.")
            .DefaultValue(false)
            .StoreTrue(&Scenario.UseXmlAPI);
        config.Opts
            ->AddLongOption("request-timeout", "Request timeout in milliseconds.")
            .DefaultValue(2000)
            .StoreResult(&Scenario.RequestTimeoutMs);
        config.Opts->AddLongOption("aws-region", "AWS region.")
            .Optional()
            .StoreResult(&Scenario.AwsRegion);
        config.Opts->AddLongOption("with-messages-order", "Write messages with order (validation can be enabled in run read command).")
            .DefaultValue(false)
            .StoreTrue(&Scenario.ValidateMessagesOrder);
        config.Opts->AddLongOption("max-unique-messages", "Max unique messages. If set to 0, content based deduplication is used.")
            .DefaultValue(0)
            .StoreResult(&Scenario.MaxUniqueMessages);
    }

    void TCommandWorkloadSqsRunWrite::Parse(TConfig& config) {
        TClientCommand::Parse(config);
    }

    int TCommandWorkloadSqsRunWrite::Run(TConfig& config) {
        return Scenario.Run(config);
    }

} // namespace NYdb::NConsoleClient
