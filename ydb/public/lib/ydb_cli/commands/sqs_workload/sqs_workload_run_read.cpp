#include "sqs_workload_run_read.h"

#include <util/stream/format.h>
#include <ydb/library/backup/util.h>

namespace NYdb::NConsoleClient {

    TCommandWorkloadSqsRunRead::TCommandWorkloadSqsRunRead()
        : TWorkloadCommand("read", {}, "Read workload")
    {
    }

    void TCommandWorkloadSqsRunRead::Config(TConfig& config) {
        TYdbCommand::Config(config);

        config.SetFreeArgsNum(0);

        // Common params
        config.Opts->AddLongOption("sqs-endpoint", "SQS HTTP endpoint of the queue.")
            .Required()
            .StoreResult(&Scenario.Endpoint);
        config.Opts->AddLongOption("queue-name", "AWS queue name.")
            .Hidden()
            .StoreResult(&Scenario.QueueName);
        config.Opts->AddLongOption("topic", "YDB topic name.")
            .DefaultValue("sqs-workload-topic")
            .StoreResult(&Scenario.Topic);
        config.Opts->AddLongOption("consumer", "YDB consumer name.")
            .DefaultValue("sqs-workload-consumer")
            .StoreResult(&Scenario.Consumer);
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
            ->AddLongOption("percentile", "Percentile for output statistics.")
            .DefaultValue(80.0)
            .StoreResult(&Scenario.Percentile);
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
        config.Opts->AddLongOption("keep-error-every", "Keep every Nth error message in the queue (do not delete it). 0 = delete all messages; 1 = keep all messages")
            .StoreResult(&Scenario.ErrorMessagesRate);
        config.Opts
            ->AddLongOption("error-policy",
                            "Error messages destiny (fatal, success-after-retry).")
            .ManualDefaultValueDescription(
                "How to treat errors:\n"
                            "  - fatal - stop immediately and return non-zero exit code\n"
                            "  - success-after-retry - retry; if retry succeeds, exit with 0 (errors are not fatal)\n"
                            "(default: fatal)")
            .DefaultValue("fatal")
            .StoreResult(&Scenario.ErrorMessagesPolicy);
        config.Opts
            ->AddLongOption("visibility-timeout",
                            "Visibility timeout in milliseconds.")
            .DefaultValue(1000)
            .StoreResult(&Scenario.VisibilityTimeoutMs);
        config.Opts
            ->AddLongOption("handle-message-time",
                            "Handle message time in milliseconds.")
            .DefaultValue(0) // 0 means no delay
            .StoreResult(&Scenario.HandleMessageDelayMs);
        config.Opts->AddLongOption("batch-size", "Batch size.")
            .DefaultValue(1)
            .StoreResult(&Scenario.BatchSize);
        config.Opts->AddLongOption("validate-messages-order", "Validate messages order.")
            .DefaultValue(false)
            .StoreTrue(&Scenario.ValidateMessagesOrder);
        config.Opts->AddLongOption("use-xml-api", "Use XML API.")
            .DefaultValue(false)
            .StoreTrue(&Scenario.UseXmlAPI);
        config.Opts
            ->AddLongOption("request-timeout", "Request timeout in milliseconds.")
            .DefaultValue(2000)
            .StoreResult(&Scenario.RequestTimeoutMs);
        config.Opts->AddLongOption("aws-region", "AWS region.")
            .StoreResult(&Scenario.AwsRegion);
    }

    void TCommandWorkloadSqsRunRead::Parse(TConfig& config) {
        TClientCommand::Parse(config);
    }

    int TCommandWorkloadSqsRunRead::Run(TConfig& config) {
        return Scenario.Run(config);
    }

} // namespace NYdb::NConsoleClient
