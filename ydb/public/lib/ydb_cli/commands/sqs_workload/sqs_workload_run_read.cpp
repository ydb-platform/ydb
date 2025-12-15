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
        config.Opts->AddLongOption("queue-url", "AWS queue URL.")
            .Required()
            .StoreResult(&Scenario.QueueUrl);
        config.Opts->AddLongOption("endpoint-override", "AWS queue endpoint.")
            .Optional()
            .StoreResult(&Scenario.EndpointOverride);
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
            ->AddLongOption("consumers", "Number of concurrent consumers.")
            .DefaultValue(1)
            .StoreResult(&Scenario.Concurrency);
        config.Opts->AddLongOption('a', "account", "AWS account ID.")
            .Required()
            .StoreResult(&Scenario.Account);
        config.Opts->AddLongOption('t', "token", "AWS token.")
            .Required()
            .StoreResult(&Scenario.Token);
        config.Opts->AddLongOption("error-messages-rate", "Error messages rate.")
            .Optional()
            .ManualDefaultValueDescription(
                "This parameter means that every Nth will not be removed from the queue.")
            .StoreResult(&Scenario.ErrorMessagesRate);
        config.Opts
            ->AddLongOption("error-messages-destiny",
                            "Error messages destiny (fatal, sucess-after-retry).")
            .DefaultValue("fatal")
            .StoreResult(&Scenario.ErrorMessagesDestiny);
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
        config.Opts->AddLongOption("validate-fifo", "Validate FIFO queue.")
            .DefaultValue(false)
            .StoreTrue(&Scenario.ValidateFifo);
        config.Opts->AddLongOption("use-json-api", "Use JSON API.")
            .DefaultValue(false)
            .StoreTrue(&Scenario.UseJsonAPI);
        config.Opts
            ->AddLongOption("request-timeout", "Request timeout in milliseconds.")
            .DefaultValue(2000)
            .StoreResult(&Scenario.RequestTimeoutMs);
        config.Opts->AddLongOption("region", "AWS region.")
            .Optional()
            .StoreResult(&Scenario.Region);
        config.Opts->AddLongOption("set-subject-token", "Set subject token.")
            .DefaultValue(false)
            .Hidden()
            .StoreTrue(&Scenario.SetSubjectToken);
    }

    void TCommandWorkloadSqsRunRead::Parse(TConfig& config) {
        TClientCommand::Parse(config);
    }

    int TCommandWorkloadSqsRunRead::Run(TConfig& config) {
        return Scenario.Run(config);
    }

} // namespace NYdb::NConsoleClient
