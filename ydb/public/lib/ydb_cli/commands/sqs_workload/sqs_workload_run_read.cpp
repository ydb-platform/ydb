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
            ->AddLongOption("percentile", "Percentile for output statistics.")
            .DefaultValue(80.0)
            .Hidden()
            .StoreResult(&Scenario.Percentile);
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
        config.Opts->AddLongOption("error-messages-rate", "Error messages rate.")
            .Optional()
            .Hidden()
            .ManualDefaultValueDescription(
                "This parameter means that every Nth received message will be "
                "handled with error.")
            .StoreResult(&Scenario.ErrorMessagesRate);
        config.Opts
            ->AddLongOption("error-messages-destiny",
                            "Error messages destiny (fatal, sucess-after-retry).")
            .DefaultValue("fatal")
            .Hidden()
            .StoreResult(&Scenario.ErrorMessagesDestiny);
        config.Opts
            ->AddLongOption("visibility-timeout",
                            "Visibility timeout in milliseconds.")
            .DefaultValue(1000)
            .Hidden()
            .StoreResult(&Scenario.VisibilityTimeoutMs);
        config.Opts
            ->AddLongOption("handle-message-time",
                            "Handle message time in milliseconds.")
            .DefaultValue(0) // 0 means no delay
            .Hidden()
            .StoreResult(&Scenario.HandleMessageDelayMs);
        config.Opts->AddLongOption("batch-size", "Batch size.")
            .DefaultValue(1)
            .Hidden()
            .StoreResult(&Scenario.BatchSize);
        config.Opts->AddLongOption("validate-fifo", "Validate FIFO queue.")
            .DefaultValue(false)
            .Hidden()
            .StoreTrue(&Scenario.ValidateFifo);
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

    void TCommandWorkloadSqsRunRead::Parse(TConfig& config) {
        TClientCommand::Parse(config);
    }

    int TCommandWorkloadSqsRunRead::Run(TConfig& config) {
        return Scenario.Run(config);
    }

} // namespace NYdb::NConsoleClient
