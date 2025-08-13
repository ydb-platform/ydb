#include "topic_workload_run_write.h"
#include "topic_workload_defines.h"
#include "topic_workload_params.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_service_topic.h>

using namespace NYdb::NConsoleClient;

TCommandWorkloadTopicRunWrite::TCommandWorkloadTopicRunWrite()
    : TWorkloadCommand("write", {}, "Write workload")
{
}

void TCommandWorkloadTopicRunWrite::Config(TConfig& config)
{
    TYdbCommand::Config(config);

    config.SetFreeArgsNum(0);

    // Common params
    config.Opts->AddLongOption('s', "seconds", "Seconds to run workload.")
        .DefaultValue(60)
        .StoreResult(&Scenario.TotalSec);
    config.Opts->AddLongOption('w', "window", "Output window duration in seconds.")
        .DefaultValue(1)
        .StoreResult(&Scenario.WindowSec);
    config.Opts->AddLongOption('q', "quiet", "Quiet mode. Doesn't print statistics each second.")
        .StoreTrue(&Scenario.Quiet);
    config.Opts->AddLongOption("print-timestamp", "Print timestamp each second with statistics.")
        .StoreTrue(&Scenario.PrintTimestamp);
    config.Opts->AddLongOption("percentile", "Percentile for output statistics.")
        .DefaultValue(50)
        .StoreResult(&Scenario.Percentile);
    config.Opts->AddLongOption("warmup", "Warm-up time in seconds.")
        .DefaultValue(5)
        .StoreResult(&Scenario.WarmupSec);
    config.Opts->AddLongOption("topic", "Topic name.")
        .DefaultValue(TOPIC)
        .StoreResult(&Scenario.TopicName);

    // Specific params
    config.Opts->AddLongOption('t', "threads", "Number of writer threads. Every writer thread will produce messages to all topic partitions in round robin manner.")
        .DefaultValue(1)
        .StoreResult(&Scenario.ProducerThreadCount);
    config.Opts->AddLongOption("use-cpu-timestamp", "If specified, worload will use cpu current timestamp as message create ts to measure latency."
                                                        " If not specified, will be used expected creation timestamp: e.g. "
                                                        "if we have 100 messages per second rate, then first message is expected at 0ms of first second, second after 10ms elapsed,"
                                                        " third after 20ms elapsed, 10th after 100ms elapsed and so on. "
                                                        "This way 101 message is expected to be generated not earlier"
                                                        " then 1 second and 10 ms after test start has passed."
                                                        )
        .DefaultValue(false)
        .Optional()
        .Hidden()
        .StoreTrue(&Scenario.UseCpuTimestamp);
    config.Opts->AddLongOption('m', "message-size", "Message size.")
        .DefaultValue(10_KB)
        .StoreMappedResult(&Scenario.MessageSizeBytes, &TCommandWorkloadTopicParams::StrToBytes);
    config.Opts->AddLongOption("message-rate", "Total message rate for all producer threads (messages per second). Exclusive with --byte-rate.")
        .DefaultValue(0)
        .StoreResult(&Scenario.MessagesPerSec);
    config.Opts->AddLongOption("byte-rate", "Total message rate for all producer threads (bytes per second). Exclusive with --message-rate.")
        .DefaultValue(0)
        .StoreMappedResult(&Scenario.BytesPerSec, &TCommandWorkloadTopicParams::StrToBytes);
    config.Opts->AddLongOption("codec", PrepareAllowedCodecsDescription("Client-side compression algorithm. When read, data will be uncompressed transparently with a codec used on write", InitAllowedCodecs()))
        .Optional()
        .DefaultValue((TStringBuilder() << NTopic::ECodec::RAW))
        .StoreMappedResult(&Scenario.Codec, &TCommandWorkloadTopicParams::StrToCodec);
    config.Opts->AddLongOption("direct", "Direct write to a partition node.")
        .Hidden()
        .StoreTrue(&Scenario.Direct);
    config.Opts->AddLongOption("key-prefix", "Generate keys with this prefix. Put pair '__key':'{key-prefix}.{key-index}' in the message metadata.")
        .Optional()
        .Hidden()
        .StoreResult(&Scenario.KeyPrefix);
    config.Opts->AddLongOption("key-count", "The number of different keys to generate. The --key-prefix parameter must be set.")
        .Optional()
        .Hidden()
        .DefaultValue(1)
        .StoreResult(&Scenario.KeyCount);

    config.Opts->MutuallyExclusive("message-rate", "byte-rate");

    config.Opts->AddLongOption("use-tx", "Use transactions.")
        .Optional()
        .DefaultValue(false)
        .StoreTrue(&Scenario.UseTransactions);
    config.Opts->AddLongOption("commit-period", "DEPRECATED: use tx-commit-intervall-ms instead. Waiting time between commit in seconds. Default - 1 second")
        .Optional()
        .Hidden()
        .StoreResult(&Scenario.CommitPeriodSeconds);
    config.Opts->AddLongOption("tx-commit-interval", "Interval of transaction commit in milliseconds. "
                                                            " Both tx-commit-messages and tx-commit-interval can trigger transaction commit. Default: 1000 ms.")
        .Optional()
        .StoreResult(&Scenario.TxCommitIntervalMs);

    config.Opts->MutuallyExclusive("commit-period", "tx-commit-interval");

    config.Opts->AddLongOption("commit-messages", "DEPRECATED. Use --tx-commit-messages instead. Number of messages per transaction")
        .DefaultValue(1'000'000)
        .Hidden()
        .StoreResult(&Scenario.CommitMessages);
    config.Opts->AddLongOption("tx-commit-messages", "Number of messages to commit transaction. "
                                                            " Both tx-commit-messages and tx-commit-interval can trigger transaction commit.")
        .DefaultValue(1'000'000)
        .StoreResult(&Scenario.CommitMessages);

    config.IsNetworkIntensive = true;
}

void TCommandWorkloadTopicRunWrite::Parse(TConfig& config)
{
    TClientCommand::Parse(config);

    Scenario.EnsurePercentileIsValid();
    Scenario.EnsureWarmupSecIsValid();
    Scenario.EnsureRatesIsValid();
}

int TCommandWorkloadTopicRunWrite::Run(TConfig& config)
{
    return Scenario.Run(config);
}
