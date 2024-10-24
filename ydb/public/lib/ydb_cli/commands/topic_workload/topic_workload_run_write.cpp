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
    config.Opts->AddLongOption('t', "threads", "Number of parallel writer threads.")
        .DefaultValue(1)
        .StoreResult(&Scenario.ProducerThreadCount);
    config.Opts->AddLongOption("producers-per-thread",
                               "Number of producers in every writer thread. "
                               "Every producers writes to only one dedicated to it partition. "
                               "If you want to write to all partitions set this param to the number of partitions in the topic. "
                               "If you use transactions, they will span across all producers that will write during the transaction "
                               "(e.g. if you have 100 messages per second, transaction commit interval 100ms and 100 producers per thread, "
                               "in 100 ms commit interval only 10 messages will fit. All those 10 messages will be written by 10 different producers to 10 different partitions in the same transaction).")
        .DefaultValue(1)
        .StoreResult(&Scenario.ProducersPerThread);
    config.Opts->AddLongOption('m', "message-size", "Message size.")
        .DefaultValue(10_KB)
        .StoreMappedResultT<TString>(&Scenario.MessageSize, &TCommandWorkloadTopicParams::StrToBytes);
    config.Opts->AddLongOption("message-rate", "Total message rate for all producer threads (messages per second). Exclusive with --byte-rate.")
        .DefaultValue(0)
        .StoreResult(&Scenario.MessageRate);
    config.Opts->AddLongOption("byte-rate", "Total message rate for all producer threads (bytes per second). Exclusive with --message-rate.")
        .DefaultValue(0)
        .StoreMappedResultT<TString>(&Scenario.ByteRate, &TCommandWorkloadTopicParams::StrToBytes);
    config.Opts->AddLongOption("codec", PrepareAllowedCodecsDescription("Client-side compression algorithm. When read, data will be uncompressed transparently with a codec used on write", InitAllowedCodecs()))
        .Optional()
        .DefaultValue((TStringBuilder() << NTopic::ECodec::RAW))
        .StoreMappedResultT<TString>(&Scenario.Codec, &TCommandWorkloadTopicParams::StrToCodec);
    config.Opts->AddLongOption("direct", "Direct write to a partition node.")
        .Hidden()
        .StoreTrue(&Scenario.Direct);

    config.Opts->MutuallyExclusive("message-rate", "byte-rate");

    config.Opts->AddLongOption("use-tx", "Use transactions.")
        .Optional()
        .DefaultValue(false)
        .StoreTrue(&Scenario.UseTransactions);
    config.Opts->AddLongOption("commit-period", "Waiting time between commit in milliseconds.")
        .DefaultValue(100)
        .StoreResult(&Scenario.CommitPeriod);
    config.Opts->AddLongOption("commit-messages", "Number of messages per transaction")
        .DefaultValue(1'000'000)
        .StoreResult(&Scenario.CommitMessages);

    config.IsNetworkIntensive = true;
}

void TCommandWorkloadTopicRunWrite::Parse(TConfig& config) 
{
    TClientCommand::Parse(config);

    Scenario.EnsurePercentileIsValid();
    Scenario.EnsureWarmupSecIsValid();
}

int TCommandWorkloadTopicRunWrite::Run(TConfig& config)
{
    return Scenario.Run(config);
}
