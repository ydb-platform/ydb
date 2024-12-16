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
    config.Opts->AddLongOption('t', "threads", "Number of producer threads.")
        .DefaultValue(1)
        .StoreResult(&Scenario.ProducerThreadCount);
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
    config.Opts->AddLongOption("commit-period", "Waiting time between commit.")
        .DefaultValue(10)
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
