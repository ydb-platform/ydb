#include "topic_workload_run_read.h"
#include "topic_workload_defines.h"

using namespace NYdb::NConsoleClient;

TCommandWorkloadTopicRunRead::TCommandWorkloadTopicRunRead()
    : TWorkloadCommand("read", {}, "Read workload")
{
}

void TCommandWorkloadTopicRunRead::Config(TConfig& config)
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
    config.Opts->AddLongOption("no-consumer", "Read without consumer")
        .Hidden()
        .StoreTrue(&Scenario.ReadWithoutConsumer);

    // Specific params
    config.Opts->AddLongOption("consumer-prefix", "Use consumers with names '<consumer-prefix>-0' ... '<consumer-prefix>-<n-1>' where n is set in the '--consumers' option.")
        .DefaultValue(CONSUMER_PREFIX)
        .StoreResult(&Scenario.ConsumerPrefix);
    config.Opts->AddLongOption('c', "consumers", "Number of consumers in a topic.")
        .DefaultValue(1)
        .StoreResult(&Scenario.ConsumerCount);
    config.Opts->AddLongOption('t', "threads", "Number of consumer threads.")
        .DefaultValue(1)
        .StoreResult(&Scenario.ConsumerThreadCount);

    config.IsNetworkIntensive = true;
}

void TCommandWorkloadTopicRunRead::Parse(TConfig& config)
{
    TClientCommand::Parse(config);

    Scenario.EnsurePercentileIsValid();
    Scenario.EnsureWarmupSecIsValid();
}

int TCommandWorkloadTopicRunRead::Run(TConfig& config)
{
    return Scenario.Run(config);
}
