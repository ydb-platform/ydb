#include "topic_workload_init.h"

#include "topic_workload_defines.h"
#include "topic_workload_describe.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_common.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>

using namespace NYdb::NConsoleClient;

int TCommandWorkloadTopicInit::TScenario::DoRun(const TConfig& config)
{
    CreateTopic(config.Database, TopicName, TopicPartitionCount, ConsumerCount, TopicAutoscaling, TopicMaxPartitionCount, StabilizationWindowSeconds, UpUtilizationPercent, DownUtilizationPercent);

    return EXIT_SUCCESS;
}

TCommandWorkloadTopicInit::TCommandWorkloadTopicInit()
    : TWorkloadCommand("init", {}, "Create and initialize topic for workload")
{
}

void TCommandWorkloadTopicInit::Config(TConfig& config)
{
    TYdbCommand::Config(config);

    config.SetFreeArgsNum(0);

    config.Opts->AddLongOption("topic", "Topic name.")
        .DefaultValue(TOPIC)
        .StoreResult(&Scenario.TopicName);
    config.Opts->AddLongOption("consumer-prefix", "Use consumers with names '<consumer-prefix>-0' ... '<consumer-prefix>-<n-1>' where n is set in the '--consumers' option.")
        .DefaultValue(CONSUMER_PREFIX)
        .StoreResult(&Scenario.ConsumerPrefix);

    config.Opts->AddLongOption('p', "partitions", "Number of partitions in the topic.")
        .DefaultValue(128)
        .StoreResult(&Scenario.TopicPartitionCount);
    config.Opts->AddLongOption('c', "consumers", "Number of consumers in the topic.")
        .DefaultValue(1)
        .StoreResult(&Scenario.ConsumerCount);


    config.Opts->AddLongOption('a', "auto-partitioning", "Enable autopartitioning of topic.")
        .DefaultValue(false)
        .StoreTrue(&Scenario.TopicAutoscaling);
    config.Opts->AddLongOption('m', "auto-partitioning-max-partitions-count", "Max number of partitions in the topic.")
        .StoreResult(&Scenario.TopicMaxPartitionCount);
    config.Opts->AddLongOption("auto-partitioning-stabilization-window-seconds", "Duration in seconds of high or low load before automatically scale the number of partitions")
        .Optional()
        .StoreResult(&Scenario.StabilizationWindowSeconds);
    config.Opts->AddLongOption("auto-partitioning-up-utilization-percent", "The load percentage at which the number of partitions will increase")
        .Optional()
        .StoreResult(&Scenario.UpUtilizationPercent);
    config.Opts->AddLongOption("auto-partitioning-down-utilization-percent", "The load percentage at which the number of partitions will decrease")
        .Optional()
        .StoreResult(&Scenario.DownUtilizationPercent);
}

void TCommandWorkloadTopicInit::Parse(TConfig& config)
{
    TClientCommand::Parse(config);
}

int TCommandWorkloadTopicInit::Run(TConfig& config)
{
    return Scenario.Run(config);
}
