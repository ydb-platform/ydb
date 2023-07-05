#include "topic_workload_init.h"

#include "topic_workload_defines.h"
#include "topic_workload_describe.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_common.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>

using namespace NYdb::NConsoleClient;

int TCommandWorkloadTopicInit::TScenario::DoRun(const TConfig& config)
{
    CreateTopic(config.Database, TopicName, PartitionCount, ConsumerCount);

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

    config.Opts->AddLongOption('p', "partitions", "Number of partitions in the topic.")
        .DefaultValue(128)
        .StoreResult(&Scenario.PartitionCount);
    config.Opts->AddLongOption('c', "consumers", "Number of consumers in the topic.")
        .DefaultValue(1)
        .StoreResult(&Scenario.ConsumerCount);
}

void TCommandWorkloadTopicInit::Parse(TConfig& config)
{
    TClientCommand::Parse(config);
}

int TCommandWorkloadTopicInit::Run(TConfig& config)
{
    return Scenario.Run(config);
}
