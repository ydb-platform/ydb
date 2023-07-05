#include "topic_workload_clean.h"

#include "topic_workload_describe.h"
#include "topic_workload_defines.h"

#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_common.h>


using namespace NYdb::NConsoleClient;

int TCommandWorkloadTopicClean::TScenario::DoRun(const TConfig& config)
{
    TCommandWorkloadTopicDescribe::DescribeTopic(config.Database, TopicName, *Driver);

    DropTopic(config.Database, TopicName);

    return EXIT_SUCCESS;
}

TCommandWorkloadTopicClean::TCommandWorkloadTopicClean()
    : TWorkloadCommand("clean", {}, "drop topic created in init phase")
{
}

void TCommandWorkloadTopicClean::Config(TConfig& config)
{
    TYdbCommand::Config(config);
    config.SetFreeArgsNum(0);

    config.Opts->AddLongOption("topic", "Topic name.")
        .DefaultValue(TOPIC)
        .StoreResult(&Scenario.TopicName);
}

void TCommandWorkloadTopicClean::Parse(TConfig& config)
{
    TClientCommand::Parse(config);
}

int TCommandWorkloadTopicClean::Run(TConfig& config)
{
    return Scenario.Run(config);
}
