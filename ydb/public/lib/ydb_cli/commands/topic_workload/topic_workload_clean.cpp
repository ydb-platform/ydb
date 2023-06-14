#include "topic_workload_clean.h"

#include "topic_workload_describe.h"
#include "topic_workload_defines.h"

#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_common.h>


using namespace NYdb::NConsoleClient;

TCommandWorkloadTopicClean::TCommandWorkloadTopicClean()
    : TWorkloadCommand("clean", {}, "drop topic created in init phase")
{
}

void TCommandWorkloadTopicClean::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.SetFreeArgsNum(0);

    config.Opts->AddLongOption("topic", "Topic name.")
        .DefaultValue(TOPIC)
        .StoreResult(&TopicName);
}

void TCommandWorkloadTopicClean::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandWorkloadTopicClean::Run(TConfig& config) {
    Driver = std::make_unique<NYdb::TDriver>(CreateDriver(config));
    auto topicClient = std::make_unique<NYdb::NTopic::TTopicClient>(*Driver);

    TCommandWorkloadTopicDescribe::DescribeTopic(config.Database, TopicName, *Driver);

    TString fullTopicName = TCommandWorkloadTopicDescribe::GenerateFullTopicName(config.Database, TopicName);
    auto result = topicClient->DropTopic(fullTopicName).GetValueSync();
    ThrowOnError(result);
    return EXIT_SUCCESS;
}