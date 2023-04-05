#include "topic_workload_clean.h"

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
}

void TCommandWorkloadTopicClean::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandWorkloadTopicClean::Run(TConfig& config) {
    Driver = std::make_unique<NYdb::TDriver>(CreateDriver(config));
    auto topicClient = std::make_unique<NYdb::NTopic::TTopicClient>(*Driver);

    auto topicName = config.Database + "/" + TOPIC;
    auto describeTopicResult = topicClient->DescribeTopic(topicName, {}).GetValueSync();
    if (describeTopicResult.GetTopicDescription().GetTotalPartitionsCount() == 0) {
        Cout << "Topic " << topicName << " does not exists.\n";
        return EXIT_FAILURE;
    }

    auto result = topicClient->DropTopic(config.Database + "/" + TOPIC).GetValueSync();
    ThrowOnError(result);
    return EXIT_SUCCESS;
}