#include "topic_workload_init.h"

#include "topic_workload_defines.h"
#include "topic_workload_describe.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_common.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>

using namespace NYdb::NConsoleClient;

TCommandWorkloadTopicInit::TCommandWorkloadTopicInit()
    : TWorkloadCommand("init", {}, "Create and initialize topic for workload")
    , PartitionCount(1)
{
}

void TCommandWorkloadTopicInit::Config(TConfig& config) {
    TYdbCommand::Config(config);

    config.SetFreeArgsNum(0);

    config.Opts->AddLongOption("topic", "Topic name.")
        .DefaultValue(TOPIC)
        .StoreResult(&TopicName);

    config.Opts->AddLongOption('p', "partitions", "Number of partitions in the topic.")
        .DefaultValue(128)
        .StoreResult(&PartitionCount);
    config.Opts->AddLongOption('c', "consumers", "Number of consumers in the topic.")
        .DefaultValue(1)
        .StoreResult(&ConsumerCount);
}

void TCommandWorkloadTopicInit::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandWorkloadTopicInit::Run(TConfig& config) {
    Driver = std::make_unique<NYdb::TDriver>(CreateDriver(config));
    auto topicClient = std::make_unique<NYdb::NTopic::TTopicClient>(*Driver);

    auto fullTopicName = TCommandWorkloadTopicDescribe::GenerateFullTopicName(config.Database, TopicName);
    auto describeTopicResult = topicClient->DescribeTopic(fullTopicName, {}).GetValueSync();
    if (describeTopicResult.GetTopicDescription().GetTotalPartitionsCount() != 0) {
        Cout << "Topic " << TopicName << " already exists.\n";
        return EXIT_FAILURE;
    }

    NYdb::NTopic::TCreateTopicSettings settings;
    settings.PartitioningSettings(PartitionCount, PartitionCount);

    for (ui32 consumerIdx = 0; consumerIdx < ConsumerCount; ++consumerIdx) {
        settings.BeginAddConsumer(TCommandWorkloadTopicDescribe::GenerateConsumerName(consumerIdx))
            .EndAddConsumer();
    }

    auto result = topicClient->CreateTopic(fullTopicName, settings).GetValueSync();
    ThrowOnError(result);

    return EXIT_SUCCESS;
}
