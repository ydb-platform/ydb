#include "setup.h"

namespace NYdb::inline Dev::NTopic::NTests {


void ITopicTestSetup::CreateTopic(const std::string& name,
                                  const std::string& consumer,
                                  size_t partitionCount,
                                  std::optional<size_t> maxPartitionCount,
                                  const TDuration retention,
                                  bool important) {
    TTopicClient client(MakeDriver());

    TCreateTopicSettings topics;
    topics
        .RetentionPeriod(retention)
        .BeginConfigurePartitioningSettings()
        .MinActivePartitions(partitionCount)
        .MaxActivePartitions(maxPartitionCount.value_or(partitionCount));

    if (maxPartitionCount.has_value() && maxPartitionCount.value() > partitionCount) {
        topics
            .BeginConfigurePartitioningSettings()
            .BeginConfigureAutoPartitioningSettings()
            .Strategy(EAutoPartitioningStrategy::ScaleUp);
    }

    TConsumerSettings<TCreateTopicSettings> consumers(topics, GetConsumerName(consumer));
    consumers.Important(important);
    topics.AppendConsumers(consumers);

    auto status = client.CreateTopic(GetTopicPath(name), topics).GetValueSync();
    Y_ENSURE_BT(status.IsSuccess(), status);
}

TTopicDescription ITopicTestSetup::DescribeTopic(const std::string& name) {
    TTopicClient client(MakeDriver());

    TDescribeTopicSettings settings;
    settings.IncludeStats(true);
    settings.IncludeLocation(true);

    auto status = client.DescribeTopic(GetTopicPath(name), settings).GetValueSync();
    Y_ENSURE_BT(status.IsSuccess(), status);

    return status.GetTopicDescription();
}

}
