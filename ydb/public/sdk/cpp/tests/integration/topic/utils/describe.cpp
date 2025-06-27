#include "describe.h"


namespace NYdb::inline Dev::NTopic::NTests {

void DescribeTopicTest(ITopicTestSetup& setup, TTopicClient& client, bool requireStats, bool requireNonEmptyStats, bool requireLocation) {
    TDescribeTopicSettings settings;
    settings.IncludeStats(requireStats);
    settings.IncludeLocation(requireLocation);

    {
        auto result = client.DescribeTopic(setup.GetTopicPath(), settings).GetValueSync();
        Y_ENSURE(result.IsSuccess());

        const auto& description = result.GetTopicDescription();

        const auto& partitions = description.GetPartitions();
        Y_ENSURE(partitions.size() == 1);

        const auto& partition = partitions[0];
        Y_ENSURE(partition.GetActive());
        Y_ENSURE(partition.GetPartitionId() == 0);

        if (requireStats) {
            const auto& stats = description.GetTopicStats();

            if (requireNonEmptyStats) {
                Y_ENSURE(stats.GetStoreSizeBytes() > 0);
                Y_ENSURE(stats.GetBytesWrittenPerMinute() > 0);
                Y_ENSURE(stats.GetBytesWrittenPerHour() > 0);
                Y_ENSURE(stats.GetBytesWrittenPerDay() > 0);
                Y_ENSURE(stats.GetMaxWriteTimeLag() > TDuration::Zero());
                Y_ENSURE(stats.GetMinLastWriteTime() > TInstant::Zero());
            } else {
                Y_ENSURE(stats.GetStoreSizeBytes() == 0);
            }
        }

        if (requireLocation) {
            Y_ENSURE(partition.GetPartitionLocation());
            const auto& partitionLocation = *partition.GetPartitionLocation();
            Y_ENSURE(partitionLocation.GetNodeId() > 0);
            Y_ENSURE(partitionLocation.GetGeneration() >= 0); // greater-or-equal 0
        }
    }
}

void DescribeConsumerTest(ITopicTestSetup& setup, TTopicClient& client, bool requireStats, bool requireNonEmptyStats, bool requireLocation){
    TDescribeConsumerSettings settings;
    settings.IncludeStats(requireStats);
    settings.IncludeLocation(requireLocation);

    {
        auto result = client.DescribeConsumer(setup.GetTopicPath(), setup.GetConsumerName(), settings).GetValueSync();
        Y_ENSURE(result.IsSuccess(), result.GetIssues().ToString());

        const auto& description = result.GetConsumerDescription();

        const auto& partitions = description.GetPartitions();
        Y_ENSURE(partitions.size() == 1);

        const auto& partition = partitions[0];
        Y_ENSURE(partition.GetActive());
        Y_ENSURE(partition.GetPartitionId() == 0);

        if (requireStats) {
            const auto& stats = partition.GetPartitionStats();
            const auto& consumerStats = partition.GetPartitionConsumerStats();
            Y_ENSURE(stats);
            Y_ENSURE(consumerStats);

            if (requireNonEmptyStats) {
                Y_ENSURE(stats->GetStartOffset() >= 0);
                Y_ENSURE(stats->GetEndOffset() >= 0);
                Y_ENSURE(stats->GetStoreSizeBytes() > 0);
                Y_ENSURE(stats->GetLastWriteTime() > TInstant::Zero());
                Y_ENSURE(stats->GetMaxWriteTimeLag() > TDuration::Zero());
                Y_ENSURE(stats->GetBytesWrittenPerMinute() > 0);
                Y_ENSURE(stats->GetBytesWrittenPerHour() > 0);
                Y_ENSURE(stats->GetBytesWrittenPerDay() > 0);

                Y_ENSURE(consumerStats->GetLastReadOffset() > 0);
                Y_ENSURE(consumerStats->GetCommittedOffset() > 0);
                Y_ENSURE(consumerStats->GetReadSessionId().size() >= 0);
                Y_ENSURE(consumerStats->GetReaderName().empty());
                Y_ENSURE(consumerStats->GetMaxWriteTimeLag() >= TDuration::Seconds(100));
            } else {
                Y_ENSURE(stats->GetStartOffset() == 0);
                Y_ENSURE(consumerStats->GetLastReadOffset() == 0);
            }
        }

        if (requireLocation) {
            Y_ENSURE(partition.GetPartitionLocation());
            const auto& partitionLocation = *partition.GetPartitionLocation();
            Y_ENSURE(partitionLocation.GetNodeId() > 0);
            Y_ENSURE(partitionLocation.GetGeneration() >= 0); // greater-or-equal 0
        }
    }
}

void DescribePartitionTest(ITopicTestSetup& setup, TTopicClient& client, bool requireStats, bool requireNonEmptyStats, bool requireLocation) {
    TDescribePartitionSettings settings;
    settings.IncludeStats(requireStats);
    settings.IncludeLocation(requireLocation);

    std::uint64_t testPartitionId = 0;

    {
        auto result = client.DescribePartition(setup.GetTopicPath(), testPartitionId, settings).GetValueSync();
        Y_ENSURE(result.IsSuccess(), result.GetIssues().ToString());

        const auto& description = result.GetPartitionDescription();

        const auto& partition = description.GetPartition();
        Y_ENSURE(partition.GetActive());
        Y_ENSURE(partition.GetPartitionId() == testPartitionId);

        if (requireStats) {
            const auto& stats = partition.GetPartitionStats();
            Y_ENSURE(stats);

            if (requireNonEmptyStats) {
                Y_ENSURE(stats->GetStartOffset() >= 0);
                Y_ENSURE(stats->GetEndOffset() >= 0);
                Y_ENSURE(stats->GetStoreSizeBytes() > 0);
                Y_ENSURE(stats->GetLastWriteTime() > TInstant::Zero());
                Y_ENSURE(stats->GetMaxWriteTimeLag() > TDuration::Zero());
                Y_ENSURE(stats->GetBytesWrittenPerMinute() > 0);
                Y_ENSURE(stats->GetBytesWrittenPerHour() > 0);
                Y_ENSURE(stats->GetBytesWrittenPerDay() > 0);
            } else {
                Y_ENSURE(stats->GetStoreSizeBytes() == 0);
            }
        }

        if (requireLocation) {
            Y_ENSURE(partition.GetPartitionLocation());
            const auto& partitionLocation = *partition.GetPartitionLocation();
            Y_ENSURE(partitionLocation.GetNodeId() > 0);
            Y_ENSURE(partitionLocation.GetGeneration() >= 0); // greater-or-equal 0
        }
    }
}

}
