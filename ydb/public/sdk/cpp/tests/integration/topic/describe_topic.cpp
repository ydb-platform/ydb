#include "setup/fixture.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <thread>

namespace NYdb::inline Dev::NTopic::NTests {

class Describe : public TTopicTestFixture {
protected:
    void DescribeTopic(TTopicClient& client, bool requireStats, bool requireNonEmptyStats, bool requireLocation) {
        TDescribeTopicSettings settings;
        settings.IncludeStats(requireStats);
        settings.IncludeLocation(requireLocation);

        {
            auto result = client.DescribeTopic(GetTopicPath(), settings).GetValueSync();
            EXPECT_TRUE(result.IsSuccess());

            const auto& description = result.GetTopicDescription();

            const auto& partitions = description.GetPartitions();
            EXPECT_EQ(partitions.size(), 1u);

            const auto& partition = partitions[0];
            EXPECT_TRUE(partition.GetActive());
            EXPECT_EQ(partition.GetPartitionId(), 0u);

            if (requireStats) {
                const auto& stats = description.GetTopicStats();

                if (requireNonEmptyStats) {
                    EXPECT_GT(stats.GetStoreSizeBytes(), 0u);
                    EXPECT_GT(stats.GetBytesWrittenPerMinute(), 0u);
                    EXPECT_GT(stats.GetBytesWrittenPerHour(), 0u);
                    EXPECT_GT(stats.GetBytesWrittenPerDay(), 0u);
                    EXPECT_GT(stats.GetMaxWriteTimeLag(), TDuration::Zero());
                    EXPECT_GT(stats.GetMinLastWriteTime(), TInstant::Zero());
                } else {
                    EXPECT_EQ(stats.GetStoreSizeBytes(), 0u);
                }
            }

            if (requireLocation) {
                EXPECT_TRUE(partition.GetPartitionLocation());
                const auto& partitionLocation = *partition.GetPartitionLocation();
                EXPECT_GT(partitionLocation.GetNodeId(), 0);
                EXPECT_GE(partitionLocation.GetGeneration(), 0); // greater-or-equal 0
            }
        }
    }

    void DescribeConsumer(TTopicClient& client, bool requireStats, bool requireNonEmptyStats, bool requireLocation){
        TDescribeConsumerSettings settings;
        settings.IncludeStats(requireStats);
        settings.IncludeLocation(requireLocation);

        {
            auto result = client.DescribeConsumer(GetTopicPath(), GetConsumerName(), settings).GetValueSync();
            EXPECT_TRUE(result.IsSuccess()) << result.GetIssues().ToString();

            const auto& description = result.GetConsumerDescription();

            const auto& partitions = description.GetPartitions();
            EXPECT_EQ(partitions.size(), 1u);

            const auto& partition = partitions[0];
            EXPECT_TRUE(partition.GetActive());
            EXPECT_EQ(partition.GetPartitionId(), 0u);

            if (requireStats) {
                const auto& stats = partition.GetPartitionStats();
                const auto& consumerStats = partition.GetPartitionConsumerStats();
                EXPECT_TRUE(stats);
                EXPECT_TRUE(consumerStats);

                if (requireNonEmptyStats) {
                    EXPECT_GE(stats->GetStartOffset(), 0u);
                    EXPECT_GE(stats->GetEndOffset(), 0u);
                    EXPECT_GT(stats->GetStoreSizeBytes(), 0u);
                    EXPECT_GT(stats->GetLastWriteTime(), TInstant::Zero());
                    EXPECT_GT(stats->GetMaxWriteTimeLag(), TDuration::Zero());
                    EXPECT_GT(stats->GetBytesWrittenPerMinute(), 0u);
                    EXPECT_GT(stats->GetBytesWrittenPerHour(), 0u);
                    EXPECT_GT(stats->GetBytesWrittenPerDay(), 0u);

                    EXPECT_GT(consumerStats->GetLastReadOffset(), 0u);
                    EXPECT_GT(consumerStats->GetCommittedOffset(), 0u);
                    EXPECT_GE(consumerStats->GetReadSessionId().size(), 0u);
                    EXPECT_EQ(consumerStats->GetReaderName(), "");
                    EXPECT_GE(consumerStats->GetMaxWriteTimeLag(), TDuration::Seconds(100));
                } else {
                    EXPECT_EQ(stats->GetStartOffset(), 0u);
                    EXPECT_EQ(consumerStats->GetLastReadOffset(), 0u);
                }
            }

            if (requireLocation) {
                EXPECT_TRUE(partition.GetPartitionLocation());
                const auto& partitionLocation = *partition.GetPartitionLocation();
                EXPECT_GT(partitionLocation.GetNodeId(), 0);
                EXPECT_GE(partitionLocation.GetGeneration(), 0); // greater-or-equal 0
            }
        }
    }

    void DescribePartition(TTopicClient& client, bool requireStats, bool requireNonEmptyStats, bool requireLocation) {
        TDescribePartitionSettings settings;
        settings.IncludeStats(requireStats);
        settings.IncludeLocation(requireLocation);

        std::uint64_t testPartitionId = 0;

        {
            auto result = client.DescribePartition(GetTopicPath(), testPartitionId, settings).GetValueSync();
            EXPECT_TRUE(result.IsSuccess()) << result.GetIssues().ToString();

            const auto& description = result.GetPartitionDescription();

            const auto& partition = description.GetPartition();
            EXPECT_TRUE(partition.GetActive());
            EXPECT_EQ(partition.GetPartitionId(), testPartitionId);

            if (requireStats) {
                const auto& stats = partition.GetPartitionStats();
                EXPECT_TRUE(stats);

                if (requireNonEmptyStats) {
                    EXPECT_GE(stats->GetStartOffset(), 0u);
                    EXPECT_GE(stats->GetEndOffset(), 0u);
                    EXPECT_GT(stats->GetStoreSizeBytes(), 0u);
                    EXPECT_GT(stats->GetLastWriteTime(), TInstant::Zero());
                    EXPECT_GT(stats->GetMaxWriteTimeLag(), TDuration::Zero());
                    EXPECT_GT(stats->GetBytesWrittenPerMinute(), 0u);
                    EXPECT_GT(stats->GetBytesWrittenPerHour(), 0u);
                    EXPECT_GT(stats->GetBytesWrittenPerDay(), 0u);
                } else {
                    EXPECT_EQ(stats->GetStoreSizeBytes(), 0u);
                }
            }

            if (requireLocation) {
                EXPECT_TRUE(partition.GetPartitionLocation());
                const auto& partitionLocation = *partition.GetPartitionLocation();
                EXPECT_GT(partitionLocation.GetNodeId(), 0);
                EXPECT_GE(partitionLocation.GetGeneration(), 0); // greater-or-equal 0
            }
        }
    }
};

TEST_F(Describe, TEST_NAME(Basic)) {
    TTopicClient client(MakeDriver());

    DescribeTopic(client, false, false, false);
    DescribeConsumer(client, false, false, false);
    DescribePartition(client, false, false, false);
}

TEST_F(Describe, TEST_NAME(Statistics)) {
    // TODO(abcdef): temporarily deleted
    GTEST_SKIP() << "temporarily deleted";

    TTopicClient client(MakeDriver());

    // Get empty description
    DescribeTopic(client, true, false, false);
    DescribeConsumer(client, true, false, false);
    DescribePartition(client, true, false, false);

    const size_t messagesCount = 1;

    // Write a message
    {
        auto writeSettings = TWriteSessionSettings().Path(GetTopicPath()).MessageGroupId("test-message_group_id").Codec(ECodec::RAW);
        auto writeSession = client.CreateSimpleBlockingWriteSession(writeSettings);
        std::string message(32_MB, 'x');

        for (size_t i = 0; i < messagesCount; ++i) {
            EXPECT_TRUE(writeSession->Write(message, {}, TInstant::Now() - TDuration::Seconds(100)));
        }
        writeSession->Close();
    }

    // Read a message
    {
        auto readSettings = TReadSessionSettings().ConsumerName(GetConsumerName()).AppendTopics(GetTopicPath());
        auto readSession = client.CreateReadSession(readSettings);

        // Event 1: start partition session
        {
            std::optional<TReadSessionEvent::TEvent> event = readSession->GetEvent(true);
            EXPECT_TRUE(event);
            auto startPartitionSession = std::get_if<TReadSessionEvent::TStartPartitionSessionEvent>(&event.value());
            EXPECT_TRUE(startPartitionSession) << DebugString(*event);

            startPartitionSession->Confirm();
        }

        // Event 2: data received
        {
            std::optional<TReadSessionEvent::TEvent> event = readSession->GetEvent(true);
            EXPECT_TRUE(event);
            auto dataReceived = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&event.value());
            EXPECT_TRUE(dataReceived) << DebugString(*event);

            dataReceived->Commit();
        }

        // Event 3: commit acknowledgement
        {
            std::optional<TReadSessionEvent::TEvent> event = readSession->GetEvent(true);
            EXPECT_TRUE(event);
            auto commitOffsetAck = std::get_if<TReadSessionEvent::TCommitOffsetAcknowledgementEvent>(&event.value());

            EXPECT_TRUE(commitOffsetAck) << DebugString(*event);

            EXPECT_EQ(commitOffsetAck->GetCommittedOffset(), messagesCount);
        }
    }

    // Additional write
    {
        auto writeSettings = TWriteSessionSettings().Path(GetTopicPath()).MessageGroupId("test-message_group_id").Codec(ECodec::RAW);
        auto writeSession = client.CreateSimpleBlockingWriteSession(writeSettings);
        std::string message(32, 'x');

        for(size_t i = 0; i < messagesCount; ++i) {
            EXPECT_TRUE(writeSession->Write(message));
        }
        writeSession->Close();
    }
    std::this_thread::sleep_for(std::chrono::seconds(3));

    // Get non-empty description

    DescribeTopic(client, true, true, false);
    DescribeConsumer(client, true, true, false);
    DescribePartition(client, true, true, false);
}

TEST_F(Describe, TEST_NAME(Location)) {
    TTopicClient client(MakeDriver());

    DescribeTopic(client, false, false, true);
    DescribeConsumer(client, false, false, true);
    DescribePartition(client, false, false, true);
}

}
