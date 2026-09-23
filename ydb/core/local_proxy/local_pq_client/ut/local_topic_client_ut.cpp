#include "common.h"

#include <ydb/core/local_proxy/local_pq_client/local_topic_client.h>
#include <ydb/library/testlib/helpers.h>

namespace NKikimr::NKqp::NLocalTopicTests {

Y_UNIT_TEST_SUITE(TLocalTopicClient) {
    Y_UNIT_TEST_F(DescribeTopic, TLocalTopicClientFixture) {
        auto client = CreateLocalTopicClient(LocalClientSettings(), ClientSettings());
        const auto result = client->DescribeTopic(TOPIC_PATH).GetValue(TEST_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        const auto& topic = result.GetTopicDescription();
        UNIT_ASSERT_VALUES_EQUAL(topic.GetTotalPartitionsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(topic.GetConsumers().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(topic.GetConsumers()[0].GetConsumerName(), CONSUMER);
        UNIT_ASSERT(!topic.GetPartitions()[0].GetPartitionStats());
        UNIT_ASSERT(!topic.GetPartitions()[0].GetPartitionLocation());
    }

    Y_UNIT_TEST_F(DescribeRelativePathWithStatsAndLocation, TLocalTopicClientFixture) {
        WriteTopicMessages({"one", "two"});
        auto client = CreateLocalTopicClient(LocalClientSettings(), ClientSettings());
        const auto result = client->DescribeTopic("topic", TDescribeTopicSettings()
            .IncludeStats(true).IncludeLocation(true)).GetValue(TEST_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        const auto& partitions = result.GetTopicDescription().GetPartitions();
        UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(partitions[0].GetPartitionId(), 0);
        UNIT_ASSERT(partitions[0].GetPartitionStats());
        UNIT_ASSERT_VALUES_EQUAL(partitions[0].GetPartitionStats()->GetStartOffset(), 0);
        UNIT_ASSERT_VALUES_EQUAL(partitions[0].GetPartitionStats()->GetEndOffset(), 2);
        UNIT_ASSERT(partitions[0].GetPartitionLocation());
    }

    Y_UNIT_TEST_F(DescribeMissingTopic, TLocalTopicClientFixture) {
        auto client = CreateLocalTopicClient(LocalClientSettings(), ClientSettings());
        const auto result = client->DescribeTopic("missing-topic").GetValue(TEST_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
        UNIT_ASSERT(!result.GetIssues().Empty());
    }

    Y_UNIT_TEST_F(DescribeConsumer, TLocalTopicClientFixture) {
        auto client = CreateLocalTopicClient(LocalClientSettings(), ClientSettings());
        const auto result = client->DescribeConsumer(TOPIC_PATH, CONSUMER).GetValue(TEST_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        const auto& description = result.GetConsumerDescription();
        UNIT_ASSERT_VALUES_EQUAL(description.GetConsumer().GetConsumerName(), CONSUMER);
        const auto& partitions = description.GetPartitions();
        UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(partitions[0].GetPartitionId(), 0);
        UNIT_ASSERT(partitions[0].GetActive());
        UNIT_ASSERT(!partitions[0].GetPartitionStats());
        UNIT_ASSERT(!partitions[0].GetPartitionConsumerStats());
        UNIT_ASSERT(!partitions[0].GetPartitionLocation());
    }

    Y_UNIT_TEST_QUAD_F(DescribeConsumerRelativePath, Stats, Location, TLocalTopicClientFixture) {
        WriteTopicMessages({"committed", "remaining-1", "remaining-2"});
        const auto committed = TopicClient->CommitOffset(TOPIC_PATH, 0, CONSUMER, 1).GetValue(TEST_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL_C(committed.GetStatus(), EStatus::SUCCESS, committed.GetIssues().ToString());

        auto client = CreateLocalTopicClient(LocalClientSettings(), ClientSettings());
        const auto result = client->DescribeConsumer("topic", CONSUMER, TDescribeConsumerSettings()
            .IncludeStats(Stats).IncludeLocation(Location)).GetValue(TEST_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        const auto& description = result.GetConsumerDescription();
        UNIT_ASSERT_VALUES_EQUAL(description.GetConsumer().GetConsumerName(), CONSUMER);
        const auto& partitions = description.GetPartitions();
        UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(partitions[0].GetPartitionId(), 0);
        UNIT_ASSERT_VALUES_EQUAL(partitions[0].GetPartitionStats().has_value(), Stats);
        UNIT_ASSERT_VALUES_EQUAL(partitions[0].GetPartitionConsumerStats().has_value(), Stats);
        UNIT_ASSERT_VALUES_EQUAL(partitions[0].GetPartitionLocation().has_value(), Location);
        if (Stats) {
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].GetPartitionStats()->GetStartOffset(), 0);
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].GetPartitionStats()->GetEndOffset(), 3);
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].GetPartitionConsumerStats()->GetCommittedOffset(), 1);
        }
        if (Location) {
            UNIT_ASSERT_GT(partitions[0].GetPartitionLocation()->GetNodeId(), 0);
            UNIT_ASSERT_GT(partitions[0].GetPartitionLocation()->GetGeneration(), 0);
        }
    }

    Y_UNIT_TEST_TWIN_F(DescribeConsumerMissingResource, MissingTopic, TLocalTopicClientFixture) {
        auto client = CreateLocalTopicClient(LocalClientSettings(), ClientSettings());
        const auto result = client->DescribeConsumer(
            MissingTopic ? "missing-topic" : TOPIC_PATH,
            MissingTopic ? CONSUMER : "missing-consumer").GetValue(TEST_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
        UNIT_ASSERT(!result.GetIssues().Empty());
    }

    Y_UNIT_TEST_TWIN_F(CommitOffsetAndResumeConsumer, RelativePath, TLocalTopicClientFixture) {
        constexpr char topic[] = "/Root/partitioned-topic";
        CreateTopic(topic, 2);
        WriteTopicMessages({"partition-0"}, WriteSettings(topic).MessageGroupId("").PartitionId(0));
        WriteTopicMessages({"committed", "remaining"}, WriteSettings(topic).MessageGroupId("").PartitionId(1));

        auto client = CreateLocalTopicClient(LocalClientSettings(), ClientSettings());
        const auto result = client->CommitOffset(RelativePath ? "partitioned-topic" : topic, 1, CONSUMER, 1)
            .GetValue(TEST_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        // Check persistence and partition selection through the regular SDK.
        const auto described = TopicClient->DescribeConsumer(topic, CONSUMER, TDescribeConsumerSettings()
            .IncludeStats(true)).GetValue(TEST_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL_C(described.GetStatus(), EStatus::SUCCESS, described.GetIssues().ToString());
        const auto& partitions = described.GetConsumerDescription().GetPartitions();
        UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 2);
        for (const auto& partition : partitions) {
            UNIT_ASSERT(partition.GetPartitionConsumerStats());
            UNIT_ASSERT_VALUES_EQUAL(partition.GetPartitionConsumerStats()->GetCommittedOffset(),
                partition.GetPartitionId() == 1 ? 1 : 0);
        }

        auto settings = ReadSettings(topic);
        settings.Topics_[0].AppendPartitionIds(1);
        auto session = TopicClient->CreateReadSession(settings);
        const auto messages = ReadMessages(*session, 1);
        UNIT_ASSERT_VALUES_EQUAL(messages[0].GetOffset(), 1);
        UNIT_ASSERT_VALUES_EQUAL(messages[0].GetData(), "remaining");
        UNIT_ASSERT_VALUES_EQUAL(messages[0].GetPartitionSession()->GetPartitionId(), 1);
        UNIT_ASSERT(session->Close(TEST_TIMEOUT));
    }

    Y_UNIT_TEST_F(CommitOffsetWithReadSessionId, TLocalTopicClientFixture) {
        WriteTopicMessages({"first", "second"});
        auto session = TopicClient->CreateReadSession(ReadSettings());
        const auto messages = ReadMessages(*session, 2);
        const auto sessionId = messages[0].GetPartitionSession()->GetReadSessionId();
        UNIT_ASSERT(!sessionId.empty());
        auto client = CreateLocalTopicClient(LocalClientSettings(), ClientSettings());

        const auto committed = client->CommitOffset(TOPIC_PATH, 0, CONSUMER, 1,
            TCommitOffsetSettings().ReadSessionId(sessionId)).GetValue(TEST_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL_C(committed.GetStatus(), EStatus::SUCCESS, committed.GetIssues().ToString());
        const auto rejected = client->CommitOffset(TOPIC_PATH, 0, CONSUMER, 2,
            TCommitOffsetSettings().ReadSessionId("invalid-session")).GetValue(TEST_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL(rejected.GetStatus(), EStatus::SESSION_EXPIRED);
        UNIT_ASSERT(!rejected.GetIssues().Empty());

        const auto described = TopicClient->DescribeConsumer(TOPIC_PATH, CONSUMER, TDescribeConsumerSettings()
            .IncludeStats(true)).GetValue(TEST_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL_C(described.GetStatus(), EStatus::SUCCESS, described.GetIssues().ToString());
        const auto& partitions = described.GetConsumerDescription().GetPartitions();
        UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 1);
        UNIT_ASSERT(partitions[0].GetPartitionConsumerStats());
        UNIT_ASSERT_VALUES_EQUAL(partitions[0].GetPartitionConsumerStats()->GetCommittedOffset(), 1);
        UNIT_ASSERT_VALUES_EQUAL(partitions[0].GetPartitionConsumerStats()->GetReadSessionId(), sessionId);

        // Passing the owning session id must preserve the active streaming reader.
        // The stream tracks its own commit ranges, so acknowledge both delivered messages.
        TDeferredCommit deferred;
        for (const auto& message : messages) {
            deferred.Add(message);
        }
        deferred.Commit();
        const auto ack = WaitForReadEvent<TReadSessionEvent::TCommitOffsetAcknowledgementEvent>(*session);
        UNIT_ASSERT_VALUES_EQUAL(ack.GetCommittedOffset(), 2);
        UNIT_ASSERT(session->Close(TEST_TIMEOUT));
    }

    Y_UNIT_TEST_F(CommitOffsetErrors, TLocalTopicClientFixture) {
        WriteTopicMessages({"uncommitted"});
        auto client = CreateLocalTopicClient(LocalClientSettings(), ClientSettings());
        const struct {
            const char* Path;
            ui64 Partition;
            const char* Consumer;
            ui64 Offset;
            EStatus Status;
        } cases[] = {
            {"missing-topic", 0, CONSUMER, 1, EStatus::SCHEME_ERROR},
            {TOPIC_PATH, 0, "missing-consumer", 1, EStatus::BAD_REQUEST},
            {TOPIC_PATH, 1, CONSUMER, 1, EStatus::SCHEME_ERROR},
            {TOPIC_PATH, 0, CONSUMER, 2, EStatus::BAD_REQUEST},
        };
        for (const auto& test : cases) {
            const auto result = client->CommitOffset(test.Path, test.Partition, test.Consumer, test.Offset)
                .GetValue(TEST_TIMEOUT);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), test.Status, result.GetIssues().ToString());
            UNIT_ASSERT(!result.GetIssues().Empty());
        }
        AssertTopicMessages({"uncommitted"});
    }

    Y_UNIT_TEST_F(CreateReadAndWriteSessions, TLocalTopicClientFixture) {
        auto client = CreateLocalTopicClient(LocalClientSettings(), ClientSettings());
        auto writeSession = client->CreateWriteSession(WriteSettings("topic"));
        TTestWriter writer(writeSession);
        AssertAck(writer.Write(TWriteMessage("client message")), 1, 0);
        CloseSession(*writeSession);

        auto readSession = client->CreateReadSession(ReadSettings("topic"));
        const auto messages = ReadMessages(*readSession, 1);
        UNIT_ASSERT_VALUES_EQUAL(messages[0].GetData(), "client message");
        UNIT_ASSERT_VALUES_EQUAL(messages[0].GetOffset(), 0);
        CloseSession(*readSession);
        AssertTopicMessages({"client message"});
    }
}

} // namespace NKikimr::NKqp::NLocalTopicTests
