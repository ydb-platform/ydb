#include "common.h"

#include <ydb/core/local_proxy/local_pq_client/local_topic_client.h>

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
