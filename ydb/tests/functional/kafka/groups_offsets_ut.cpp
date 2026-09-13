#include "helpers.h"

using namespace NKafkaRdkafkaTests;

Y_UNIT_TEST_SUITE(KafkaLibrdkafkaGroupsAndOffsets) {
    Y_UNIT_TEST(WatermarkOffsets) {
        const TString topic = UniqueName("rdk-offsets-wm");
        CreateYdbTopic(topic, 1);
        auto producer = MakeProducer();
        WaitTopicPartitions(*producer->Handle, topic, 1);
        ProduceAndFlush(*producer, topic, {"one", "two"});

        int64_t low = 0;
        int64_t high = 0;
        AssertRdKafkaOk(
            producer->Handle->query_watermark_offsets(std::string(topic), 0, &low, &high, 15000),
            "query_watermark_offsets");
        UNIT_ASSERT_VALUES_EQUAL(low, 0);
        UNIT_ASSERT_VALUES_EQUAL(high, 2);
    }

    Y_UNIT_TEST(CommitAndFetchOffsets) {
        const TString topic = UniqueName("rdk-offsets-commit");
        CreateYdbTopic(topic, 1);
        auto producer = MakeProducer();
        WaitTopicPartitions(*producer->Handle, topic, 1);
        ProduceAndFlush(*producer, topic, {"first", "second"});

        const TString group = UniqueName("offset-group");
        auto consumer = MakeConsumer(group);
        AssertRdKafkaOk(consumer->Handle->subscribe({std::string(topic)}), "subscribe");
        ConsumePayloads(*consumer->Handle, 1);
        AssertRdKafkaOk(consumer->Handle->commitSync(), "commitSync");

        std::vector<RdKafka::TopicPartition*> committed;
        committed.push_back(RdKafka::TopicPartition::create(std::string(topic), 0));
        AssertRdKafkaOk(consumer->Handle->committed(committed, 15000), "committed");
        UNIT_ASSERT_VALUES_EQUAL(committed.size(), 1);
        UNIT_ASSERT_GT(committed[0]->offset(), 0);
        RdKafka::TopicPartition::destroy(committed);
    }

    Y_UNIT_TEST(ListAndDescribeGroups) {
        const TString topic = UniqueName("rdk-groups");
        CreateYdbTopic(topic, 1);
        auto producer = MakeProducer();
        WaitTopicPartitions(*producer->Handle, topic, 1);
        ProduceAndFlush(*producer, topic, {"g0"});

        const TString group = UniqueName("listed-group");
        auto consumer = MakeConsumer(group);
        AssertRdKafkaOk(consumer->Handle->subscribe({std::string(topic)}), "subscribe");
        WaitAssignment(*consumer->Handle, 1);

        const TInstant deadline = TInstant::Now() + TDuration::Seconds(30);
        bool found = false;
        while (TInstant::Now() < deadline) {
            const auto groups = ListConsumerGroups(*producer->Handle);
            for (const auto& name : groups) {
                if (name == group) {
                    found = true;
                    break;
                }
            }
            if (found) {
                break;
            }
            delete consumer->Handle->consume(500);
        }
        UNIT_ASSERT_C(found, "consumer group was not listed");
        DescribeConsumerGroup(*producer->Handle, group);
    }

    Y_UNIT_TEST(SaslPlainProduceConsume) {
        const TString topic = UniqueName("rdk-sasl");
        CreateYdbTopic(topic, 1);

        auto producer = MakeSaslProducer();
        WaitTopicPartitions(*producer->Handle, topic, 1);
        ProduceAndFlush(*producer, topic, {"sasl-payload"}, {"sasl-key"});

        auto consumer = MakeSaslConsumer(UniqueName("sasl-group"));
        AssertRdKafkaOk(consumer->Handle->subscribe({std::string(topic)}), "subscribe");
        const auto payloads = ConsumePayloads(*consumer->Handle, 1);
        UNIT_ASSERT_VALUES_EQUAL(payloads[0], "sasl-payload");
    }
}
