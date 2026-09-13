#include "helpers.h"

using namespace NKafkaRdkafkaTests;

Y_UNIT_TEST_SUITE(KafkaLibrdkafkaConsume) {
    Y_UNIT_TEST(ReadWrittenMessages) {
        const TString topic = UniqueName("rdk-consume-read");
        CreateYdbTopic(topic, 1);

        auto producer = MakeProducer();
        WaitTopicPartitions(*producer->Handle, topic, 1);
        ProduceAndFlush(*producer, topic, {"alpha", "beta", "gamma"});

        auto consumer = MakeConsumer(UniqueName("group"));
        AssertRdKafkaOk(consumer->Handle->subscribe({std::string(topic)}), "subscribe");
        const auto payloads = ConsumePayloads(*consumer->Handle, 3);
        UNIT_ASSERT_VALUES_EQUAL(payloads[0], "alpha");
        UNIT_ASSERT_VALUES_EQUAL(payloads[1], "beta");
        UNIT_ASSERT_VALUES_EQUAL(payloads[2], "gamma");
    }

    Y_UNIT_TEST(EmptyTopic) {
        const TString topic = UniqueName("rdk-consume-empty");
        CreateYdbTopic(topic, 1);

        auto consumer = MakeConsumer(UniqueName("group"));
        std::vector<RdKafka::TopicPartition*> partitions;
        partitions.push_back(RdKafka::TopicPartition::create(
            std::string(topic),
            0,
            RdKafka::Topic::OFFSET_BEGINNING));
        AssertRdKafkaOk(consumer->Handle->assign(partitions), "assign");
        RdKafka::TopicPartition::destroy(partitions);

        ConsumeUntilEmpty(*consumer->Handle, TDuration::Seconds(2));
    }
}
