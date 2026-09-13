#include "helpers.h"

using namespace NKafkaRdkafkaTests;

Y_UNIT_TEST_SUITE(KafkaLibrdkafkaAdmin) {
    Y_UNIT_TEST(CreateTopics) {
        const TString topic = UniqueName("rdk-admin-create");
        auto producer = MakeProducer();
        CreateKafkaTopic(*producer->Handle, topic, 2);
        ProduceAndFlush(*producer, topic, {"created-via-kafka"});
    }

    Y_UNIT_TEST(CreatePartitions) {
        const TString topic = UniqueName("rdk-admin-parts");
        CreateYdbTopic(topic, 1);
        auto producer = MakeProducer();
        WaitTopicPartitions(*producer->Handle, topic, 1);
        CreateKafkaPartitions(*producer->Handle, topic, 3);
        Produce(
            *producer->Handle,
            topic,
            "p2",
            "k2",
            /*partition*/ 2);
        Flush(*producer);
    }

    Y_UNIT_TEST(DescribeConfigs) {
        const TString topic = UniqueName("rdk-admin-configs");
        CreateYdbTopic(topic, 1);
        auto producer = MakeProducer();
        WaitTopicPartitions(*producer->Handle, topic, 1);

        auto configs = DescribeTopicConfigs(*producer->Handle, topic);
        UNIT_ASSERT(configs.contains("retention.ms"));
        UNIT_ASSERT(!configs["retention.ms"].empty());
    }

    Y_UNIT_TEST(MetadataHasBrokerAndTopic) {
        const TString topic = UniqueName("rdk-admin-meta");
        CreateYdbTopic(topic, 1);
        auto producer = MakeProducer();
        WaitTopicPartitions(*producer->Handle, topic, 1);

        RdKafka::Metadata* metadata = nullptr;
        AssertRdKafkaOk(producer->Handle->metadata(true, nullptr, &metadata, 10000), "metadata");
        std::unique_ptr<RdKafka::Metadata> holder(metadata);
        UNIT_ASSERT(metadata);
        UNIT_ASSERT(!metadata->brokers()->empty());
        bool found = false;
        for (const auto* topicMeta : *metadata->topics()) {
            if (topicMeta->topic() == std::string(topic)) {
                found = true;
                AssertRdKafkaOk(topicMeta->err(), "topic metadata");
                UNIT_ASSERT_VALUES_EQUAL(topicMeta->partitions()->size(), 1);
            }
        }
        UNIT_ASSERT(found);
    }
}
