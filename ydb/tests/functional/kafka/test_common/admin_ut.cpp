#include "helpers.h"

using namespace NKafkaRdkafkaTests;

Y_UNIT_TEST_SUITE(KafkaLibrdkafkaAdmin) {
    Y_UNIT_TEST(CreateTopics) {
        const TString topic = UniqueName("rdk-admin-create");
        auto producer = MakeProducer();
        CreateKafkaTopic(*producer->Handle, topic, 2);
        ProduceAndFlush(*producer, topic, {"created-via-kafka"});
    }

    Y_UNIT_TEST(CreateTopicsValidateOnly) {
        const TString topic = UniqueName("rdk-admin-validate");
        auto producer = MakeProducer();
        const auto err = CreateKafkaTopicEx(*producer->Handle, topic, {
            .Partitions = 1,
            .ValidateOnly = true,
            .WaitReady = false,
        });
        AssertCOk(err, "validate.only CreateTopics");
        UNIT_ASSERT_C(!TopicVisible(*producer->Handle, topic), "validate.only must not create the topic");
    }

    Y_UNIT_TEST(CreateTopicsDuplicate) {
        const TString topic = UniqueName("rdk-admin-dup");
        auto producer = MakeProducer();
        CreateKafkaTopic(*producer->Handle, topic, 1);
        const auto err = CreateKafkaTopicEx(*producer->Handle, topic, {
            .Partitions = 1,
            .WaitReady = false,
        });
        UNIT_ASSERT_C(
            err == RD_KAFKA_RESP_ERR_TOPIC_ALREADY_EXISTS,
            rd_kafka_err2str(err));
    }

    Y_UNIT_TEST(CreateTopicsWithConfigs) {
        const TString topic = UniqueName("rdk-admin-cfg");
        auto producer = MakeProducer();
        AssertCOk(CreateKafkaTopicEx(*producer->Handle, topic, {
            .Partitions = 1,
            .Configs = {
                {"cleanup.policy", "compact"},
                {"retention.ms", "3600000"},
                {"message.timestamp.type", "CreateTime"},
            },
        }), "CreateTopics with configs");

        auto configs = DescribeTopicConfigs(*producer->Handle, topic);
        UNIT_ASSERT_VALUES_EQUAL(configs["cleanup.policy"], "compact");
        UNIT_ASSERT_VALUES_EQUAL(configs["retention.ms"], "3600000");
        UNIT_ASSERT_VALUES_EQUAL(configs["message.timestamp.type"], "CreateTime");
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

    Y_UNIT_TEST(CreatePartitionsMissingTopic) {
        auto producer = MakeProducer();
        const auto err = CreateKafkaPartitionsResult(*producer->Handle, UniqueName("rdk-admin-missing"), 2);
        UNIT_ASSERT_C(err != RD_KAFKA_RESP_ERR_NO_ERROR, rd_kafka_err2str(err));
    }

    Y_UNIT_TEST(DescribeConfigs) {
        const TString topic = UniqueName("rdk-admin-configs");
        CreateYdbTopic(topic, 1);
        auto producer = MakeProducer();
        WaitTopicPartitions(*producer->Handle, topic, 1);

        auto configs = DescribeTopicConfigs(*producer->Handle, topic);
        UNIT_ASSERT(configs.contains("retention.ms"));
        UNIT_ASSERT(!configs["retention.ms"].empty());
        UNIT_ASSERT(configs.contains("cleanup.policy"));
        UNIT_ASSERT(configs.contains("message.timestamp.type"));
        UNIT_ASSERT(!configs["message.timestamp.type"].empty());
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

    Y_UNIT_TEST(MetadataMissingTopic) {
        auto producer = MakeProducer();
        std::string errstr;
        std::unique_ptr<RdKafka::Topic> topic(RdKafka::Topic::create(
            producer->Handle.get(),
            std::string(UniqueName("rdk-admin-no-topic")),
            nullptr,
            errstr));
        UNIT_ASSERT_C(topic, errstr);
        RdKafka::Metadata* metadata = nullptr;
        AssertRdKafkaOk(producer->Handle->metadata(false, topic.get(), &metadata, 10000), "metadata missing");
        std::unique_ptr<RdKafka::Metadata> holder(metadata);
        UNIT_ASSERT(metadata);
        UNIT_ASSERT_VALUES_EQUAL(metadata->topics()->size(), 1);
        UNIT_ASSERT_C(
            (*metadata->topics())[0]->err() == RdKafka::ERR_UNKNOWN_TOPIC_OR_PART
                || (*metadata->topics())[0]->err() == RdKafka::ERR_UNKNOWN_TOPIC_ID
                || (*metadata->topics())[0]->err() != RdKafka::ERR_NO_ERROR,
            RdKafka::err2str((*metadata->topics())[0]->err()));
    }

    Y_UNIT_TEST(DeleteTopicsNotSupported) {
        const TString topic = UniqueName("rdk-admin-del");
        CreateYdbTopic(topic, 1);
        auto producer = MakeProducer();
        WaitTopicPartitions(*producer->Handle, topic, 1);
        const auto err = DeleteKafkaTopic(*producer->Handle, topic);
        UNIT_ASSERT_C(err != RD_KAFKA_RESP_ERR_NO_ERROR, "DeleteTopics is not supported");
        UNIT_ASSERT(TopicVisible(*producer->Handle, topic));
    }

    Y_UNIT_TEST(AlterConfigsKnownParseFailure) {
        const TString topic = UniqueName("rdk-admin-alter");
        CreateYdbTopic(topic, 1);
        auto producer = MakeProducer();
        WaitTopicPartitions(*producer->Handle, topic, 1);
        const TString error = AlterTopicConfigs(*producer->Handle, topic, {{"retention.ms", "3600000"}});
        UNIT_ASSERT_C(error, "AlterConfigs is expected to fail with librdkafka");
        UNIT_ASSERT_C(
            error.Contains("Bad message") || error.Contains("parse") || error.Contains("Local:"),
            error);
    }
}
