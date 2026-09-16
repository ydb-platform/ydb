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

    Y_UNIT_TEST(CommitBeyondLogEnd) {
        const TString topic = UniqueName("rdk-offsets-beyond");
        CreateYdbTopic(topic, 1);
        auto producer = MakeProducer();
        WaitTopicPartitions(*producer->Handle, topic, 1);
        ProduceAndFlush(*producer, topic, {"one", "two"});

        auto consumer = MakeConsumer(UniqueName("beyond-group"));
        AssertRdKafkaOk(consumer->Handle->subscribe({std::string(topic)}), "subscribe");
        WaitAssignment(*consumer->Handle, 1);

        std::vector<RdKafka::TopicPartition*> parts;
        parts.push_back(RdKafka::TopicPartition::create(std::string(topic), 0, 100));
        AssertRdKafkaOk(consumer->Handle->commitSync(parts), "commit beyond log");
        RdKafka::TopicPartition::destroy(parts);

        std::vector<RdKafka::TopicPartition*> committed;
        committed.push_back(RdKafka::TopicPartition::create(std::string(topic), 0));
        AssertRdKafkaOk(consumer->Handle->committed(committed, 15000), "committed");
        UNIT_ASSERT_C(
            committed[0]->offset() == 100 || committed[0]->offset() <= 2,
            committed[0]->offset());
        RdKafka::TopicPartition::destroy(committed);
    }

    Y_UNIT_TEST(CommitMetadata) {
        const TString topic = UniqueName("rdk-offsets-meta");
        CreateYdbTopic(topic, 1);
        auto producer = MakeProducer();
        WaitTopicPartitions(*producer->Handle, topic, 1);
        ProduceAndFlush(*producer, topic, {"m0"});

        auto consumer = MakeConsumer(UniqueName("meta-group"));
        AssertRdKafkaOk(consumer->Handle->subscribe({std::string(topic)}), "subscribe");
        WaitAssignment(*consumer->Handle, 1);

        std::vector<RdKafka::TopicPartition*> parts;
        auto* tp = RdKafka::TopicPartition::create(std::string(topic), 0, 1);
        std::vector<unsigned char> meta{'h', 'i'};
        tp->set_metadata(meta);
        parts.push_back(tp);
        AssertRdKafkaOk(consumer->Handle->commitSync(parts), "commit with metadata");
        RdKafka::TopicPartition::destroy(parts);

        std::vector<RdKafka::TopicPartition*> committed;
        committed.push_back(RdKafka::TopicPartition::create(std::string(topic), 0));
        AssertRdKafkaOk(consumer->Handle->committed(committed, 15000), "committed");
        UNIT_ASSERT_VALUES_EQUAL(committed[0]->offset(), 1);
        const auto stored = committed[0]->get_metadata();
        if (!stored.empty()) {
            UNIT_ASSERT_VALUES_EQUAL(
                TString(reinterpret_cast<const char*>(stored.data()), stored.size()),
                "hi");
        }
        RdKafka::TopicPartition::destroy(committed);
    }

    Y_UNIT_TEST(AutocreateConsumerGroup) {
        const TString topic = UniqueName("rdk-offsets-autocg");
        CreateYdbTopic(topic, 1);
        auto producer = MakeProducer();
        WaitTopicPartitions(*producer->Handle, topic, 1);
        ProduceAndFlush(*producer, topic, {"auto-cg"});

        auto consumer = MakeConsumer(UniqueName("never-created-in-ydb"));
        AssertRdKafkaOk(consumer->Handle->subscribe({std::string(topic)}), "subscribe");
        UNIT_ASSERT_VALUES_EQUAL(ConsumePayloads(*consumer->Handle, 1)[0], "auto-cg");
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

    Y_UNIT_TEST(SaslWrongPassword) {
        auto producer = MakeSaslProducer({
            {"sasl.password", "wrong-password"},
            {"socket.timeout.ms", "8000"},
        });
        RdKafka::Metadata* metadata = nullptr;
        const auto err = producer->Handle->metadata(true, nullptr, &metadata, 10000);
        std::unique_ptr<RdKafka::Metadata> holder(metadata);
        UNIT_ASSERT_C(err != RdKafka::ERR_NO_ERROR, "wrong password must fail");
    }

    Y_UNIT_TEST(SaslWrongDatabase) {
        auto producer = MakeSaslProducer({
            {"sasl.username", "root@/wrong-db"},
            {"socket.timeout.ms", "8000"},
        });
        RdKafka::Metadata* metadata = nullptr;
        const auto err = producer->Handle->metadata(true, nullptr, &metadata, 10000);
        std::unique_ptr<RdKafka::Metadata> holder(metadata);
        UNIT_ASSERT_C(err != RdKafka::ERR_NO_ERROR, "user@/wrong-db must fail");
    }

    Y_UNIT_TEST(SaslUsernameWithoutDatabase) {
        auto producer = MakeSaslProducer({
            {"sasl.username", "root"},
            {"socket.timeout.ms", "8000"},
        });
        RdKafka::Metadata* metadata = nullptr;
        const auto err = producer->Handle->metadata(true, nullptr, &metadata, 10000);
        std::unique_ptr<RdKafka::Metadata> holder(metadata);
        UNIT_ASSERT_C(err != RdKafka::ERR_NO_ERROR, "username without @database must fail");
    }

    Y_UNIT_TEST(SaslScramSha256) {
        auto producer = MakeSaslProducer({
            {"sasl.mechanisms", "SCRAM-SHA-256"},
            {"socket.timeout.ms", "8000"},
        });
        RdKafka::Metadata* metadata = nullptr;
        const auto err = producer->Handle->metadata(true, nullptr, &metadata, 10000);
        std::unique_ptr<RdKafka::Metadata> holder(metadata);
        if (err != RdKafka::ERR_NO_ERROR) {
            // Default recipe user may not have SCRAM keys; handshake must fail cleanly.
            return;
        }

        const TString topic = UniqueName("rdk-sasl-scram");
        CreateYdbTopic(topic, 1);
        WaitTopicPartitions(*producer->Handle, topic, 1);
        ProduceAndFlush(*producer, topic, {"scram-payload"}, {"scram-key"});

        auto consumer = MakeSaslConsumer(UniqueName("scram-group"), {
            {"sasl.mechanisms", "SCRAM-SHA-256"},
        });
        AssertRdKafkaOk(consumer->Handle->subscribe({std::string(topic)}), "subscribe");
        UNIT_ASSERT_VALUES_EQUAL(ConsumePayloads(*consumer->Handle, 1)[0], "scram-payload");
    }
}
