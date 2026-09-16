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

    Y_UNIT_TEST(SeekToMiddle) {
        const TString topic = UniqueName("rdk-consume-seek");
        CreateYdbTopic(topic, 1);
        auto producer = MakeProducer();
        WaitTopicPartitions(*producer->Handle, topic, 1);
        ProduceAndFlush(*producer, topic, {"a0", "a1", "a2", "a3", "a4"});

        auto consumer = MakeConsumer(UniqueName("group"));
        std::vector<RdKafka::TopicPartition*> partitions;
        partitions.push_back(RdKafka::TopicPartition::create(
            std::string(topic),
            0,
            RdKafka::Topic::OFFSET_BEGINNING));
        AssertRdKafkaOk(consumer->Handle->assign(partitions), "assign");
        RdKafka::TopicPartition::destroy(partitions);
        ConsumeMessages(*consumer->Handle, 1);

        std::unique_ptr<RdKafka::TopicPartition> seekTo(RdKafka::TopicPartition::create(std::string(topic), 0, 3));
        AssertRdKafkaOk(consumer->Handle->seek(*seekTo, 15000), "seek");
        const auto rest = ConsumeMessages(*consumer->Handle, 2);
        UNIT_ASSERT_VALUES_EQUAL(rest[0].Payload, "a3");
        UNIT_ASSERT_VALUES_EQUAL(rest[1].Payload, "a4");
        UNIT_ASSERT_VALUES_EQUAL(rest[0].Offset, 3);
    }

    Y_UNIT_TEST(AssignOffsetEnd) {
        const TString topic = UniqueName("rdk-consume-end");
        CreateYdbTopic(topic, 1);
        auto producer = MakeProducer();
        WaitTopicPartitions(*producer->Handle, topic, 1);
        ProduceAndFlush(*producer, topic, {"old-1", "old-2", "old-3"});

        auto consumer = MakeConsumer(UniqueName("group"));
        std::vector<RdKafka::TopicPartition*> partitions;
        partitions.push_back(RdKafka::TopicPartition::create(
            std::string(topic),
            0,
            RdKafka::Topic::OFFSET_END));
        AssertRdKafkaOk(consumer->Handle->assign(partitions), "assign");
        RdKafka::TopicPartition::destroy(partitions);
        ConsumeUntilEmpty(*consumer->Handle, TDuration::Seconds(2));

        ProduceAndFlush(*producer, topic, {"new-only"});
        const auto messages = ConsumeMessages(*consumer->Handle, 1);
        UNIT_ASSERT_VALUES_EQUAL(messages[0].Payload, "new-only");
    }

    Y_UNIT_TEST(AutoOffsetResetLatest) {
        const TString topic = UniqueName("rdk-consume-latest");
        CreateYdbTopic(topic, 1);
        auto producer = MakeProducer();
        WaitTopicPartitions(*producer->Handle, topic, 1);
        ProduceAndFlush(*producer, topic, {"old-a", "old-b", "old-c"});

        auto consumer = MakeConsumer(UniqueName("group"), {{"auto.offset.reset", "latest"}});
        AssertRdKafkaOk(consumer->Handle->subscribe({std::string(topic)}), "subscribe");
        const TInstant assignedDeadline = TInstant::Now() + TDuration::Seconds(30);
        while (TInstant::Now() < assignedDeadline && AssignmentPartitions(*consumer->Handle).empty()) {
            std::unique_ptr<RdKafka::Message> message(consumer->Handle->consume(200));
            UNIT_ASSERT(message);
            if (message->err() == RdKafka::ERR_NO_ERROR) {
                // auto.offset.reset=latest is not always honored; keep going after a fresh produce.
                break;
            }
        }

        ProduceAndFlush(*producer, topic, {"fresh"});
        const TInstant deadline = TInstant::Now() + TDuration::Seconds(30);
        bool gotFresh = false;
        while (TInstant::Now() < deadline) {
            std::unique_ptr<RdKafka::Message> message(consumer->Handle->consume(500));
            UNIT_ASSERT(message);
            if (message->err() == RdKafka::ERR_NO_ERROR && MessagePayload(*message) == "fresh") {
                gotFresh = true;
                break;
            }
            if (message->err() != RdKafka::ERR_NO_ERROR && message->err() != RdKafka::ERR__TIMED_OUT) {
                UNIT_FAIL("consume failed: " << RdKafka::err2str(message->err()));
            }
        }
        UNIT_ASSERT_C(gotFresh, "did not receive the message produced after joining with auto.offset.reset=latest");
    }

    Y_UNIT_TEST(FetchMaxBytes) {
        const TString topic = UniqueName("rdk-consume-fetchmax");
        CreateYdbTopic(topic, 1);
        auto producer = MakeProducer({{"linger.ms", "20"}});
        WaitTopicPartitions(*producer->Handle, topic, 1);
        TVector<TString> payloads;
        for (int i = 0; i < 8; ++i) {
            payloads.push_back(TStringBuilder() << "f" << i);
        }
        ProduceAndFlush(*producer, topic, payloads);

        auto consumer = MakeConsumer(UniqueName("group"), {
            {"fetch.max.bytes", "1048576"},
            {"max.partition.fetch.bytes", "1024"},
        });
        AssertRdKafkaOk(consumer->Handle->subscribe({std::string(topic)}), "subscribe");
        const auto consumed = ConsumePayloads(*consumer->Handle, payloads.size(), TDuration::Seconds(45));
        UNIT_ASSERT_VALUES_EQUAL(consumed, payloads);
    }

    Y_UNIT_TEST(FetchFromMiddleOfBatch) {
        const TString topic = UniqueName("rdk-consume-midbatch");
        CreateYdbTopic(topic, 1);
        auto producer = MakeProducer({
            {"linger.ms", "50"},
            {"batch.num.messages", "10"},
        });
        WaitTopicPartitions(*producer->Handle, topic, 1);
        ProduceAndFlush(*producer, topic, {"b0", "b1", "b2", "b3", "b4"});

        auto consumer = MakeConsumer(UniqueName("group"));
        std::vector<RdKafka::TopicPartition*> partitions;
        partitions.push_back(RdKafka::TopicPartition::create(std::string(topic), 0, 2));
        AssertRdKafkaOk(consumer->Handle->assign(partitions), "assign");
        RdKafka::TopicPartition::destroy(partitions);
        const auto rest = ConsumeMessages(*consumer->Handle, 3);
        UNIT_ASSERT_VALUES_EQUAL(rest[0].Payload, "b2");
        UNIT_ASSERT_VALUES_EQUAL(rest[1].Payload, "b3");
        UNIT_ASSERT_VALUES_EQUAL(rest[2].Payload, "b4");
        UNIT_ASSERT_VALUES_EQUAL(rest[0].Offset, 2);
    }

    Y_UNIT_TEST(TimestampsAndOffsetsForTimes) {
        const TString topic = UniqueName("rdk-consume-ts");
        CreateYdbTopic(topic, 1);
        auto producer = MakeProducer();
        WaitTopicPartitions(*producer->Handle, topic, 1);
        const int64_t ts = TInstant::Now().MilliSeconds();
        Produce(*producer->Handle, topic, "ts-payload", "ts-key", RdKafka::Topic::PARTITION_UA, ts);
        Flush(*producer);

        auto consumer = MakeConsumer(UniqueName("group"));
        AssertRdKafkaOk(consumer->Handle->subscribe({std::string(topic)}), "subscribe");
        const auto messages = ConsumeMessages(*consumer->Handle, 1);
        UNIT_ASSERT_VALUES_EQUAL(messages[0].Payload, "ts-payload");
        UNIT_ASSERT_GT(messages[0].Timestamp, 0);

        std::vector<RdKafka::TopicPartition*> query;
        query.push_back(RdKafka::TopicPartition::create(std::string(topic), 0, ts - 1000));
        const auto timesErr = producer->Handle->offsetsForTimes(query, 15000);
        if (timesErr == RdKafka::ERR_NO_ERROR && query[0]->err() == RdKafka::ERR_NO_ERROR) {
            UNIT_ASSERT_VALUES_EQUAL(query[0]->offset(), 0);
        }
        RdKafka::TopicPartition::destroy(query);
    }

    Y_UNIT_TEST(CheckCrcsDoesNotHang) {
        const TString topic = UniqueName("rdk-consume-crc");
        CreateYdbTopic(topic, 1);
        auto producer = MakeProducer();
        WaitTopicPartitions(*producer->Handle, topic, 1);
        ProduceAndFlush(*producer, topic, {"crc-payload"});

        auto consumer = MakeConsumer(UniqueName("group"), {{"check.crcs", "true"}});
        AssertRdKafkaOk(consumer->Handle->subscribe({std::string(topic)}), "subscribe");
        const TInstant deadline = TInstant::Now() + TDuration::Seconds(15);
        bool gotMessage = false;
        bool gotError = false;
        TString lastError;
        while (TInstant::Now() < deadline) {
            std::unique_ptr<RdKafka::Message> message(consumer->Handle->consume(500));
            UNIT_ASSERT(message);
            if (message->err() == RdKafka::ERR_NO_ERROR) {
                UNIT_ASSERT_VALUES_EQUAL(MessagePayload(*message), "crc-payload");
                gotMessage = true;
                break;
            }
            if (message->err() == RdKafka::ERR__TIMED_OUT) {
                continue;
            }
            gotError = true;
            lastError = TString(message->errstr());
            break;
        }
        UNIT_ASSERT_C(gotMessage || gotError, "check.crcs=true hung without a message or error: " << lastError);
    }

    Y_UNIT_TEST(MixRawAndCompressed) {
        const TString topic = UniqueName("rdk-consume-mix");
        CreateYdbTopic(topic, 1);
        auto raw = MakeProducer();
        WaitTopicPartitions(*raw->Handle, topic, 1);
        ProduceAndFlush(*raw, topic, {"raw-1", "raw-2"});

        if (!TopicMessagesBatchingEnabled()) {
            auto consumer = MakeConsumer(UniqueName("group"));
            AssertRdKafkaOk(consumer->Handle->subscribe({std::string(topic)}), "subscribe");
            const auto payloads = ConsumePayloads(*consumer->Handle, 2);
            UNIT_ASSERT_VALUES_EQUAL(payloads[0], "raw-1");
            UNIT_ASSERT_VALUES_EQUAL(payloads[1], "raw-2");
            return;
        }

        auto gzip = MakeProducer({{"compression.codec", "gzip"}});
        ProduceAndFlush(*gzip, topic, {"gzip-1"});
        auto consumer = MakeConsumer(UniqueName("group"));
        AssertRdKafkaOk(consumer->Handle->subscribe({std::string(topic)}), "subscribe");
        const auto payloads = ConsumePayloads(*consumer->Handle, 3);
        UNIT_ASSERT_VALUES_EQUAL(payloads[0], "raw-1");
        UNIT_ASSERT_VALUES_EQUAL(payloads[1], "raw-2");
        UNIT_ASSERT_VALUES_EQUAL(payloads[2], "gzip-1");
    }
}
