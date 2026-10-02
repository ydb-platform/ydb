#include "helpers.h"

using namespace NKafkaRdkafkaTests;

Y_UNIT_TEST_SUITE(KafkaLibrdkafkaProduce) {
    Y_UNIT_TEST(OneMessage) {
        const TString topic = UniqueName("rdk-produce-one");
        CreateYdbTopic(topic, 1);

        auto producer = MakeProducer();
        WaitTopicPartitions(*producer->Handle, topic, 1);
        ProduceAndFlush(*producer, topic, {"hello-one"}, {"key-one"});

        auto consumer = MakeConsumer(UniqueName("group"));
        AssertRdKafkaOk(consumer->Handle->subscribe({std::string(topic)}), "subscribe");
        const auto messages = ConsumeMessages(*consumer->Handle, 1);
        UNIT_ASSERT_VALUES_EQUAL(messages[0].Payload, "hello-one");
        UNIT_ASSERT_VALUES_EQUAL(messages[0].Key, "key-one");
    }

    Y_UNIT_TEST(SeveralMessagesInOneBatch) {
        const TString topic = UniqueName("rdk-produce-batch");
        CreateYdbTopic(topic, 1);

        auto producer = MakeProducer({
            {"linger.ms", "50"},
            {"batch.num.messages", "10"},
        });
        WaitTopicPartitions(*producer->Handle, topic, 1);

        const TVector<TString> payloads = {"m0", "m1", "m2", "m3", "m4"};
        const TVector<TString> keys = {"k0", "k1", "k2", "k3", "k4"};
        ProduceAndFlush(*producer, topic, payloads, keys);
        UNIT_ASSERT_VALUES_EQUAL(producer->Dr.Ok.load(), static_cast<int>(payloads.size()));

        auto consumer = MakeConsumer(UniqueName("group"));
        AssertRdKafkaOk(consumer->Handle->subscribe({std::string(topic)}), "subscribe");
        const auto consumed = ConsumeMessages(*consumer->Handle, payloads.size());
        UNIT_ASSERT_VALUES_EQUAL(consumed.size(), payloads.size());
        for (size_t i = 0; i < payloads.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(consumed[i].Payload, payloads[i]);
            UNIT_ASSERT_VALUES_EQUAL(consumed[i].Key, keys[i]);
        }
    }

    Y_UNIT_TEST(EmptyAndNullKeyValue) {
        const TString topic = UniqueName("rdk-produce-empty");
        CreateYdbTopic(topic, 1);
        auto producer = MakeProducer();
        WaitTopicPartitions(*producer->Handle, topic, 1);

        Produce(*producer->Handle, topic, "", "");
        AssertRdKafkaOk(ProduceOnce(*producer->Handle, topic, {}, {}, RdKafka::Topic::PARTITION_UA, true), "null key produce");
        Flush(*producer);

        auto consumer = MakeConsumer(UniqueName("group"));
        AssertRdKafkaOk(consumer->Handle->subscribe({std::string(topic)}), "subscribe");
        const auto messages = ConsumeMessages(*consumer->Handle, 2);
        UNIT_ASSERT_VALUES_EQUAL(messages[0].Payload, "");
        UNIT_ASSERT_VALUES_EQUAL(messages[1].Payload, "");
    }

    Y_UNIT_TEST(HeadersRoundTrip) {
        const TString topic = UniqueName("rdk-produce-hdrs");
        CreateYdbTopic(topic, 1);
        auto producer = MakeProducer();
        WaitTopicPartitions(*producer->Handle, topic, 1);
        ProduceWithHeaders(*producer->Handle, topic, "hdr-payload", "hdr-key", {{"h1", "v1"}, {"h2", "v2"}});
        Flush(*producer);

        auto consumer = MakeConsumer(UniqueName("group"));
        AssertRdKafkaOk(consumer->Handle->subscribe({std::string(topic)}), "subscribe");
        auto messages = ConsumeMessages(*consumer->Handle, 1);
        UNIT_ASSERT_VALUES_EQUAL(messages[0].Payload, "hdr-payload");
        UNIT_ASSERT_VALUES_EQUAL(messages[0].Key, "hdr-key");
        UNIT_ASSERT_VALUES_EQUAL(messages[0].Headers["h1"], "v1");
        UNIT_ASSERT_VALUES_EQUAL(messages[0].Headers["h2"], "v2");
    }

    Y_UNIT_TEST(SeveralPartitionsInOneFlush) {
        const TString topic = UniqueName("rdk-produce-parts");
        constexpr ui32 partitions = 3;
        CreateYdbTopic(topic, partitions);
        auto producer = MakeProducer();
        WaitTopicPartitions(*producer->Handle, topic, partitions);
        for (ui32 partition = 0; partition < partitions; ++partition) {
            Produce(
                *producer->Handle,
                topic,
                TStringBuilder() << "p" << partition,
                TStringBuilder() << "k" << partition,
                static_cast<int32_t>(partition));
        }
        Flush(*producer);

        auto consumer = MakeConsumer(UniqueName("group"));
        AssertRdKafkaOk(consumer->Handle->subscribe({std::string(topic)}), "subscribe");
        auto messages = ConsumeMessages(*consumer->Handle, partitions);
        THashSet<TString> payloads;
        for (const auto& message : messages) {
            payloads.insert(message.Payload);
        }
        UNIT_ASSERT(payloads.contains("p0"));
        UNIT_ASSERT(payloads.contains("p1"));
        UNIT_ASSERT(payloads.contains("p2"));
    }

    Y_UNIT_TEST(LargeMessage) {
        const TString topic = UniqueName("rdk-produce-large");
        CreateYdbTopic(topic, 1);
        auto producer = MakeProducer({{"message.max.bytes", "2000000"}});
        WaitTopicPartitions(*producer->Handle, topic, 1);
        const TString payload(100 * 1024, 'x');
        ProduceAndFlush(*producer, topic, {payload}, {"large-key"});

        auto consumer = MakeConsumer(UniqueName("group"), {{"fetch.message.max.bytes", "2000000"}});
        AssertRdKafkaOk(consumer->Handle->subscribe({std::string(topic)}), "subscribe");
        const auto messages = ConsumeMessages(*consumer->Handle, 1);
        UNIT_ASSERT_VALUES_EQUAL(messages[0].Payload.size(), payload.size());
        UNIT_ASSERT_VALUES_EQUAL(messages[0].Key, "large-key");
    }

    Y_UNIT_TEST(UnknownTopic) {
        auto producer = MakeProducer({{"message.timeout.ms", "8000"}});
        const TString topic = UniqueName("rdk-missing-topic");
        const auto err = ProduceOnce(*producer->Handle, topic, "missing");
        if (err == RdKafka::ERR_NO_ERROR) {
            FlushAllowErrors(*producer);
        }
        UNIT_ASSERT_C(
            producer->Dr.Fail.load() > 0 || IsUnknownTopicErr(err),
            TString(RdKafka::err2str(err)) + " " + producer->Dr.LastError);
    }

    Y_UNIT_TEST(UnknownPartition) {
        const TString topic = UniqueName("rdk-missing-part");
        CreateYdbTopic(topic, 1);
        auto producer = MakeProducer({{"message.timeout.ms", "8000"}});
        WaitTopicPartitions(*producer->Handle, topic, 1);
        const auto err = ProduceOnce(*producer->Handle, topic, "missing-part", "k", /*partition*/ 99);
        if (err == RdKafka::ERR_NO_ERROR) {
            FlushAllowErrors(*producer);
        }
        UNIT_ASSERT_C(
            producer->Dr.Fail.load() > 0 || IsUnknownTopicErr(err),
            TString(RdKafka::err2str(err)) + " " + producer->Dr.LastError);
    }

    Y_UNIT_TEST(ShortAndFullTopicName) {
        const TString topic = UniqueName("rdk-produce-path");
        CreateYdbTopic(topic, 1);
        const TString full = TopicFullPath(topic);

        auto producer = MakeProducer();
        WaitTopicPartitions(*producer->Handle, topic, 1);
        ProduceAndFlush(*producer, topic, {"via-short"}, {"short-key"});

        auto consumer = MakeConsumer(UniqueName("group"));
        AssertRdKafkaOk(consumer->Handle->subscribe({std::string(full)}), "subscribe full");
        const auto first = ConsumeMessages(*consumer->Handle, 1);
        UNIT_ASSERT_VALUES_EQUAL(first[0].Payload, "via-short");
        UNIT_ASSERT_VALUES_EQUAL(first[0].Key, "short-key");

        auto producerFull = MakeProducer();
        ProduceAndFlush(*producerFull, full, {"via-full"}, {"full-key"});

        auto consumerShort = MakeConsumer(UniqueName("group"));
        AssertRdKafkaOk(consumerShort->Handle->subscribe({std::string(topic)}), "subscribe short");
        const auto second = ConsumeMessages(*consumerShort->Handle, 2);
        THashSet<TString> payloads;
        for (const auto& message : second) {
            payloads.insert(message.Payload);
        }
        UNIT_ASSERT(payloads.contains("via-short"));
        UNIT_ASSERT(payloads.contains("via-full"));
    }

    Y_UNIT_TEST(IdempotentProduceWithoutTxn) {
        const TString topic = UniqueName("rdk-produce-idemp");
        CreateYdbTopic(topic, 1);
        auto producer = MakeProducer({{"enable.idempotence", "true"}});
        WaitTopicPartitions(*producer->Handle, topic, 1);
        ProduceAndFlush(*producer, topic, {"idemp-value"}, {"idemp-key"});

        auto consumer = MakeConsumer(UniqueName("group"));
        AssertRdKafkaOk(consumer->Handle->subscribe({std::string(topic)}), "subscribe");
        const auto messages = ConsumeMessages(*consumer->Handle, 1);
        UNIT_ASSERT_VALUES_EQUAL(messages[0].Payload, "idemp-value");
        UNIT_ASSERT_VALUES_EQUAL(messages[0].Key, "idemp-key");
    }

    void DoCompression(const TString& codec) {
        const TString topic = UniqueName(TString("rdk-produce-") + codec);
        CreateYdbTopic(topic, 1);
        auto producer = MakeProducer({
            {"compression.codec", codec},
            {"compression.type", codec},
            {"linger.ms", "50"},
            {"batch.num.messages", "5"},
            {"message.timeout.ms", "15000"},
        });
        WaitTopicPartitions(*producer->Handle, topic, 1);
        for (int i = 0; i < 5; ++i) {
            Produce(*producer->Handle, topic, TStringBuilder() << "compressed-" << codec << "-" << i, "ckey");
        }
        FlushAllowErrors(*producer);
        if (TopicMessagesBatchingEnabled()) {
            UNIT_ASSERT_GT(producer->Dr.Ok.load(), 0);
            UNIT_ASSERT_VALUES_EQUAL(producer->Dr.Fail.load(), 0);
            auto consumer = MakeConsumer(UniqueName("group"));
            AssertRdKafkaOk(consumer->Handle->subscribe({std::string(topic)}), "subscribe");
            const auto messages = ConsumeMessages(*consumer->Handle, 5);
            UNIT_ASSERT_VALUES_EQUAL(messages[0].Payload, TString("compressed-") + codec + "-0");
            return;
        }
        if (producer->Dr.Ok.load() > 0 && producer->Dr.Fail.load() == 0) {
            auto consumer = MakeConsumer(UniqueName("group"));
            AssertRdKafkaOk(consumer->Handle->subscribe({std::string(topic)}), "subscribe");
            ConsumeMessages(*consumer->Handle, 5);
            return;
        }
        UNIT_ASSERT_C(producer->Dr.Fail.load() > 0, "compressed produce failed as expected without batching");
    }

    Y_UNIT_TEST(CompressionGzip) {
        DoCompression("gzip");
    }

    Y_UNIT_TEST(CompressionZstd) {
        DoCompression("zstd");
    }

    Y_UNIT_TEST(AutopartitionedTopicRejected) {
        const TString topic = UniqueName("rdk-produce-auto");
        CreateAutopartitionedYdbTopic(topic);

        auto driver = MakeYdbDriver();
        NYdb::NTopic::TTopicClient client(driver);
        auto desc = client.DescribeTopic(std::string(topic)).ExtractValueSync();
        UNIT_ASSERT_C(desc.IsSuccess(), desc.GetIssues().ToString());
        UNIT_ASSERT(
            desc.GetTopicDescription().GetPartitioningSettings().GetAutoPartitioningSettings().GetStrategy()
                == NYdb::NTopic::EAutoPartitioningStrategy::ScaleUp);

        auto producer = MakeProducer({{"message.timeout.ms", "8000"}});
        const auto err = ProduceOnce(*producer->Handle, topic, "auto-payload");
        if (err == RdKafka::ERR_NO_ERROR) {
            FlushAllowErrors(*producer);
        }
        if (producer->Dr.Ok.load() > 0) {
            auto consumer = MakeConsumer(UniqueName("group"));
            AssertRdKafkaOk(consumer->Handle->subscribe({std::string(topic)}), "subscribe");
            UNIT_ASSERT_VALUES_EQUAL(ConsumePayloads(*consumer->Handle, 1)[0], "auto-payload");
            return;
        }
        UNIT_ASSERT_C(
            producer->Dr.Fail.load() > 0 || err != RdKafka::ERR_NO_ERROR,
            "kafka produce to autopartitioned topic must fail");
    }
}
