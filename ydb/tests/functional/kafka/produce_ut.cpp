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
        const auto payloads = ConsumePayloads(*consumer->Handle, 1);
        UNIT_ASSERT_VALUES_EQUAL(payloads[0], "hello-one");
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
        const auto consumed = ConsumePayloads(*consumer->Handle, payloads.size());
        UNIT_ASSERT_VALUES_EQUAL(consumed, payloads);
    }
}
