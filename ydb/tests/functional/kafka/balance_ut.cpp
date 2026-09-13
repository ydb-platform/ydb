#include "helpers.h"

using namespace NKafkaRdkafkaTests;

Y_UNIT_TEST_SUITE(KafkaLibrdkafkaBalance) {
    Y_UNIT_TEST(TwoConsumersSharePartitions) {
        const TString topic = UniqueName("rdk-balance");
        constexpr ui32 partitionCount = 4;
        CreateYdbTopic(topic, partitionCount);

        auto producer = MakeProducer();
        WaitTopicPartitions(*producer->Handle, topic, partitionCount);
        for (ui32 partition = 0; partition < partitionCount; ++partition) {
            Produce(
                *producer->Handle,
                topic,
                TStringBuilder() << "payload-" << partition,
                TStringBuilder() << "key-" << partition,
                static_cast<int32_t>(partition));
        }
        Flush(*producer);

        const TString group = UniqueName("balance-group");
        auto first = MakeConsumer(group);
        auto second = MakeConsumer(group);
        AssertRdKafkaOk(first->Handle->subscribe({std::string(topic)}), "subscribe first");
        AssertRdKafkaOk(second->Handle->subscribe({std::string(topic)}), "subscribe second");
        WaitBalanced(*first->Handle, *second->Handle, partitionCount);
    }
}
