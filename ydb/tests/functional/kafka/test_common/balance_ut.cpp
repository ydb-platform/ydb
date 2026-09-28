#include "helpers.h"

using namespace NKafkaRdkafkaTests;

namespace {
    void ProduceToAllPartitions(const TString& topic, ui32 partitionCount) {
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
    }
}

Y_UNIT_TEST_SUITE(KafkaLibrdkafkaBalance) {
    Y_UNIT_TEST(TwoConsumersSharePartitions) {
        const TString topic = UniqueName("rdk-balance");
        constexpr ui32 partitionCount = 4;
        CreateYdbTopic(topic, partitionCount);
        ProduceToAllPartitions(topic, partitionCount);

        const TString group = UniqueName("balance-group");
        auto first = MakeConsumer(group);
        auto second = MakeConsumer(group);
        AssertRdKafkaOk(first->Handle->subscribe({std::string(topic)}), "subscribe first");
        AssertRdKafkaOk(second->Handle->subscribe({std::string(topic)}), "subscribe second");
        WaitBalanced(*first->Handle, *second->Handle, partitionCount);
    }

    Y_UNIT_TEST(CooperativeSticky) {
        const TString topic = UniqueName("rdk-balance-coop");
        constexpr ui32 partitionCount = 4;
        CreateYdbTopic(topic, partitionCount);
        ProduceToAllPartitions(topic, partitionCount);

        const TString group = UniqueName("coop-group");
        auto first = MakeConsumer(group, {{"partition.assignment.strategy", "cooperative-sticky"}});
        auto second = MakeConsumer(group, {{"partition.assignment.strategy", "cooperative-sticky"}});
        AssertRdKafkaOk(first->Handle->subscribe({std::string(topic)}), "subscribe first");
        AssertRdKafkaOk(second->Handle->subscribe({std::string(topic)}), "subscribe second");
        WaitBalanced(*first->Handle, *second->Handle, partitionCount);
    }

    Y_UNIT_TEST(RangeAssignor) {
        const TString topic = UniqueName("rdk-balance-range");
        constexpr ui32 partitionCount = 4;
        CreateYdbTopic(topic, partitionCount);
        ProduceToAllPartitions(topic, partitionCount);

        const TString group = UniqueName("range-group");
        auto first = MakeConsumer(group, {{"partition.assignment.strategy", "range"}});
        auto second = MakeConsumer(group, {{"partition.assignment.strategy", "range"}});
        AssertRdKafkaOk(first->Handle->subscribe({std::string(topic)}), "subscribe first");
        AssertRdKafkaOk(second->Handle->subscribe({std::string(topic)}), "subscribe second");
        WaitBalanced(*first->Handle, *second->Handle, partitionCount);
    }

    Y_UNIT_TEST(ThirdConsumerJoins) {
        const TString topic = UniqueName("rdk-balance-three");
        constexpr ui32 partitionCount = 6;
        CreateYdbTopic(topic, partitionCount);
        ProduceToAllPartitions(topic, partitionCount);

        const TString group = UniqueName("three-group");
        auto first = MakeConsumer(group);
        auto second = MakeConsumer(group);
        AssertRdKafkaOk(first->Handle->subscribe({std::string(topic)}), "subscribe first");
        AssertRdKafkaOk(second->Handle->subscribe({std::string(topic)}), "subscribe second");
        WaitBalanced(*first->Handle, *second->Handle, partitionCount);

        auto third = MakeConsumer(group);
        AssertRdKafkaOk(third->Handle->subscribe({std::string(topic)}), "subscribe third");
        WaitGroupCovers({first->Handle.get(), second->Handle.get(), third->Handle.get()}, partitionCount);
    }

    Y_UNIT_TEST(LeaveReturnsPartitions) {
        const TString topic = UniqueName("rdk-balance-leave");
        constexpr ui32 partitionCount = 4;
        CreateYdbTopic(topic, partitionCount);
        ProduceToAllPartitions(topic, partitionCount);

        const TString group = UniqueName("leave-group");
        auto first = MakeConsumer(group);
        auto second = MakeConsumer(group);
        AssertRdKafkaOk(first->Handle->subscribe({std::string(topic)}), "subscribe first");
        AssertRdKafkaOk(second->Handle->subscribe({std::string(topic)}), "subscribe second");
        WaitBalanced(*first->Handle, *second->Handle, partitionCount);

        first.reset();
        WaitAssignment(*second->Handle, partitionCount);
    }

    Y_UNIT_TEST(SessionTimeoutReassigns) {
        const TString topic = UniqueName("rdk-balance-timeout");
        constexpr ui32 partitionCount = 4;
        CreateYdbTopic(topic, partitionCount);
        ProduceToAllPartitions(topic, partitionCount);

        const THashMap<TString, TString> conf = {
            {"session.timeout.ms", "6000"},
            {"heartbeat.interval.ms", "1000"},
            {"max.poll.interval.ms", "6000"},
        };
        const TString group = UniqueName("timeout-group");
        auto first = MakeConsumer(group, conf);
        auto second = MakeConsumer(group, conf);
        AssertRdKafkaOk(first->Handle->subscribe({std::string(topic)}), "subscribe first");
        AssertRdKafkaOk(second->Handle->subscribe({std::string(topic)}), "subscribe second");
        WaitBalanced(*first->Handle, *second->Handle, partitionCount);

        const TInstant deadline = TInstant::Now() + TDuration::Seconds(40);
        while (TInstant::Now() < deadline) {
            delete second->Handle->consume(200);
            if (AssignmentPartitions(*second->Handle).size() == partitionCount) {
                return;
            }
        }
        UNIT_FAIL("second consumer did not take over after session timeout");
    }

    Y_UNIT_TEST(SubscribeMultipleTopics) {
        const TString firstTopic = UniqueName("rdk-balance-mt-a");
        const TString secondTopic = UniqueName("rdk-balance-mt-b");
        CreateYdbTopic(firstTopic, 1);
        CreateYdbTopic(secondTopic, 1);
        auto producer = MakeProducer();
        WaitTopicPartitions(*producer->Handle, firstTopic, 1);
        WaitTopicPartitions(*producer->Handle, secondTopic, 1);
        ProduceAndFlush(*producer, firstTopic, {"a"});
        ProduceAndFlush(*producer, secondTopic, {"b"});

        auto consumer = MakeConsumer(UniqueName("multi-group"));
        AssertRdKafkaOk(
            consumer->Handle->subscribe({std::string(firstTopic), std::string(secondTopic)}),
            "subscribe two topics");
        WaitAssignment(*consumer->Handle, 2);
        const auto keys = AssignmentKeys(*consumer->Handle);
        UNIT_ASSERT_VALUES_EQUAL(keys.size(), 2);
        bool hasFirst = false;
        bool hasSecond = false;
        for (const auto& key : keys) {
            hasFirst = hasFirst || key.Contains(firstTopic);
            hasSecond = hasSecond || key.Contains(secondTopic);
        }
        UNIT_ASSERT_C(hasFirst, "assignment is missing " << firstTopic);
        UNIT_ASSERT_C(hasSecond, "assignment is missing " << secondTopic);

        auto lateProducer = MakeProducer();
        ProduceAndFlush(*lateProducer, firstTopic, {"a2"});
        ProduceAndFlush(*lateProducer, secondTopic, {"b2"});
        THashSet<TString> payloads;
        const TInstant deadline = TInstant::Now() + TDuration::Seconds(15);
        while (TInstant::Now() < deadline && payloads.size() < 4) {
            std::unique_ptr<RdKafka::Message> message(consumer->Handle->consume(500));
            UNIT_ASSERT(message);
            if (message->err() == RdKafka::ERR_NO_ERROR) {
                payloads.insert(MessagePayload(*message));
            } else if (message->err() != RdKafka::ERR__TIMED_OUT) {
                UNIT_FAIL("consume failed: " << RdKafka::err2str(message->err()));
            }
        }
        UNIT_ASSERT_C(!payloads.empty(), "subscribed consumer received no messages");
    }
}
