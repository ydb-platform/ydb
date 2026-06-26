#include <ydb/services/persqueue_v1/actors/schema_actors.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/include/control_plane.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NGRpcProxy::V1::NTests {

using namespace NYdb::NPersQueue;

Y_UNIT_TEST_SUITE(FillReadRuleConsumerType) {

Y_UNIT_TEST(UnknownConsumerTypeDefaultsToStreaming) {
    NKikimrPQ::TPQTabletConfig::TConsumer consumer;
    // Field 11 (Type) = 99 is not a known EConsumerType value; set via wire format because
    // SetType() validates the enum in debug builds.
    UNIT_ASSERT(consumer.ParseFromString(std::string("\x58\x63", 2)));
    UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(consumer.GetType()), 99);

    Ydb::PersQueue::V1::TopicSettings::ReadRule readRule;
    FillReadRuleConsumerType(consumer, &readRule);

    UNIT_ASSERT(readRule.has_streaming_consumer_type());
    UNIT_ASSERT(!readRule.has_shared_consumer_type());

    const TDescribeTopicResult::TTopicSettings::TReadRule sdkReadRule(readRule);
    UNIT_ASSERT(!sdkReadRule.SharedConsumer().has_value());

    TReadRuleSettings roundTrip;
    roundTrip.SetSettings(sdkReadRule);
    UNIT_ASSERT(!roundTrip.GetSharedConsumer().has_value());
}

Y_UNIT_TEST(StreamingConsumerTypeSetsStreamingConsumerType) {
    NKikimrPQ::TPQTabletConfig::TConsumer consumer;
    consumer.SetName("test_consumer");
    consumer.SetType(NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_STREAMING);

    Ydb::PersQueue::V1::TopicSettings::ReadRule readRule;
    FillReadRuleConsumerType(consumer, &readRule);

    UNIT_ASSERT(readRule.has_streaming_consumer_type());
    UNIT_ASSERT(!readRule.has_shared_consumer_type());
}

Y_UNIT_TEST(MlpConsumerTypeSetsSharedConsumerType) {
    NKikimrPQ::TPQTabletConfig::TConsumer consumer;
    consumer.SetName("test_consumer");
    consumer.SetType(NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP);
    consumer.SetKeepMessageOrder(true);

    Ydb::PersQueue::V1::TopicSettings::ReadRule readRule;
    FillReadRuleConsumerType(consumer, &readRule);

    UNIT_ASSERT(!readRule.has_streaming_consumer_type());
    UNIT_ASSERT(readRule.has_shared_consumer_type());
    UNIT_ASSERT_VALUES_EQUAL(readRule.shared_consumer_type().keep_messages_order(), true);
}

} // Y_UNIT_TEST_SUITE(FillReadRuleConsumerType)

} // namespace NKikimr::NGRpcProxy::V1::NTests
