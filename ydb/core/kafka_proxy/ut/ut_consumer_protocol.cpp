#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/kafka_proxy/actors/kafka_read_session_utils.h>
#include <ydb/core/kafka_proxy/kafka_consumer_protocol.h>
#include <ydb/core/kafka_proxy/kafka_messages.h>

using namespace NKafka;

namespace {

constexpr TKafkaVersion ProtocolVersion = 3;

TKafkaBytes BytesFromBuffer(TKafkaWriteBuffer& buf) {
    const auto& front = buf.GetFrontBuffer();
    return TKafkaRawBytes(front.data(), front.size());
}

TString MakeShortBlob(size_t size) {
    return TString(size, '\x00');
}

TJoinGroupRequestData MakeJoinGroupRequest(TKafkaBytes metadata, TString protocolName = ASSIGN_STRATEGY_ROUNDROBIN) {
    TJoinGroupRequestData request;
    request.ProtocolType = SUPPORTED_JOIN_GROUP_PROTOCOL;

    TJoinGroupRequestData::TJoinGroupRequestProtocol protocol;
    protocol.Name = std::move(protocolName);
    protocol.Metadata = metadata;
    request.Protocols.push_back(std::move(protocol));
    return request;
}

} // namespace

Y_UNIT_TEST_SUITE(ConsumerProtocolBlob) {

Y_UNIT_TEST(TryReadAssignmentRejectsShortBlob) {
    for (size_t size = 0; size < KafkaConsumerProtocolVersionBytes; ++size) {
        TString blob = MakeShortBlob(size);
        TKafkaBytes assignment = TKafkaRawBytes(blob.data(), blob.size());
        UNIT_ASSERT_C(
            !TryReadConsumerProtocolBlob<TConsumerProtocolAssignment>(assignment),
            "size=" << size);
    }
}

Y_UNIT_TEST(TryReadAssignmentRejectsNullBlob) {
    UNIT_ASSERT(!TryReadConsumerProtocolBlob<TConsumerProtocolAssignment>(std::nullopt));
}

Y_UNIT_TEST(TryReadAssignmentParsesSyncGroupBlob) {
    TConsumerProtocolAssignment assignment;
    TConsumerProtocolAssignment::TopicPartition topicPartition;
    topicPartition.Topic = "topic";
    topicPartition.Partitions = {0, 1};
    assignment.AssignedPartitions.push_back(topicPartition);

    TKafkaWriteBuffer buf(assignment.Size(ProtocolVersion) + sizeof(ProtocolVersion));
    TKafkaWritable writable(buf);
    writable << ProtocolVersion;
    assignment.Write(writable, ProtocolVersion);

    auto parsed = TryReadConsumerProtocolBlob<TConsumerProtocolAssignment>(BytesFromBuffer(buf));
    UNIT_ASSERT(parsed);
    UNIT_ASSERT_VALUES_EQUAL(parsed->AssignedPartitions.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(*parsed->AssignedPartitions[0].Topic, "topic");
    UNIT_ASSERT_VALUES_EQUAL(parsed->AssignedPartitions[0].Partitions.size(), 2u);
}

Y_UNIT_TEST(GetSubscriptionsRejectsShortMetadata) {
    TString blob = MakeShortBlob(3);
    auto request = MakeJoinGroupRequest(TKafkaRawBytes(blob.data(), blob.size()));
    UNIT_ASSERT(!GetSubscriptions(request));
}

Y_UNIT_TEST(GetSubscriptionsRejectsEmptyMetadata) {
    auto request = MakeJoinGroupRequest(TKafkaRawBytes());
    UNIT_ASSERT(!GetSubscriptions(request));
}

Y_UNIT_TEST(GetSubscriptionsRejectsNullMetadata) {
    auto request = MakeJoinGroupRequest(std::nullopt);
    UNIT_ASSERT(!GetSubscriptions(request));
}

Y_UNIT_TEST(GetSubscriptionsRejectsWrongProtocolType) {
    TConsumerProtocolSubscription subscription;
    subscription.Topics.push_back("topic");

    TKafkaWriteBuffer buf(subscription.Size(ProtocolVersion) + sizeof(ProtocolVersion));
    TKafkaWritable writable(buf);
    writable << ProtocolVersion;
    subscription.Write(writable, ProtocolVersion);

    auto request = MakeJoinGroupRequest(BytesFromBuffer(buf));
    request.ProtocolType = "unknown";
    UNIT_ASSERT(!GetSubscriptions(request));
}

Y_UNIT_TEST(GetSubscriptionsParsesValidMetadata) {
    TConsumerProtocolSubscription subscription;
    subscription.Topics.push_back("topic-a");
    subscription.Topics.push_back("topic-b");

    TKafkaWriteBuffer buf(subscription.Size(ProtocolVersion) + sizeof(ProtocolVersion));
    TKafkaWritable writable(buf);
    writable << ProtocolVersion;
    subscription.Write(writable, ProtocolVersion);

    auto request = MakeJoinGroupRequest(BytesFromBuffer(buf));
    auto parsed = GetSubscriptions(request);
    UNIT_ASSERT(parsed);
    UNIT_ASSERT_VALUES_EQUAL(parsed->Topics.size(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(*parsed->Topics[0], "topic-a");
    UNIT_ASSERT_VALUES_EQUAL(*parsed->Topics[1], "topic-b");
}

}
