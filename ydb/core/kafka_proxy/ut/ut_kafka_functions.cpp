#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/kafka_proxy/actors/kafka_read_session_utils.h>
#include <ydb/core/kafka_proxy/kafka_messages.h>

using namespace NKafka;

const std::vector<i16> apiKeys {
     PRODUCE,
     FETCH,
     METADATA,
     API_VERSIONS,
     INIT_PRODUCER_ID
};

Y_UNIT_TEST_SUITE(Functions) {

Y_UNIT_TEST(CreateRequest) {
    for(i16 apiKey : apiKeys) {
        auto result = CreateRequest(apiKey);
        UNIT_ASSERT_EQUAL(result->ApiKey(), apiKey);
    }
}

Y_UNIT_TEST(CreateResponse) {
    for(i16 apiKey : apiKeys) {
        auto result = CreateResponse(apiKey);
        UNIT_ASSERT_EQUAL(result->ApiKey(), apiKey);
    }
}

Y_UNIT_TEST(GetSubscriptionsReadsVersionPrefix) {
    constexpr TKafkaVersion version = 3;

    TConsumerProtocolSubscription subscription;
    subscription.Topics = {"topic"};
    subscription.GenerationId = 42;
    subscription.RackId = "rack";

    TKafkaWriteBuffer buffer(1024);
    TKafkaWritable writable(buffer);
    writable << version;
    subscription.Write(writable, version);

    const TString metadata = buffer.AsString();

    TJoinGroupRequestData request;
    request.ProtocolType = SUPPORTED_JOIN_GROUP_PROTOCOL;
    auto& protocol = request.Protocols.emplace_back();
    protocol.Name = ASSIGN_STRATEGY_ROUNDROBIN;
    protocol.Metadata = TKafkaRawBytes(metadata.data(), metadata.size());

    const auto result = GetSubscriptions(request);

    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL(result->Topics.size(), 1u);
    UNIT_ASSERT(result->Topics.front());
    UNIT_ASSERT_VALUES_EQUAL(*result->Topics.front(), "topic");
    UNIT_ASSERT_VALUES_EQUAL(result->GenerationId, 42);
    UNIT_ASSERT(result->RackId);
    UNIT_ASSERT_VALUES_EQUAL(*result->RackId, "rack");
}

}
