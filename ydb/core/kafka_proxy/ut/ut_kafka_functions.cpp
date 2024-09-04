#include <library/cpp/testing/unittest/registar.h>

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

}
