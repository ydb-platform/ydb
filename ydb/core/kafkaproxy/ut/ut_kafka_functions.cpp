#include <library/cpp/testing/gtest/gtest.h>

#include <strstream>

#include "../kafka_messages.h"

using namespace NKafka;

const std::vector<i16> apiKeys {
     PRODUCE,
     FETCH,
     METADATA,
     API_VERSIONS,
     INIT_PRODUCER_ID
};

TEST(Functions, CreateRequest) {
    for(i16 apiKey : apiKeys) {
        auto result = CreateRequest(apiKey);
        EXPECT_EQ(result->ApiKey(), apiKey);
    }
}

TEST(Functions, CreateResponse) {
    for(i16 apiKey : apiKeys) {
        auto result = CreateResponse(apiKey);
        EXPECT_EQ(result->ApiKey(), apiKey);
    }
}
