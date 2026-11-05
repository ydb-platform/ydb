#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/producer.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/exceptions/exceptions.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::inline Dev::NTopic {
    Y_UNIT_TEST_SUITE(TopicProducerFederationEndpointTest) {
        Y_UNIT_TEST(RejectsLbkSubdomain) {
            TDriverConfig config;
            config.SetEndpoint("service.lbk.logbroker.yandex.net:2135");
            config.SetDatabase("/Root");
            config.SetDiscoveryMode(EDiscoveryMode::Off);
            TDriver driver(config);
            TTopicClient client(driver);
            TProducerSettings settings;
            settings.Path("test-topic");
            settings.ProducerIdPrefix("test-producer");
            UNIT_ASSERT_EXCEPTION_CONTAINS(
                client.CreateProducer(settings), TContractViolation,
                "Producer is not supported for federation");
        }
    } // Y_UNIT_TEST_SUITE(TopicProducerFederationEndpointTest)
} // namespace NYdb::inline Dev::NTopic
