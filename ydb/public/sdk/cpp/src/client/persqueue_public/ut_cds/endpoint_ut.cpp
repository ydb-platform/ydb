#include <string>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::inline Dev::NPersQueue {
    bool IsFederation(const std::string& endpoint);
} // namespace NYdb::inline Dev::NPersQueue

Y_UNIT_TEST_SUITE(PersQueueFederationEndpointTest) {
    Y_UNIT_TEST(RecognizesLbkSubdomain) {
        UNIT_ASSERT(NYdb::Dev::NPersQueue::IsFederation("service.lbk.logbroker.yandex.net:2135"));
        UNIT_ASSERT(!NYdb::Dev::NPersQueue::IsFederation("notlbk.logbroker.yandex.net:2135"));
        UNIT_ASSERT(!NYdb::Dev::NPersQueue::IsFederation("service.lbk.logbroker.yandex.net.example.org:2135"));
    }
} // Y_UNIT_TEST_SUITE(PersQueueFederationEndpointTest)
