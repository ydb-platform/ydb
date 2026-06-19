#include "pqv1_sdk_test_utils.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NGRpcProxy::V1::NPQv1::NTests {

using namespace NYdb::NPersQueue;

Y_UNIT_TEST_SUITE(CreateTopic_PQv1SDK) {

Y_UNIT_TEST(CreateTopic) {
    TPqv1SdkTestSetup setup("CreateTopic");

    auto& client = setup.GetPersQueueClient();
    const std::string path = TPqv1SdkTestSetup::MakeTopicPath();

    TCreateTopicSettings settings;

    AssertStatusSuccess(CreateTopicViaSdk(client, path, settings), "CreateTopic");

    const auto describe = DescribeTopicViaSdk(client, path);
    AssertStatusSuccess(describe, "DescribeTopic");
    AssertTopicSettings(describe.TopicSettings(), MakeDefaultCreateTopicExpectation());
}

} // Y_UNIT_TEST_SUITE(CreateTopic_PQv1SDK)

} // namespace NKikimr::NGRpcProxy::V1::NPQv1::NTests
