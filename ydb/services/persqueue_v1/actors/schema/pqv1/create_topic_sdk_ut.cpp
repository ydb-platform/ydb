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

Y_UNIT_TEST(CreateTopicWithStreamingConsumer) {
    TPqv1SdkTestSetup setup("CreateTopicWithStreamingConsumer");

    auto& client = setup.GetPersQueueClient();
    const std::string path = TPqv1SdkTestSetup::MakeTopicPath("topic-streaming");

    TCreateTopicSettings settings;
    settings.ReadRules({TReadRuleSettings{}.ConsumerName(DEFAULT_STREAMING_CONSUMER)});

    AssertStatusSuccess(CreateTopicViaSdk(client, path, settings), "CreateTopic");

    const auto describe = DescribeTopicViaSdk(client, path);
    AssertStatusSuccess(describe, "DescribeTopic");

    TExpectedTopicSettings expected = MakeDefaultCreateTopicExpectation();
    expected.ReadRules.push_back({.ConsumerName = DEFAULT_STREAMING_CONSUMER});
    AssertTopicSettings(describe.TopicSettings(), expected);

    AssertConsumerTypeViaDescriber(
        setup.GetRuntime(),
        TString(setup.GetBaseSetup().GetDatabase()),
        TString(path),
        DEFAULT_STREAMING_CONSUMER,
        NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_STREAMING);
}

Y_UNIT_TEST(CreateTopicWithSharedConsumer) {
    TPqv1SdkTestSetup setup("CreateTopicWithSharedConsumer");

    auto& client = setup.GetPersQueueClient();
    const std::string path = TPqv1SdkTestSetup::MakeTopicPath("topic-shared");

    TCreateTopicSettings settings;
    settings.ReadRules({MakeSharedConsumerReadRuleSettings()});

    AssertStatusSuccess(CreateTopicViaSdk(client, path, settings), "CreateTopic");

    const auto describe = DescribeTopicViaSdk(client, path);
    AssertStatusSuccess(describe, "DescribeTopic");

    TExpectedTopicSettings expected = MakeDefaultCreateTopicExpectation();
    expected.ReadRules.push_back({
        .ConsumerName = DEFAULT_SHARED_CONSUMER,
        .StartingMessageTimestamp = TInstant::MilliSeconds(1000),
        .Version = 1,
    });
    AssertTopicSettings(describe.TopicSettings(), expected);

    AssertConsumerTypeViaDescriber(
        setup.GetRuntime(),
        TString(setup.GetBaseSetup().GetDatabase()),
        TString(path),
        DEFAULT_SHARED_CONSUMER,
        NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP);

    AssertSharedConsumerViaDescriber(
        setup.GetRuntime(),
        TString(setup.GetBaseSetup().GetDatabase()),
        TString(path),
        DEFAULT_SHARED_CONSUMER,
        TExpectedSharedConsumer{
            .KeepMessagesOrder = true,
            .DefaultProcessingTimeoutSeconds = 3,
            .ReceiveMessageWaitTimeMs = 5000,
            .ReceiveMessageDelayMs = 7000,
            .MaxProcessingAttempts = 11,
            .DeadLetterQueue = DEFAULT_DEAD_LETTER_QUEUE,
        });
}

} // Y_UNIT_TEST_SUITE(CreateTopic_PQv1SDK)

} // namespace NKikimr::NGRpcProxy::V1::NPQv1::NTests
