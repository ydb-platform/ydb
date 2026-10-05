#include "pqv1_sdk_test_utils.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/size_literals.h>

namespace NKikimr::NGRpcProxy::V1::NPQv1::NTests {

using namespace NYdb;
using namespace NYdb::NPersQueue;

Y_UNIT_TEST_SUITE(DescribeTopic_PQv1SDK) {

Y_UNIT_TEST(DescribeMissingTopic) {
    TPqv1SdkTestSetup setup("DescribeMissingTopic");

    auto& client = setup.GetPersQueueClient();
    const auto describe = DescribeTopicViaSdk(client, "/Root/no_such_topic");
    UNIT_ASSERT_C(!describe.IsSuccess(), describe.GetIssues().ToOneLineString());
    UNIT_ASSERT_VALUES_EQUAL(describe.GetStatus(), EStatus::SCHEME_ERROR);
}

Y_UNIT_TEST(DescribeCreatedTopicSettings) {
    TPqv1SdkTestSetup setup("DescribeCreatedTopicSettings");

    auto& client = setup.GetPersQueueClient();
    const std::string path = TPqv1SdkTestSetup::MakeTopicPath("topic-describe");

    TCreateTopicSettings settings;
    settings
        .PartitionsCount(3)
        .RetentionPeriod(TDuration::Hours(6))
        .MaxPartitionWriteSpeed(123456)
        .MaxPartitionWriteBurst(234567)
        .SupportedCodecs({ECodec::RAW, ECodec::GZIP})
        .AllowUnauthenticatedRead(true)
        .AbcId(42)
        .AbcSlug("slug")
        .ReadRules({
            TReadRuleSettings{}
                .ConsumerName(DEFAULT_STREAMING_CONSUMER)
                .Important(true)
                .StartingMessageTimestamp(TInstant::MilliSeconds(1000))
                .Version(7)
                .SupportedCodecs({ECodec::RAW, ECodec::GZIP, ECodec::ZSTD}),
        });

    const auto createStatus = CreateTopicViaSdk(client, path, settings);
    UNIT_ASSERT_C(createStatus.IsSuccess(), "CreateTopic: " << createStatus.GetIssues().ToOneLineString());

    const auto describe = DescribeTopicViaSdk(client, path);
    UNIT_ASSERT_C(describe.IsSuccess(), "DescribeTopic: " << describe.GetIssues().ToOneLineString());

    const auto& topicSettings = describe.TopicSettings();
    UNIT_ASSERT_VALUES_EQUAL(topicSettings.PartitionsCount(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(topicSettings.RetentionPeriod(), TDuration::Hours(6));
    UNIT_ASSERT_VALUES_EQUAL(topicSettings.MaxPartitionWriteSpeed(), 123456u);
    UNIT_ASSERT_VALUES_EQUAL(topicSettings.MaxPartitionWriteBurst(), 234567u);
    UNIT_ASSERT_VALUES_EQUAL(topicSettings.SupportedCodecs().size(), 2u);
    UNIT_ASSERT_EQUAL(topicSettings.SupportedCodecs().at(0), ECodec::RAW);
    UNIT_ASSERT_EQUAL(topicSettings.SupportedCodecs().at(1), ECodec::GZIP);
    UNIT_ASSERT_VALUES_EQUAL(topicSettings.AllowUnauthenticatedRead(), true);
    UNIT_ASSERT_VALUES_EQUAL(topicSettings.AllowUnauthenticatedWrite(), false);
    UNIT_ASSERT(topicSettings.AbcId().has_value());
    UNIT_ASSERT_VALUES_EQUAL(*topicSettings.AbcId(), 42u);
    UNIT_ASSERT(topicSettings.AbcSlug().has_value());
    UNIT_ASSERT_VALUES_EQUAL(*topicSettings.AbcSlug(), "slug");

    UNIT_ASSERT_VALUES_EQUAL(topicSettings.ReadRules().size(), 1u);
    const auto& rule = topicSettings.ReadRules().at(0);
    UNIT_ASSERT_VALUES_EQUAL(rule.ConsumerName(), DEFAULT_STREAMING_CONSUMER);
    UNIT_ASSERT_VALUES_EQUAL(rule.Important(), true);
    UNIT_ASSERT_VALUES_EQUAL(rule.StartingMessageTimestamp(), TInstant::MilliSeconds(1000));
    UNIT_ASSERT_VALUES_EQUAL(rule.Version(), 7u);
    UNIT_ASSERT_VALUES_EQUAL(rule.SupportedCodecs().size(), 3u);
    UNIT_ASSERT_EQUAL(rule.SupportedCodecs().at(0), ECodec::RAW);
    UNIT_ASSERT_EQUAL(rule.SupportedCodecs().at(1), ECodec::GZIP);
    UNIT_ASSERT_EQUAL(rule.SupportedCodecs().at(2), ECodec::ZSTD);
    UNIT_ASSERT(!rule.SharedConsumer().has_value());
}

Y_UNIT_TEST(DescribeUnauthenticatedAttributesAbsentWhenFalse) {
    TPqv1SdkTestSetup setup("DescribeUnauthAttr");

    auto& client = setup.GetPersQueueClient();
    const std::string path = TPqv1SdkTestSetup::MakeTopicPath("topic-auth-attr");

    const auto createStatus = CreateTopicViaSdk(client, path);
    UNIT_ASSERT_C(createStatus.IsSuccess(), "CreateTopic: " << createStatus.GetIssues().ToOneLineString());

    const auto describe = DescribeTopicViaSdk(client, path);
    UNIT_ASSERT_C(describe.IsSuccess(), "DescribeTopic: " << describe.GetIssues().ToOneLineString());
    UNIT_ASSERT_VALUES_EQUAL(describe.TopicSettings().AllowUnauthenticatedRead(), false);
    UNIT_ASSERT_VALUES_EQUAL(describe.TopicSettings().AllowUnauthenticatedWrite(), false);
}

} // Y_UNIT_TEST_SUITE(DescribeTopic_PQv1SDK)

} // namespace NKikimr::NGRpcProxy::V1::NPQv1::NTests
