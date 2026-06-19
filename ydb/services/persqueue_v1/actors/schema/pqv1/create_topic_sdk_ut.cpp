#include "pqv1_sdk_test_utils.h"

#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/persqueue/public/utils.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/size_literals.h>

namespace NKikimr::NGRpcProxy::V1::NPQv1::NTests {

using namespace NYdb;
using namespace NYdb::NPersQueue;

Y_UNIT_TEST_SUITE(CreateTopic_PQv1SDK) {

Y_UNIT_TEST(CreateTopic) {
    TPqv1SdkTestSetup setup("CreateTopic");

    auto& client = setup.GetPersQueueClient();
    const std::string path = TPqv1SdkTestSetup::MakeTopicPath();

    TCreateTopicSettings settings;

    const auto createStatus = CreateTopicViaSdk(client, path, settings);
    UNIT_ASSERT_C(createStatus.IsSuccess(), "CreateTopic: " << createStatus.GetIssues().ToOneLineString());

    const auto describe = DescribeTopicViaSdk(client, path);
    UNIT_ASSERT_C(describe.IsSuccess(), "DescribeTopic: " << describe.GetIssues().ToOneLineString());

    const auto& topicSettings = describe.TopicSettings();
    UNIT_ASSERT_VALUES_EQUAL(topicSettings.PartitionsCount(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(topicSettings.RetentionPeriod(), TDuration::Hours(18));
    UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(topicSettings.SupportedFormat()), static_cast<int>(EFormat::BASE));
    UNIT_ASSERT_VALUES_EQUAL(topicSettings.MaxPartitionStorageSize(), 0u);
    UNIT_ASSERT_VALUES_EQUAL(topicSettings.MaxPartitionWriteSpeed(), 2_MB);
    UNIT_ASSERT_VALUES_EQUAL(topicSettings.MaxPartitionWriteBurst(), 2_MB);
    UNIT_ASSERT_VALUES_EQUAL(topicSettings.ClientWriteDisabled(), true);
    UNIT_ASSERT_VALUES_EQUAL(topicSettings.AllowUnauthenticatedRead(), false);
    UNIT_ASSERT_VALUES_EQUAL(topicSettings.AllowUnauthenticatedWrite(), false);
    UNIT_ASSERT(!topicSettings.AbcId().has_value());
    UNIT_ASSERT(!topicSettings.AbcSlug().has_value());
    UNIT_ASSERT(!topicSettings.FederationAccount().has_value() || topicSettings.FederationAccount()->empty());
    UNIT_ASSERT(!topicSettings.MetricsLevel().has_value());
    UNIT_ASSERT(!topicSettings.AdvancedMonitoringSettings().has_value());
    UNIT_ASSERT_VALUES_EQUAL(topicSettings.ReadRules().size(), 0u);
}

Y_UNIT_TEST(CreateTopicWithStreamingConsumer) {
    TPqv1SdkTestSetup setup("CreateTopicWithStreamingConsumer");

    auto& client = setup.GetPersQueueClient();
    const std::string path = TPqv1SdkTestSetup::MakeTopicPath("topic-streaming");

    TCreateTopicSettings settings;
    settings.ReadRules({TReadRuleSettings{}.ConsumerName(DEFAULT_STREAMING_CONSUMER)});

    const auto createStatus = CreateTopicViaSdk(client, path, settings);
    UNIT_ASSERT_C(createStatus.IsSuccess(), "CreateTopic: " << createStatus.GetIssues().ToOneLineString());

    const auto describe = DescribeTopicViaSdk(client, path);
    UNIT_ASSERT_C(describe.IsSuccess(), "DescribeTopic: " << describe.GetIssues().ToOneLineString());

    const auto& topicSettings = describe.TopicSettings();
    UNIT_ASSERT_VALUES_EQUAL(topicSettings.ReadRules().size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(topicSettings.ReadRules().at(0).ConsumerName(), DEFAULT_STREAMING_CONSUMER);
    UNIT_ASSERT_VALUES_EQUAL(topicSettings.ReadRules().at(0).Important(), false);
    UNIT_ASSERT_VALUES_EQUAL(topicSettings.ReadRules().at(0).AvailabilityPeriod(), TDuration::Zero());
    UNIT_ASSERT_VALUES_EQUAL(topicSettings.ReadRules().at(0).StartingMessageTimestamp(), TInstant::Zero());
    UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(topicSettings.ReadRules().at(0).SupportedFormat()), static_cast<int>(EFormat::BASE));
    UNIT_ASSERT_VALUES_EQUAL(topicSettings.ReadRules().at(0).Version(), 0u);
    UNIT_ASSERT(!topicSettings.ReadRules().at(0).SharedConsumer().has_value());

    auto& runtime = setup.GetRuntime();
    runtime.Register(NPQ::NDescriber::CreateDescriberActor(
        runtime.AllocateEdgeActor(),
        TString(setup.GetBaseSetup().GetDatabase()),
        {TString(path)}));
    auto response = runtime.GrabEdgeEvent<NPQ::NDescriber::TEvDescribeTopicsResponse>(TDuration::Seconds(5));

    UNIT_ASSERT_VALUES_EQUAL(response->Topics.size(), 1u);
    const auto& topic = response->Topics.begin()->second;
    UNIT_ASSERT_VALUES_EQUAL(topic.Status, NPQ::NDescriber::EStatus::SUCCESS);

    const auto* consumer = NPQ::GetConsumer(topic.Info->Description.GetPQTabletConfig(), DEFAULT_STREAMING_CONSUMER);
    UNIT_ASSERT(consumer);
    UNIT_ASSERT_VALUES_EQUAL(
        NKikimrPQ::TPQTabletConfig::EConsumerType_Name(consumer->GetType()),
        NKikimrPQ::TPQTabletConfig::EConsumerType_Name(NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_STREAMING));
}

Y_UNIT_TEST(CreateTopicWithSharedConsumer) {
    TPqv1SdkTestSetup setup("CreateTopicWithSharedConsumer");

    auto& client = setup.GetPersQueueClient();
    const std::string path = TPqv1SdkTestSetup::MakeTopicPath("topic-shared");

    TCreateTopicSettings settings;
    settings.ReadRules({MakeSharedConsumerReadRuleSettings()});

    const auto createStatus = CreateTopicViaSdk(client, path, settings);
    UNIT_ASSERT_C(createStatus.IsSuccess(), "CreateTopic: " << createStatus.GetIssues().ToOneLineString());

    const auto describe = DescribeTopicViaSdk(client, path);
    UNIT_ASSERT_C(describe.IsSuccess(), "DescribeTopic: " << describe.GetIssues().ToOneLineString());

    const auto& topicSettings = describe.TopicSettings();
    UNIT_ASSERT_VALUES_EQUAL(topicSettings.ReadRules().size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(topicSettings.ReadRules().at(0).ConsumerName(), DEFAULT_SHARED_CONSUMER);
    UNIT_ASSERT_VALUES_EQUAL(topicSettings.ReadRules().at(0).Important(), false);
    UNIT_ASSERT_VALUES_EQUAL(topicSettings.ReadRules().at(0).AvailabilityPeriod(), TDuration::Zero());
    UNIT_ASSERT_VALUES_EQUAL(topicSettings.ReadRules().at(0).StartingMessageTimestamp(), TInstant::MilliSeconds(1000));
    UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(topicSettings.ReadRules().at(0).SupportedFormat()), static_cast<int>(EFormat::BASE));
    UNIT_ASSERT_VALUES_EQUAL(topicSettings.ReadRules().at(0).Version(), 1u);

    auto& runtime = setup.GetRuntime();
    runtime.Register(NPQ::NDescriber::CreateDescriberActor(
        runtime.AllocateEdgeActor(),
        TString(setup.GetBaseSetup().GetDatabase()),
        {TString(path)}));
    auto response = runtime.GrabEdgeEvent<NPQ::NDescriber::TEvDescribeTopicsResponse>(TDuration::Seconds(5));

    UNIT_ASSERT_VALUES_EQUAL(response->Topics.size(), 1u);
    const auto& topic = response->Topics.begin()->second;
    UNIT_ASSERT_VALUES_EQUAL(topic.Status, NPQ::NDescriber::EStatus::SUCCESS);

    const auto& config = topic.Info->Description.GetPQTabletConfig();
    const auto* consumer = NPQ::GetConsumer(config, DEFAULT_SHARED_CONSUMER);
    UNIT_ASSERT(consumer);
    UNIT_ASSERT_VALUES_EQUAL(consumer->GetImportant(), false);
    UNIT_ASSERT_VALUES_EQUAL(
        NKikimrPQ::TPQTabletConfig::EConsumerType_Name(consumer->GetType()),
        NKikimrPQ::TPQTabletConfig::EConsumerType_Name(NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP));
    UNIT_ASSERT_VALUES_EQUAL(consumer->GetKeepMessageOrder(), true);
    UNIT_ASSERT_VALUES_EQUAL(consumer->GetDefaultProcessingTimeoutSeconds(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(consumer->GetDefaultReceiveMessageWaitTimeMs(), 5000u);
    UNIT_ASSERT_VALUES_EQUAL(consumer->GetDefaultDelayMessageTimeMs(), 7000u);
    UNIT_ASSERT_VALUES_EQUAL(
        NKikimrPQ::TPQTabletConfig::EDeadLetterPolicy_Name(consumer->GetDeadLetterPolicy()),
        NKikimrPQ::TPQTabletConfig::EDeadLetterPolicy_Name(NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_MOVE));
    UNIT_ASSERT_VALUES_EQUAL(consumer->GetMaxProcessingAttempts(), 11u);
    UNIT_ASSERT_VALUES_EQUAL(consumer->GetDeadLetterQueue(), DEFAULT_DEAD_LETTER_QUEUE);
}

Y_UNIT_TEST(DescribeTopicReturnsSharedConsumerSettings) {
    TPqv1SdkTestSetup setup("DescribeTopicReturnsSharedConsumerSettings");

    auto& client = setup.GetPersQueueClient();
    const std::string path = TPqv1SdkTestSetup::MakeTopicPath("topic-describe-shared");

    TCreateTopicSettings settings;
    settings.ReadRules({MakeSharedConsumerReadRuleSettings()});

    const auto createStatus = CreateTopicViaSdk(client, path, settings);
    UNIT_ASSERT_C(createStatus.IsSuccess(), "CreateTopic: " << createStatus.GetIssues().ToOneLineString());

    const auto describe = DescribeTopicViaSdk(client, path);
    UNIT_ASSERT_C(describe.IsSuccess(), "DescribeTopic: " << describe.GetIssues().ToOneLineString());

    const auto& readRule = describe.TopicSettings().ReadRules().at(0);
    UNIT_ASSERT(readRule.SharedConsumer().has_value());

    const auto sharedConsumer = readRule.SharedConsumer().value();
    UNIT_ASSERT_VALUES_EQUAL(sharedConsumer.GetKeepMessagesOrder(), true);
    UNIT_ASSERT_VALUES_EQUAL(sharedConsumer.GetDefaultProcessingTimeout(), TDuration::Seconds(3));
    UNIT_ASSERT_VALUES_EQUAL(sharedConsumer.GetReceiveMessageWaitTime(), TDuration::Seconds(5));
    UNIT_ASSERT_VALUES_EQUAL(sharedConsumer.GetReceiveMessageDelay(), TDuration::Seconds(7));

    const auto& deadLetterPolicy = sharedConsumer.GetDeadLetterPolicy();
    UNIT_ASSERT_VALUES_EQUAL(deadLetterPolicy.GetEnabled(), true);
    UNIT_ASSERT_VALUES_EQUAL(deadLetterPolicy.GetMaxProcessingAttempts(), 11u);
    UNIT_ASSERT_VALUES_EQUAL(deadLetterPolicy.GetDeadLetterQueue(), DEFAULT_DEAD_LETTER_QUEUE);

    TReadRuleSettings roundTrip;
    roundTrip.SetSettings(readRule);
    UNIT_ASSERT(roundTrip.GetSharedConsumer().has_value());
    UNIT_ASSERT_VALUES_EQUAL(roundTrip.GetSharedConsumer()->GetKeepMessagesOrder(), true);
    UNIT_ASSERT_VALUES_EQUAL(roundTrip.GetSharedConsumer()->GetDefaultProcessingTimeout(), TDuration::Seconds(3));
    UNIT_ASSERT_VALUES_EQUAL(roundTrip.GetSharedConsumer()->GetDeadLetterPolicy().GetDeadLetterQueue(), DEFAULT_DEAD_LETTER_QUEUE);
}

} // Y_UNIT_TEST_SUITE(CreateTopic_PQv1SDK)

} // namespace NKikimr::NGRpcProxy::V1::NPQv1::NTests
