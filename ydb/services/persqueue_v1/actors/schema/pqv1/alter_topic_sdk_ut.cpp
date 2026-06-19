#include "pqv1_sdk_test_utils.h"

#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/persqueue/public/utils.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/size_literals.h>

namespace NKikimr::NGRpcProxy::V1::NPQv1::NTests {

using namespace NYdb;
using namespace NYdb::NPersQueue;

Y_UNIT_TEST_SUITE(AlterTopic_PQv1SDK) {

Y_UNIT_TEST(AddStreamingConsumer) {
    TPqv1SdkTestSetup setup("AddStreamingConsumer");

    auto& client = setup.GetPersQueueClient();
    const std::string path = TPqv1SdkTestSetup::MakeTopicPath("topic-alter-streaming");

    const auto createStatus = CreateTopicViaSdk(client, path);
    UNIT_ASSERT_C(createStatus.IsSuccess(), "CreateTopic: " << createStatus.GetIssues().ToOneLineString());

    {
        const auto describe = DescribeTopicViaSdk(client, path);
        UNIT_ASSERT_C(describe.IsSuccess(), "DescribeTopic: " << describe.GetIssues().ToOneLineString());
        UNIT_ASSERT_VALUES_EQUAL(describe.TopicSettings().ReadRules().size(), 0u);
    }

    TAlterTopicSettings alterSettings;
    alterSettings.ReadRules({TReadRuleSettings{}.ConsumerName(DEFAULT_STREAMING_CONSUMER)});

    const auto alterStatus = AlterTopicViaSdk(client, path, alterSettings);
    UNIT_ASSERT_C(alterStatus.IsSuccess(), "AlterTopic: " << alterStatus.GetIssues().ToOneLineString());

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

Y_UNIT_TEST(AddSharedConsumer) {
    TPqv1SdkTestSetup setup("AddSharedConsumer");

    auto& client = setup.GetPersQueueClient();
    const std::string path = TPqv1SdkTestSetup::MakeTopicPath("topic-alter-shared");

    const auto createStatus = CreateTopicViaSdk(client, path);
    UNIT_ASSERT_C(createStatus.IsSuccess(), "CreateTopic: " << createStatus.GetIssues().ToOneLineString());

    {
        const auto describe = DescribeTopicViaSdk(client, path);
        UNIT_ASSERT_C(describe.IsSuccess(), "DescribeTopic: " << describe.GetIssues().ToOneLineString());
        UNIT_ASSERT_VALUES_EQUAL(describe.TopicSettings().ReadRules().size(), 0u);
    }

    TAlterTopicSettings alterSettings;
    alterSettings.ReadRules({MakeSharedConsumerReadRuleSettings()});

    const auto alterStatus = AlterTopicViaSdk(client, path, alterSettings);
    UNIT_ASSERT_C(alterStatus.IsSuccess(), "AlterTopic: " << alterStatus.GetIssues().ToOneLineString());

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

Y_UNIT_TEST(CannotChangeConsumerType) {
    TPqv1SdkTestSetup setup("CannotChangeConsumerType");

    auto& client = setup.GetPersQueueClient();
    const std::string path = TPqv1SdkTestSetup::MakeTopicPath("topic-alter-change-type");

    TCreateTopicSettings createSettings;
    createSettings.ReadRules({TReadRuleSettings{}.ConsumerName(DEFAULT_STREAMING_CONSUMER)});

    const auto createStatus = CreateTopicViaSdk(client, path, createSettings);
    UNIT_ASSERT_C(createStatus.IsSuccess(), "CreateTopic: " << createStatus.GetIssues().ToOneLineString());

    {
        const auto describe = DescribeTopicViaSdk(client, path);
        UNIT_ASSERT_C(describe.IsSuccess(), "DescribeTopic: " << describe.GetIssues().ToOneLineString());
        UNIT_ASSERT_VALUES_EQUAL(describe.TopicSettings().ReadRules().size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(describe.TopicSettings().ReadRules().at(0).ConsumerName(), DEFAULT_STREAMING_CONSUMER);
    }

    TAlterTopicSettings alterSettings;
    alterSettings.ReadRules({MakeSharedConsumerReadRuleSettings(DEFAULT_STREAMING_CONSUMER)});

    const auto alterStatus = AlterTopicViaSdk(client, path, alterSettings);
    const auto issue = alterStatus.GetIssues().ToOneLineString();
    UNIT_ASSERT_C(!alterStatus.IsSuccess(), issue);
    UNIT_ASSERT_C(issue.contains("Cannot alter consumer type"), issue);
}

} // Y_UNIT_TEST_SUITE(AlterTopic_PQv1SDK)

} // namespace NKikimr::NGRpcProxy::V1::NPQv1::NTests
