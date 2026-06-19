#include "pqv1_sdk_test_utils.h"

#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/persqueue/public/utils.h>

#include <util/generic/size_literals.h>

namespace NKikimr::NGRpcProxy::V1::NPQv1::NTests {

namespace {

using namespace NYdb;
using namespace NYdb::NPersQueue;
using namespace NYdb::NTopic::NTests;

std::shared_ptr<TTopicSdkTestSetup> MakeBaseSetup(const char* testCaseName, bool createTopic) {
    auto setup = std::make_shared<TTopicSdkTestSetup>(testCaseName, TTopicSdkTestSetup::MakeServerSettings(), createTopic);
    setup->GetServer().EnableLogs({
            NKikimrServices::PQ_SCHEMA,
            NKikimrServices::PQ_MLP_DESCRIBER,
        },
        NActors::NLog::PRI_DEBUG
    );
    setup->GetServer().EnableLogs({
            NKikimrServices::PERSQUEUE,
            NKikimrServices::PERSQUEUE_READ_BALANCER,
            NKikimrServices::PQ_WRITE_PROXY
        },
        NActors::NLog::PRI_INFO
    );
    return setup;
}

} // namespace

TPqv1SdkTestSetup::TPqv1SdkTestSetup(const char* testCaseName, bool createTopic)
    : BaseSetup_(MakeBaseSetup(testCaseName, createTopic))
{
}

TPersQueueClient& TPqv1SdkTestSetup::GetPersQueueClient() {
    if (!Driver_) {
        Driver_ = std::make_unique<TDriver>(BaseSetup_->MakeDriverConfig());
    }
    if (!Client_) {
        Client_ = std::make_unique<TPersQueueClient>(*Driver_);
    }
    return *Client_;
}

TTopicSdkTestSetup& TPqv1SdkTestSetup::GetBaseSetup() {
    return *BaseSetup_;
}

NActors::TTestActorRuntime& TPqv1SdkTestSetup::GetRuntime() {
    return BaseSetup_->GetRuntime();
}

void TPqv1SdkTestSetup::SetTopicsAreFirstClassCitizen(bool value) {
    GetRuntime().GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(value);
}

std::string TPqv1SdkTestSetup::MakeTopicPath(const std::string& topicName) {
    return "/Root/" + topicName;
}

TExpectedTopicSettings MakeDefaultCreateTopicExpectation() {
    return TExpectedTopicSettings{};
}

TStatus CreateTopicViaSdk(
    TPersQueueClient& client,
    const std::string& path,
    const TCreateTopicSettings& settings)
{
    return client.CreateTopic(path, settings).GetValueSync();
}

TDescribeTopicResult DescribeTopicViaSdk(
    TPersQueueClient& client,
    const std::string& path)
{
    return client.DescribeTopic(path).GetValueSync();
}

void AssertStatusSuccess(const TStatus& status, const char* operation) {
    UNIT_ASSERT_C(status.IsSuccess(), operation << ": " << status.GetIssues().ToOneLineString());
}

void AssertTopicSettings(
    const TDescribeTopicResult::TTopicSettings& actual,
    const TExpectedTopicSettings& expected)
{
    UNIT_ASSERT_VALUES_EQUAL(actual.PartitionsCount(), expected.PartitionsCount);
    UNIT_ASSERT_VALUES_EQUAL(actual.RetentionPeriod(), expected.RetentionPeriod);
    UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(actual.SupportedFormat()), static_cast<int>(expected.SupportedFormat));
    UNIT_ASSERT_VALUES_EQUAL(actual.MaxPartitionStorageSize(), expected.MaxPartitionStorageSize);
    UNIT_ASSERT_VALUES_EQUAL(actual.MaxPartitionWriteSpeed(), expected.MaxPartitionWriteSpeed);
    UNIT_ASSERT_VALUES_EQUAL(actual.MaxPartitionWriteBurst(), expected.MaxPartitionWriteBurst);
    UNIT_ASSERT_VALUES_EQUAL(actual.ClientWriteDisabled(), expected.ClientWriteDisabled);
    UNIT_ASSERT_VALUES_EQUAL(actual.AllowUnauthenticatedRead(), expected.AllowUnauthenticatedRead);
    UNIT_ASSERT_VALUES_EQUAL(actual.AllowUnauthenticatedWrite(), expected.AllowUnauthenticatedWrite);

    if (expected.AbcId.has_value()) {
        UNIT_ASSERT(actual.AbcId().has_value());
        UNIT_ASSERT_VALUES_EQUAL(actual.AbcId().value(), expected.AbcId.value());
    } else {
        UNIT_ASSERT(!actual.AbcId().has_value());
    }

    if (expected.AbcSlug.has_value()) {
        UNIT_ASSERT(actual.AbcSlug().has_value());
        UNIT_ASSERT_VALUES_EQUAL(actual.AbcSlug().value(), expected.AbcSlug.value());
    } else {
        UNIT_ASSERT(!actual.AbcSlug().has_value());
    }

    if (expected.FederationAccount.has_value()) {
        UNIT_ASSERT(actual.FederationAccount().has_value());
        UNIT_ASSERT_VALUES_EQUAL(actual.FederationAccount().value(), expected.FederationAccount.value());
    } else {
        UNIT_ASSERT(!actual.FederationAccount().has_value() || actual.FederationAccount()->empty());
    }

    if (expected.MetricsLevel.has_value()) {
        UNIT_ASSERT(actual.MetricsLevel().has_value());
        UNIT_ASSERT_VALUES_EQUAL(actual.MetricsLevel().value(), expected.MetricsLevel.value());
    } else {
        UNIT_ASSERT(!actual.MetricsLevel().has_value());
    }

    if (expected.AdvancedMonitoringSettings.has_value()) {
        UNIT_ASSERT(actual.AdvancedMonitoringSettings().has_value());
        UNIT_ASSERT_VALUES_EQUAL(actual.AdvancedMonitoringSettings().value(), expected.AdvancedMonitoringSettings.value());
    } else {
        UNIT_ASSERT(!actual.AdvancedMonitoringSettings().has_value());
    }

    UNIT_ASSERT_VALUES_EQUAL(actual.ReadRules().size(), expected.ReadRules.size());
    for (size_t i = 0; i < expected.ReadRules.size(); ++i) {
        const auto& actualRule = actual.ReadRules().at(i);
        const auto& expectedRule = expected.ReadRules.at(i);

        UNIT_ASSERT_VALUES_EQUAL(actualRule.ConsumerName(), expectedRule.ConsumerName);
        UNIT_ASSERT_VALUES_EQUAL(actualRule.Important(), expectedRule.Important);
        UNIT_ASSERT_VALUES_EQUAL(actualRule.AvailabilityPeriod(), expectedRule.AvailabilityPeriod);
        UNIT_ASSERT_VALUES_EQUAL(actualRule.StartingMessageTimestamp(), expectedRule.StartingMessageTimestamp);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(actualRule.SupportedFormat()), static_cast<int>(expectedRule.SupportedFormat));
        UNIT_ASSERT_VALUES_EQUAL(actualRule.Version(), expectedRule.Version);
        if (!expectedRule.ServiceType.empty()) {
            UNIT_ASSERT_VALUES_EQUAL(actualRule.ServiceType(), expectedRule.ServiceType);
        }
    }
}

void AssertConsumerTypeViaDescriber(
    NActors::TTestActorRuntime& runtime,
    const TString& database,
    const TString& topicPath,
    const TString& consumerName,
    NKikimrPQ::TPQTabletConfig::EConsumerType expectedType)
{
    runtime.Register(NPQ::NDescriber::CreateDescriberActor(
        runtime.AllocateEdgeActor(),
        database,
        {topicPath}));
    auto response = runtime.GrabEdgeEvent<NPQ::NDescriber::TEvDescribeTopicsResponse>(TDuration::Seconds(5));

    UNIT_ASSERT_VALUES_EQUAL(response->Topics.size(), 1);
    const auto& topic = response->Topics.begin()->second;
    UNIT_ASSERT_VALUES_EQUAL(topic.Status, NPQ::NDescriber::EStatus::SUCCESS);

    const auto* consumer = NPQ::GetConsumer(topic.Info->Description.GetPQTabletConfig(), consumerName);
    UNIT_ASSERT(consumer);
    UNIT_ASSERT_VALUES_EQUAL(
        NKikimrPQ::TPQTabletConfig::EConsumerType_Name(consumer->GetType()),
        NKikimrPQ::TPQTabletConfig::EConsumerType_Name(expectedType));
}

} // namespace NKikimr::NGRpcProxy::V1::NPQv1::NTests
