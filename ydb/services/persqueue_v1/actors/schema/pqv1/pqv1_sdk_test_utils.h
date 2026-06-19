#pragma once

#include <ydb/public/sdk/cpp/src/client/persqueue_public/include/client.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/size_literals.h>

#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace NKikimr::NGRpcProxy::V1::NPQv1::NTests {

inline constexpr const char* DEFAULT_TEST_TOPIC = "topic1";

struct TExpectedTopicSettings {
    ui32 PartitionsCount = 1;
    TDuration RetentionPeriod = TDuration::Hours(18);
    NYdb::NPersQueue::EFormat SupportedFormat = NYdb::NPersQueue::EFormat::BASE;
    ui64 MaxPartitionStorageSize = 0;
    ui64 MaxPartitionWriteSpeed = 2_MB;
    ui64 MaxPartitionWriteBurst = 2_MB;
    // First-class topics don't persist LocalDC; Describe maps that to client_write_disabled=true.
    bool ClientWriteDisabled = true;
    bool AllowUnauthenticatedRead = false;
    bool AllowUnauthenticatedWrite = false;
    std::optional<ui32> PartitionsPerTablet = 2;
    std::optional<ui32> AbcId;
    std::optional<std::string> AbcSlug;
    std::optional<std::string> FederationAccount;
    std::optional<ui32> MetricsLevel;
    std::optional<std::string> AdvancedMonitoringSettings;
    size_t ReadRulesCount = 0;
};

class TPqv1SdkTestSetup {
public:
    explicit TPqv1SdkTestSetup(const char* testCaseName, bool createTopic = false);

    NYdb::NPersQueue::TPersQueueClient& GetPersQueueClient();

    NYdb::NTopic::NTests::TTopicSdkTestSetup& GetBaseSetup();
    NActors::TTestActorRuntime& GetRuntime();

    void SetTopicsAreFirstClassCitizen(bool value);

    static std::string MakeTopicPath(const std::string& topicName = DEFAULT_TEST_TOPIC);

private:
    std::shared_ptr<NYdb::NTopic::NTests::TTopicSdkTestSetup> BaseSetup_;
    std::unique_ptr<NYdb::TDriver> Driver_;
    std::unique_ptr<NYdb::NPersQueue::TPersQueueClient> Client_;
};

TExpectedTopicSettings MakeDefaultCreateTopicExpectation();

NYdb::TStatus CreateTopicViaSdk(
    NYdb::NPersQueue::TPersQueueClient& client,
    const std::string& path,
    const NYdb::NPersQueue::TCreateTopicSettings& settings = {});

NYdb::NPersQueue::TDescribeTopicResult DescribeTopicViaSdk(
    NYdb::NPersQueue::TPersQueueClient& client,
    const std::string& path);

void AssertStatusSuccess(const NYdb::TStatus& status, const char* operation);

void AssertTopicSettings(
    const NYdb::NPersQueue::TDescribeTopicResult::TTopicSettings& actual,
    const TExpectedTopicSettings& expected);

} // namespace NKikimr::NGRpcProxy::V1::NPQv1::NTests
