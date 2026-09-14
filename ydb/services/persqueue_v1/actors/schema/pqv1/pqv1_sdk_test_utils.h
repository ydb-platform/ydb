#pragma once

#include <ydb/public/sdk/cpp/src/client/persqueue_public/include/client.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <memory>
#include <string>

namespace NKikimr::NGRpcProxy::V1::NPQv1::NTests {

inline constexpr const char* DEFAULT_TEST_TOPIC = "topic1";
inline constexpr const char* DEFAULT_STREAMING_CONSUMER = "test_consumer";
inline constexpr const char* DEFAULT_SHARED_CONSUMER = "test_shared_consumer";
inline constexpr const char* DEFAULT_DEAD_LETTER_QUEUE = "test_dead_letter_queue";

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

NYdb::NPersQueue::TReadRuleSettings MakeSharedConsumerReadRuleSettings(
    const std::string& consumerName = DEFAULT_SHARED_CONSUMER,
    NYdb::NPersQueue::TSharedConsumerDeadLetterPolicySettings::EAction deadLetterAction =
        NYdb::NPersQueue::TSharedConsumerDeadLetterPolicySettings::EAction::Move,
    const std::string& deadLetterQueue = DEFAULT_DEAD_LETTER_QUEUE);

NYdb::TStatus CreateTopicViaSdk(
    NYdb::NPersQueue::TPersQueueClient& client,
    const std::string& path,
    const NYdb::NPersQueue::TCreateTopicSettings& settings = {});

NYdb::TStatus CreateDlqTopicViaSdk(
    NYdb::NPersQueue::TPersQueueClient& client,
    const std::string& dlqTopicName = DEFAULT_DEAD_LETTER_QUEUE);

NYdb::NPersQueue::TDescribeTopicResult DescribeTopicViaSdk(
    NYdb::NPersQueue::TPersQueueClient& client,
    const std::string& path);

NYdb::TStatus AlterTopicViaSdk(
    NYdb::NPersQueue::TPersQueueClient& client,
    const std::string& path,
    const NYdb::NPersQueue::TAlterTopicSettings& settings = {});

} // namespace NKikimr::NGRpcProxy::V1::NPQv1::NTests
