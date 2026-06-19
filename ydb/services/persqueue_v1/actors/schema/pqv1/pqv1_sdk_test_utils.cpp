#include "pqv1_sdk_test_utils.h"

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

TReadRuleSettings MakeSharedConsumerReadRuleSettings(const std::string& consumerName) {
    TSharedConsumerDeadLetterPolicySettings deadLetterPolicy;
    deadLetterPolicy
        .Enabled(true)
        .MaxProcessingAttempts(11)
        .DeadLetterQueue(DEFAULT_DEAD_LETTER_QUEUE);

    TSharedConsumerSettings sharedConsumer;
    sharedConsumer
        .KeepMessagesOrder(true)
        .DefaultProcessingTimeout(TDuration::Seconds(3))
        .ReceiveMessageWaitTime(TDuration::Seconds(5))
        .ReceiveMessageDelay(TDuration::Seconds(7))
        .DeadLetterPolicy(deadLetterPolicy);

    return TReadRuleSettings{}
        .ConsumerName(consumerName)
        .StartingMessageTimestamp(TInstant::MilliSeconds(1000))
        .Version(1)
        .SharedConsumer(std::make_optional(sharedConsumer));
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

TStatus AlterTopicViaSdk(
    TPersQueueClient& client,
    const std::string& path,
    const TAlterTopicSettings& settings)
{
    return client.AlterTopic(path, settings).GetValueSync();
}

} // namespace NKikimr::NGRpcProxy::V1::NPQv1::NTests
