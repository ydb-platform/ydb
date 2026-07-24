#include "ut_utils/topic_sdk_test_setup.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/deferred_publications.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/exceptions/exceptions.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/tx/tx.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYdb;
using namespace NYdb::NTopic;
using namespace NYdb::NTopic::NTests;

namespace {

NKikimr::Tests::TServerSettings MakeDeferredPublishEnabledSettings() {
    auto settings = TTopicSdkTestSetup::MakeServerSettings();
    settings.SetEnableTopicDeferredPublish(true);
    settings.PQConfig.SetCheckACL(false);
    return settings;
}

NTopic::TContinuationToken WaitForWriteToken(NTopic::IWriteSession& session) {
    std::optional<NTopic::TContinuationToken> token;
    while (!token.has_value()) {
        UNIT_ASSERT_C(
            session.WaitEvent().Wait(TDuration::Seconds(30)),
            "timeout waiting write continuation token");
        for (auto& event : session.GetEvents()) {
            if (auto* ready = std::get_if<NTopic::TWriteSessionEvent::TReadyToAcceptEvent>(&event)) {
                token = std::move(ready->ContinuationToken);
            } else if (auto* closed = std::get_if<NTopic::TSessionClosedEvent>(&event)) {
                UNIT_FAIL("Write session closed unexpectedly: " << closed->GetIssues().ToString());
            }
        }
    }
    return std::move(*token);
}

void WaitForWriteAcks(NTopic::IWriteSession& session, size_t expectedAcks = 1) {
    size_t acks = 0;
    while (acks < expectedAcks) {
        UNIT_ASSERT_C(
            session.WaitEvent().Wait(TDuration::Seconds(30)),
            "timeout waiting write acks");
        for (auto& event : session.GetEvents()) {
            if (std::holds_alternative<NTopic::TWriteSessionEvent::TAcksEvent>(event)) {
                ++acks;
            } else if (auto* closed = std::get_if<NTopic::TSessionClosedEvent>(&event)) {
                UNIT_FAIL("Write session closed unexpectedly: " << closed->GetIssues().ToString());
            }
        }
    }
}

class TDeferredWriteHelper {
public:
    explicit TDeferredWriteHelper(NTopic::TTopicClient& client, const std::string& topicPath)
        : Client_(client)
    {
        Settings_.Path(topicPath);
        Settings_.ProducerId("deferred-producer");
        Session_ = Client_.CreateWriteSession(Settings_);
    }

    ~TDeferredWriteHelper() {
        Close(TDuration::Seconds(10));
    }

    void WriteDeferred(const std::string& payload, const NTopic::TDeferredPublication& publication) {
        Token_ = WaitForWriteToken(*Session_);
        NTopic::TWriteMessage message(payload);
        message.DeferredPublication(publication);
        Session_->Write(std::move(*Token_), std::move(message));
        Token_.reset();
    }

    void Close(TDuration timeout = TDuration::Seconds(10)) {
        if (Session_) {
            Session_->Close(timeout);
            Session_.reset();
        }
    }

    NTopic::IWriteSession& Session() {
        return *Session_;
    }

private:
    NTopic::TTopicClient& Client_;
    NTopic::TWriteSessionSettings Settings_;
    std::shared_ptr<NTopic::IWriteSession> Session_;
    std::optional<NTopic::TContinuationToken> Token_;
};

struct TReadHelperState {
    std::vector<std::string> Messages;
    NThreading::TPromise<void> Done = NThreading::NewPromise<void>();
};

std::vector<std::string> ReadMessages(
    NTopic::TTopicClient& client,
    const std::string& topicPath,
    const std::string& consumerName,
    size_t expectedCount,
    TDuration timeout = TDuration::Seconds(30))
{
    // Shared so the handler outlives Wait/Close: TrySetValue wakes the waiter while the
    // callback may still be running on the handlers executor.
    auto state = std::make_shared<TReadHelperState>();

    NTopic::TReadSessionSettings settings;
    settings.ConsumerName(consumerName);
    settings.AppendTopics({NTopic::TTopicReadSettings(topicPath).ReadFromTimestamp(TInstant::Zero())});
    settings.EventHandlers_.SimpleDataHandlers(
        [state, expectedCount](NTopic::TReadSessionEvent::TDataReceivedEvent& event) {
        for (const auto& message : event.GetMessages()) {
            state->Messages.emplace_back(TString(message.GetData()));
        }
        if (state->Messages.size() >= expectedCount) {
            state->Done.TrySetValue();
        }
    }, true);

    auto session = client.CreateReadSession(settings);
    UNIT_ASSERT(state->Done.GetFuture().Wait(timeout));
    session->Close(TDuration::Seconds(5));
    return state->Messages;
}

std::vector<std::string> ReadNoMessages(
    NTopic::TTopicClient& client,
    const std::string& topicPath,
    const std::string& consumerName,
    TDuration timeout = TDuration::Seconds(5))
{
    auto state = std::make_shared<TReadHelperState>();

    NTopic::TReadSessionSettings settings;
    settings.ConsumerName(consumerName);
    settings.AppendTopics({NTopic::TTopicReadSettings(topicPath).ReadFromTimestamp(TInstant::Zero())});
    settings.EventHandlers_.SimpleDataHandlers(
        [state](NTopic::TReadSessionEvent::TDataReceivedEvent& event) {
        for (const auto& message : event.GetMessages()) {
            state->Messages.emplace_back(TString(message.GetData()));
        }
        state->Done.TrySetValue();
    }, true);

    auto session = client.CreateReadSession(settings);
    state->Done.GetFuture().Wait(timeout);
    session->Close(TDuration::Seconds(5));
    return state->Messages;
}

} // namespace

Y_UNIT_TEST_SUITE(TopicDeferredPublishSdkClient) {

Y_UNIT_TEST(BeginPublicationRejectsEmptyExtPublicationId) {
    TDriverConfig config;
    TDriver driver(config);
    TDeferredPublishClient client(driver);

    auto result = client.BeginPublication("").GetValueSync();
    UNIT_ASSERT(!result.IsSuccess());
    UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
    UNIT_ASSERT_EXCEPTION(result.GetPublication(), TContractViolation);
    UNIT_ASSERT_EXCEPTION(result.GetIntPublicationId(), TContractViolation);
}

Y_UNIT_TEST(BeginPublicationDisabledByDefault) {
    TTopicSdkTestSetup setup("BeginPublicationDisabledByDefault");
    TDriver driver(setup.MakeDriverConfig());
    TDeferredPublishClient client(driver);

    auto result = client.BeginPublication("ext-disabled").GetValueSync();
    UNIT_ASSERT(!result.IsSuccess());
    UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNSUPPORTED);
    UNIT_ASSERT_EXCEPTION(result.GetPublication(), TContractViolation);
    UNIT_ASSERT_EXCEPTION(result.GetIntPublicationId(), TContractViolation);
}

Y_UNIT_TEST(BeginPublicationCreatesPublication) {
    TTopicSdkTestSetup setup("BeginPublicationCreatesPublication", MakeDeferredPublishEnabledSettings());
    TDriver driver(setup.MakeDriverConfig());
    TDeferredPublishClient client(driver);

    const std::string extId = "ext-sdk-begin";
    auto begin = client.BeginPublication(extId).GetValueSync();
    UNIT_ASSERT_C(begin.IsSuccess(), begin.GetIssues().ToString());
    UNIT_ASSERT_GT(begin.GetIntPublicationId(), 0u);
    UNIT_ASSERT_VALUES_EQUAL(begin.GetPublication().IntPublicationId, begin.GetIntPublicationId());
    UNIT_ASSERT(begin.GetPublication().ExtPublicationId.has_value());
    UNIT_ASSERT_VALUES_EQUAL(*begin.GetPublication().ExtPublicationId, extId);

    auto list = client.ListPublications().GetValueSync();
    UNIT_ASSERT_C(list.IsSuccess(), list.GetIssues().ToString());
    UNIT_ASSERT_VALUES_EQUAL(list.GetPublications().size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(list.GetPublications()[0].ExtPublicationId, extId);
    UNIT_ASSERT_VALUES_EQUAL(list.GetPublications()[0].IntPublicationId, begin.GetIntPublicationId());
}

Y_UNIT_TEST(DescribePublicationByHandle) {
    TTopicSdkTestSetup setup("DescribePublicationByHandle", MakeDeferredPublishEnabledSettings());
    TDriver driver(setup.MakeDriverConfig());
    NTopic::TTopicClient topicClient(driver);
    TDeferredPublishClient deferredClient(driver);

    auto zeroId = deferredClient.DescribePublication(NTopic::TDeferredPublication(0)).GetValueSync();
    UNIT_ASSERT(!zeroId.IsSuccess());
    UNIT_ASSERT_VALUES_EQUAL(zeroId.GetStatus(), EStatus::BAD_REQUEST);

    const std::string extId = "ext-sdk-describe";
    const std::string payload = "sdk-describe-payload";
    const auto topicPath = setup.GetFullTopicPath();

    auto begin = deferredClient.BeginPublication(extId).GetValueSync();
    UNIT_ASSERT_C(begin.IsSuccess(), begin.GetIssues().ToString());
    const auto& publication = begin.GetPublication();

    auto describe = deferredClient.DescribePublication(publication).GetValueSync();
    UNIT_ASSERT_C(describe.IsSuccess(), describe.GetIssues().ToString());
    UNIT_ASSERT_VALUES_EQUAL(describe.GetPublication().ExtPublicationId, extId);
    UNIT_ASSERT(describe.GetPublication().CreatedAt != TInstant::Zero());

    // Cold handle with the same int id also works (Describe does not use ack state).
    auto describeCold = deferredClient.DescribePublication(
        NTopic::TDeferredPublication(publication.IntPublicationId)).GetValueSync();
    UNIT_ASSERT_C(describeCold.IsSuccess(), describeCold.GetIssues().ToString());
    UNIT_ASSERT_VALUES_EQUAL(describeCold.GetPublication().ExtPublicationId, extId);

    TDeferredWriteHelper writer(topicClient, topicPath);
    writer.WriteDeferred(payload, publication);
    auto publish = deferredClient.Publish(publication).GetValueSync();
    UNIT_ASSERT_C(publish.IsSuccess(), publish.GetIssues().ToString());

    auto afterPublish = deferredClient.DescribePublication(publication).GetValueSync();
    UNIT_ASSERT(!afterPublish.IsSuccess());
    UNIT_ASSERT_VALUES_EQUAL(afterPublish.GetStatus(), EStatus::NOT_FOUND);
}

Y_UNIT_TEST(PublishMakesDataVisible) {
    TTopicSdkTestSetup setup("PublishMakesDataVisible", MakeDeferredPublishEnabledSettings());
    TDriver driver(setup.MakeDriverConfig());
    NTopic::TTopicClient topicClient(driver);
    TDeferredPublishClient deferredClient(driver);

    const std::string extId = "ext-sdk-publish";
    const std::string payload = "sdk-deferred-payload";
    const auto topicPath = setup.GetFullTopicPath();

    auto begin = deferredClient.BeginPublication(extId).GetValueSync();
    UNIT_ASSERT_C(begin.IsSuccess(), begin.GetIssues().ToString());
    const auto& publication = begin.GetPublication();

    // Keep write session alive across Publish: no explicit ack wait; Publish waits internally.
    TDeferredWriteHelper writer(topicClient, topicPath);
    writer.WriteDeferred(payload, publication);

    auto publish = deferredClient.Publish(publication).GetValueSync();
    UNIT_ASSERT_C(publish.IsSuccess(), publish.GetIssues().ToString());

    const auto messages = ReadMessages(topicClient, topicPath, TEST_CONSUMER, 1);
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(messages[0], payload);
}

Y_UNIT_TEST(StreamWriteAllowsOmitExtPublicationId) {
    TTopicSdkTestSetup setup("StreamWriteAllowsOmitExtPublicationId", MakeDeferredPublishEnabledSettings());
    TDriver driver(setup.MakeDriverConfig());
    NTopic::TTopicClient topicClient(driver);
    TDeferredPublishClient deferredClient(driver);

    const std::string extId = "ext-sdk-omit-ext";
    const std::string payload = "sdk-omit-ext-payload";
    const auto topicPath = setup.GetFullTopicPath();

    auto begin = deferredClient.BeginPublication(extId).GetValueSync();
    UNIT_ASSERT_C(begin.IsSuccess(), begin.GetIssues().ToString());
    const auto& publication = begin.GetPublication();

    NTopic::TDeferredPublication writePublication = publication;
    writePublication.ExtPublicationId.reset();

    TDeferredWriteHelper writer(topicClient, topicPath);
    writer.WriteDeferred(payload, writePublication);

    auto publish = deferredClient.Publish(publication).GetValueSync();
    UNIT_ASSERT_C(publish.IsSuccess(), publish.GetIssues().ToString());

    const auto messages = ReadMessages(topicClient, topicPath, TEST_CONSUMER, 1);
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(messages[0], payload);
}

Y_UNIT_TEST(StreamWriteAllowsEmptyAndAlternateExtPublicationId) {
    TTopicSdkTestSetup setup("StreamWriteAllowsEmptyAndAlternateExtPublicationId", MakeDeferredPublishEnabledSettings());
    TDriver driver(setup.MakeDriverConfig());
    NTopic::TTopicClient topicClient(driver);
    TDeferredPublishClient deferredClient(driver);

    const std::string extId = "ext-sdk-empty-and-alt";
    const std::string payloadAlt = "sdk-alt-ext-payload";
    const std::string payloadEmpty = "sdk-empty-ext-payload";
    const auto topicPath = setup.GetFullTopicPath();

    auto begin = deferredClient.BeginPublication(extId).GetValueSync();
    UNIT_ASSERT_C(begin.IsSuccess(), begin.GetIssues().ToString());
    const auto& publication = begin.GetPublication();

    NTopic::TDeferredPublication writeAlt = publication;
    writeAlt.ExtPublicationId = "other-valid-ext";
    NTopic::TDeferredPublication writeEmpty = publication;
    writeEmpty.ExtPublicationId = "";

    TDeferredWriteHelper writer(topicClient, topicPath);
    writer.WriteDeferred(payloadAlt, writeAlt);
    writer.WriteDeferred(payloadEmpty, writeEmpty);

    auto publish = deferredClient.Publish(publication).GetValueSync();
    UNIT_ASSERT_C(publish.IsSuccess(), publish.GetIssues().ToString());

    const auto messages = ReadMessages(topicClient, topicPath, TEST_CONSUMER, 2);
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(messages[0], payloadAlt);
    UNIT_ASSERT_VALUES_EQUAL(messages[1], payloadEmpty);
}

Y_UNIT_TEST(CancelDiscardsData) {
    TTopicSdkTestSetup setup("CancelDiscardsData", MakeDeferredPublishEnabledSettings());
    TDriver driver(setup.MakeDriverConfig());
    NTopic::TTopicClient topicClient(driver);
    TDeferredPublishClient deferredClient(driver);

    const std::string extId = "ext-sdk-cancel";
    const std::string payload = "sdk-cancel-payload";
    const auto topicPath = setup.GetFullTopicPath();

    auto begin = deferredClient.BeginPublication(extId).GetValueSync();
    UNIT_ASSERT_C(begin.IsSuccess(), begin.GetIssues().ToString());
    const auto& publication = begin.GetPublication();

    TDeferredWriteHelper writer(topicClient, topicPath);
    writer.WriteDeferred(payload, publication);

    auto cancel = deferredClient.CancelPublication(publication).GetValueSync();
    UNIT_ASSERT_C(cancel.IsSuccess(), cancel.GetIssues().ToString());

    const auto messages = ReadNoMessages(topicClient, topicPath, TEST_CONSUMER);
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 0u);
}

Y_UNIT_TEST(StagingNotVisibleBeforePublish) {
    TTopicSdkTestSetup setup("StagingNotVisibleBeforePublish", MakeDeferredPublishEnabledSettings());
    TDriver driver(setup.MakeDriverConfig());
    NTopic::TTopicClient topicClient(driver);
    TDeferredPublishClient deferredClient(driver);

    const std::string extId = "ext-sdk-staging";
    const std::string payload = "sdk-staging-payload";
    const auto topicPath = setup.GetFullTopicPath();

    auto begin = deferredClient.BeginPublication(extId).GetValueSync();
    UNIT_ASSERT_C(begin.IsSuccess(), begin.GetIssues().ToString());
    const auto& publication = begin.GetPublication();

    TDeferredWriteHelper writer(topicClient, topicPath);
    writer.WriteDeferred(payload, publication);

    const auto messagesBeforePublish = ReadNoMessages(topicClient, topicPath, TEST_CONSUMER);
    UNIT_ASSERT_VALUES_EQUAL(messagesBeforePublish.size(), 0u);

    auto publish = deferredClient.Publish(publication).GetValueSync();
    UNIT_ASSERT_C(publish.IsSuccess(), publish.GetIssues().ToString());

    const auto messagesAfterPublish = ReadMessages(topicClient, topicPath, TEST_CONSUMER, 1);
    UNIT_ASSERT_VALUES_EQUAL(messagesAfterPublish.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(messagesAfterPublish[0], payload);
}

Y_UNIT_TEST(RepeatFinalizeNotFound) {
    TTopicSdkTestSetup setup("RepeatFinalizeNotFound", MakeDeferredPublishEnabledSettings());
    TDriver driver(setup.MakeDriverConfig());
    NTopic::TTopicClient topicClient(driver);
    TDeferredPublishClient deferredClient(driver);

    const std::string extId = "ext-sdk-repeat-finalize";
    const auto topicPath = setup.GetFullTopicPath();

    auto begin = deferredClient.BeginPublication(extId).GetValueSync();
    UNIT_ASSERT_C(begin.IsSuccess(), begin.GetIssues().ToString());
    const auto& publication = begin.GetPublication();

    TDeferredWriteHelper writer(topicClient, topicPath);
    writer.WriteDeferred("payload", publication);

    auto publish = deferredClient.Publish(publication).GetValueSync();
    UNIT_ASSERT_C(publish.IsSuccess(), publish.GetIssues().ToString());

    auto repeatPublish = deferredClient.Publish(publication).GetValueSync();
    UNIT_ASSERT(!repeatPublish.IsSuccess());
    UNIT_ASSERT_VALUES_EQUAL(repeatPublish.GetStatus(), EStatus::NOT_FOUND);
}

Y_UNIT_TEST(ColdPublishByIntPublicationId) {
    TTopicSdkTestSetup setup("ColdPublishByIntPublicationId", MakeDeferredPublishEnabledSettings());
    TDriver driver(setup.MakeDriverConfig());
    NTopic::TTopicClient topicClient(driver);
    TDeferredPublishClient deferredClient(driver);

    const std::string extId = "ext-sdk-cold-publish";
    const std::string payload = "sdk-cold-payload";
    const auto topicPath = setup.GetFullTopicPath();

    auto begin = deferredClient.BeginPublication(extId).GetValueSync();
    UNIT_ASSERT_C(begin.IsSuccess(), begin.GetIssues().ToString());
    const auto& hotPublication = begin.GetPublication();

    // Scenario 1: cold handle used for both Write and Publish waits for acks.
    NTopic::TDeferredPublication coldPublication(hotPublication.IntPublicationId);
    TDeferredWriteHelper writer(topicClient, topicPath);
    writer.WriteDeferred(payload, coldPublication);

    auto publish = deferredClient.Publish(coldPublication).GetValueSync();
    UNIT_ASSERT_C(publish.IsSuccess(), publish.GetIssues().ToString());

    const auto messages = ReadMessages(topicClient, topicPath, TEST_CONSUMER, 1);
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(messages[0], payload);
}

Y_UNIT_TEST(IndependentColdHandleSkipsLocalAckWait) {
    TTopicSdkTestSetup setup("IndependentColdHandleSkipsLocalAckWait", MakeDeferredPublishEnabledSettings());
    TDriver driver(setup.MakeDriverConfig());
    NTopic::TTopicClient topicClient(driver);
    TDeferredPublishClient deferredClient(driver);

    const std::string extId = "ext-sdk-independent-cold";
    const std::string payload = "sdk-independent-cold-payload";
    const auto topicPath = setup.GetFullTopicPath();

    auto begin = deferredClient.BeginPublication(extId).GetValueSync();
    UNIT_ASSERT_C(begin.IsSuccess(), begin.GetIssues().ToString());

    NTopic::TDeferredPublication writePublication(begin.GetIntPublicationId());
    NTopic::TDeferredPublication publishPublication(begin.GetIntPublicationId());

    TDeferredWriteHelper writer(topicClient, topicPath);
    writer.WriteDeferred(payload, writePublication);
    WaitForWriteAcks(writer.Session());

    // Scenario 2: different cold handle — no local wait; RPC only.
    auto publish = deferredClient.Publish(publishPublication).GetValueSync();
    UNIT_ASSERT_C(publish.IsSuccess(), publish.GetIssues().ToString());

    const auto messages = ReadMessages(topicClient, topicPath, TEST_CONSUMER, 1);
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(messages[0], payload);
}

Y_UNIT_TEST(WriteAfterPublishClosesWriteSession) {
    TTopicSdkTestSetup setup("WriteAfterPublishClosesWriteSession", MakeDeferredPublishEnabledSettings());
    TDriver driver(setup.MakeDriverConfig());
    NTopic::TTopicClient topicClient(driver);
    TDeferredPublishClient deferredClient(driver);

    const std::string extId = "ext-sdk-write-after-publish";
    const auto topicPath = setup.GetFullTopicPath();

    auto begin = deferredClient.BeginPublication(extId).GetValueSync();
    UNIT_ASSERT_C(begin.IsSuccess(), begin.GetIssues().ToString());
    const auto& publication = begin.GetPublication();

    TDeferredWriteHelper writer(topicClient, topicPath);
    writer.WriteDeferred("first", publication);

    // ReadyToAccept for the next write often arrives in the same event batch as the ack;
    // collect both so WaitForWriteAcks does not drop the token.
    std::optional<NTopic::TContinuationToken> nextToken;
    size_t acks = 0;
    while (acks < 1 || !nextToken.has_value()) {
        UNIT_ASSERT_C(
            writer.Session().WaitEvent().Wait(TDuration::Seconds(30)),
            "timeout waiting write ack/token");
        for (auto& event : writer.Session().GetEvents()) {
            if (std::holds_alternative<NTopic::TWriteSessionEvent::TAcksEvent>(event)) {
                ++acks;
            } else if (auto* ready = std::get_if<NTopic::TWriteSessionEvent::TReadyToAcceptEvent>(&event)) {
                nextToken = std::move(ready->ContinuationToken);
            } else if (auto* closed = std::get_if<NTopic::TSessionClosedEvent>(&event)) {
                UNIT_FAIL("Write session closed unexpectedly: " << closed->GetIssues().ToString());
            }
        }
    }

    // WriteCount > 0: Publish seals even though the first write is already acked.
    auto publishFuture = deferredClient.Publish(publication);

    NTopic::TWriteMessage message("second");
    message.DeferredPublication(publication);
    writer.Session().Write(std::move(*nextToken), std::move(message));

    bool closedWithBadRequest = false;
    UNIT_ASSERT_C(
        writer.Session().WaitEvent().Wait(TDuration::Seconds(30)),
        "timeout waiting write session close");
    for (auto& event : writer.Session().GetEvents()) {
        if (auto* closed = std::get_if<NTopic::TSessionClosedEvent>(&event)) {
            closedWithBadRequest = closed->GetStatus() == EStatus::BAD_REQUEST;
        }
    }
    UNIT_ASSERT(closedWithBadRequest);

    auto publish = publishFuture.GetValueSync();
    UNIT_ASSERT_C(publish.IsSuccess(), publish.GetIssues().ToString());
}

Y_UNIT_TEST(PublishAfterIdleSessionClose) {
    TTopicSdkTestSetup setup("PublishAfterIdleSessionClose", MakeDeferredPublishEnabledSettings());
    TDriver driver(setup.MakeDriverConfig());
    NTopic::TTopicClient topicClient(driver);
    TDeferredPublishClient deferredClient(driver);

    const std::string extId = "ext-sdk-idle-close";
    const std::string payload = "sdk-idle-close-payload";
    const auto topicPath = setup.GetFullTopicPath();

    auto begin = deferredClient.BeginPublication(extId).GetValueSync();
    UNIT_ASSERT_C(begin.IsSuccess(), begin.GetIssues().ToString());
    const auto& publication = begin.GetPublication();

    {
        TDeferredWriteHelper writer(topicClient, topicPath);
        writer.WriteDeferred(payload, publication);
        WaitForWriteAcks(writer.Session());
        writer.Close();
    }

    // Abort with no unacked writes must not poison a later Publish on the same handle.
    auto publish = deferredClient.Publish(publication).GetValueSync();
    UNIT_ASSERT_C(publish.IsSuccess(), publish.GetIssues().ToString());

    const auto messages = ReadMessages(topicClient, topicPath, TEST_CONSUMER, 1);
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(messages[0], payload);
}

Y_UNIT_TEST(PublishRetryAfterWriteSessionAbort) {
    TTopicSdkTestSetup setup("PublishRetryAfterWriteSessionAbort", MakeDeferredPublishEnabledSettings());
    TDriver driver(setup.MakeDriverConfig());
    NTopic::TTopicClient topicClient(driver);
    TDeferredPublishClient deferredClient(driver);

    const std::string extId = "ext-sdk-abort-retry";
    const std::string payload = "sdk-abort-retry-payload";
    const auto topicPath = setup.GetFullTopicPath();

    auto begin = deferredClient.BeginPublication(extId).GetValueSync();
    UNIT_ASSERT_C(begin.IsSuccess(), begin.GetIssues().ToString());
    const auto& publication = begin.GetPublication();

    NThreading::TFuture<TPublishResult> firstPublish;
    {
        TDeferredWriteHelper writer(topicClient, topicPath);
        writer.WriteDeferred(payload, publication);
        firstPublish = deferredClient.Publish(publication);
        // Abort before/while waiting for acks so WaitAllAcks can fail.
        writer.Close(TDuration::Zero());
    }

    const auto firstResult = firstPublish.GetValueSync();
    if (firstResult.IsSuccess()) {
        // Write was acked before abort; nothing to retry for this race.
        const auto messages = ReadMessages(topicClient, topicPath, TEST_CONSUMER, 1);
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(messages[0], payload);
        return;
    }

    UNIT_ASSERT_VALUES_EQUAL(firstResult.GetStatus(), EStatus::SESSION_EXPIRED);
    UNIT_ASSERT_STRING_CONTAINS(
        firstResult.GetIssues().ToString(),
        "Cannot finalize deferred publication: associated write session was aborted");

    TDeferredWriteHelper writer2(topicClient, topicPath);
    writer2.WriteDeferred(payload, publication);
    auto secondPublish = deferredClient.Publish(publication).GetValueSync();
    UNIT_ASSERT_C(secondPublish.IsSuccess(), secondPublish.GetIssues().ToString());

    const auto messages = ReadMessages(topicClient, topicPath, TEST_CONSUMER, 1);
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(messages[0], payload);
}

Y_UNIT_TEST(CancelRetryAfterWriteSessionAbort) {
    TTopicSdkTestSetup setup("CancelRetryAfterWriteSessionAbort", MakeDeferredPublishEnabledSettings());
    TDriver driver(setup.MakeDriverConfig());
    NTopic::TTopicClient topicClient(driver);
    TDeferredPublishClient deferredClient(driver);

    const std::string extId = "ext-sdk-cancel-abort-retry";
    const std::string payload = "sdk-cancel-abort-retry-payload";
    const auto topicPath = setup.GetFullTopicPath();

    auto begin = deferredClient.BeginPublication(extId).GetValueSync();
    UNIT_ASSERT_C(begin.IsSuccess(), begin.GetIssues().ToString());
    const auto& publication = begin.GetPublication();

    NThreading::TFuture<TCancelPublicationResult> firstCancel;
    {
        TDeferredWriteHelper writer(topicClient, topicPath);
        writer.WriteDeferred(payload, publication);
        firstCancel = deferredClient.CancelPublication(publication);
        writer.Close(TDuration::Zero());
    }

    const auto firstResult = firstCancel.GetValueSync();
    if (firstResult.IsSuccess()) {
        // Write was acked before abort; cancel already finalized the publication.
        const auto messages = ReadNoMessages(topicClient, topicPath, TEST_CONSUMER);
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 0u);
        return;
    }

    UNIT_ASSERT_VALUES_EQUAL(firstResult.GetStatus(), EStatus::SESSION_EXPIRED);
    UNIT_ASSERT_STRING_CONTAINS(
        firstResult.GetIssues().ToString(),
        "Cannot finalize deferred publication: associated write session was aborted");

    TDeferredWriteHelper writer2(topicClient, topicPath);
    writer2.WriteDeferred(payload, publication);
    auto secondCancel = deferredClient.CancelPublication(publication).GetValueSync();
    UNIT_ASSERT_C(secondCancel.IsSuccess(), secondCancel.GetIssues().ToString());

    const auto messages = ReadNoMessages(topicClient, topicPath, TEST_CONSUMER);
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 0u);
}

Y_UNIT_TEST(StreamWriteRejectsDeferredPlusTx) {
    TTopicSdkTestSetup setup("StreamWriteRejectsDeferredPlusTx", MakeDeferredPublishEnabledSettings());
    TDriver driver(setup.MakeDriverConfig());
    NTopic::TTopicClient topicClient(driver);
    TDeferredPublishClient deferredClient(driver);

    auto begin = deferredClient.BeginPublication("ext-sdk-deferred-tx").GetValueSync();
    UNIT_ASSERT(begin.IsSuccess());

    NTopic::TWriteSessionSettings settings;
    settings.Path(setup.GetFullTopicPath());
    settings.ProducerId("deferred-tx-producer");
    auto session = topicClient.CreateWriteSession(settings);

    auto token = WaitForWriteToken(*session);

    NTopic::TWriteMessage message("payload");
    message.DeferredPublication(begin.GetPublication());

    struct TFakeTransaction : public TTransactionBase {
        TFakeTransaction()
            : SessionIdStorage_("tx-session")
            , TxIdStorage_("tx-id")
        {
            SessionId_ = &SessionIdStorage_;
            TxId_ = &TxIdStorage_;
        }

        void AddPrecommitCallback(TPrecommitTransactionCallback) override {}
        void AddOnFailureCallback(TOnFailureTransactionCallback) override {}

    private:
        std::string SessionIdStorage_;
        std::string TxIdStorage_;
    } fakeTx;
    message.Tx(fakeTx);

    session->Write(std::move(token), std::move(message));

    bool closedWithBadRequest = false;
    UNIT_ASSERT_C(
        session->WaitEvent().Wait(TDuration::Seconds(30)),
        "timeout waiting write session close");
    for (auto& event : session->GetEvents()) {
        if (auto* closed = std::get_if<NTopic::TSessionClosedEvent>(&event)) {
            closedWithBadRequest = closed->GetStatus() == EStatus::BAD_REQUEST;
        }
    }
    UNIT_ASSERT(closedWithBadRequest);
}

} // Y_UNIT_TEST_SUITE
