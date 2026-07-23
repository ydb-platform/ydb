#include "ut_utils/topic_sdk_test_setup.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/ydb_topic_deferred_publish.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/tx/tx.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYdb;
using namespace NYdb::NTopic::NDeferredPublish;
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

std::vector<std::string> ReadMessages(
    NTopic::TTopicClient& client,
    const std::string& topicPath,
    const std::string& consumerName,
    size_t expectedCount,
    TDuration timeout = TDuration::Seconds(30))
{
    std::vector<std::string> messages;
    auto done = NThreading::NewPromise<void>();

    NTopic::TReadSessionSettings settings;
    settings.ConsumerName(consumerName);
    settings.AppendTopics({NTopic::TTopicReadSettings(topicPath).ReadFromTimestamp(TInstant::Zero())});
    settings.EventHandlers_.SimpleDataHandlers(
        [&](NTopic::TReadSessionEvent::TDataReceivedEvent& event) {
        for (const auto& message : event.GetMessages()) {
            messages.emplace_back(TString(message.GetData()));
        }
        if (messages.size() >= expectedCount) {
            done.TrySetValue();
        }
    }, true);

    auto session = client.CreateReadSession(settings);
    UNIT_ASSERT(done.GetFuture().Wait(timeout));
    session->Close(TDuration::Seconds(5));
    return messages;
}

std::vector<std::string> ReadNoMessages(
    NTopic::TTopicClient& client,
    const std::string& topicPath,
    const std::string& consumerName,
    TDuration timeout = TDuration::Seconds(5))
{
    std::vector<std::string> messages;
    auto done = NThreading::NewPromise<void>();

    NTopic::TReadSessionSettings settings;
    settings.ConsumerName(consumerName);
    settings.AppendTopics({NTopic::TTopicReadSettings(topicPath).ReadFromTimestamp(TInstant::Zero())});
    settings.EventHandlers_.SimpleDataHandlers(
        [&](NTopic::TReadSessionEvent::TDataReceivedEvent& event) {
        for (const auto& message : event.GetMessages()) {
            messages.emplace_back(TString(message.GetData()));
        }
        done.TrySetValue();
    }, true);

    auto session = client.CreateReadSession(settings);
    done.GetFuture().Wait(timeout);
    session->Close(TDuration::Seconds(5));
    return messages;
}

} // namespace

Y_UNIT_TEST_SUITE(TopicDeferredPublishSdkClient) {

Y_UNIT_TEST(BeginPublicationRejectsEmptyExtPublicationId) {
    TDriverConfig config;
    TDriver driver(config);
    TTopicDeferredPublishClient client(driver);

    auto result = client.BeginPublication("").GetValueSync();
    UNIT_ASSERT(!result.IsSuccess());
    UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
}

Y_UNIT_TEST(BeginPublicationDisabledByDefault) {
    TTopicSdkTestSetup setup("BeginPublicationDisabledByDefault");
    TDriver driver(setup.MakeDriverConfig());
    TTopicDeferredPublishClient client(driver);

    auto result = client.BeginPublication("ext-disabled").GetValueSync();
    UNIT_ASSERT(!result.IsSuccess());
    UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNSUPPORTED);
}

Y_UNIT_TEST(BeginPublicationCreatesPublication) {
    TTopicSdkTestSetup setup("BeginPublicationCreatesPublication", MakeDeferredPublishEnabledSettings());
    TDriver driver(setup.MakeDriverConfig());
    TTopicDeferredPublishClient client(driver);

    const std::string extId = "ext-sdk-begin";
    auto begin = client.BeginPublication(extId).GetValueSync();
    UNIT_ASSERT_C(begin.IsSuccess(), begin.GetIssues().ToString());
    UNIT_ASSERT_GT(begin.GetIntPublicationId(), 0u);
    UNIT_ASSERT_VALUES_EQUAL(begin.GetPublication().IntPublicationId, begin.GetIntPublicationId());
    UNIT_ASSERT(begin.GetPublication().ExtPublicationId.has_value());
    UNIT_ASSERT_VALUES_EQUAL(*begin.GetPublication().ExtPublicationId, extId);
    UNIT_ASSERT(begin.GetPublication().AckState != nullptr);

    auto list = client.ListPublications().GetValueSync();
    UNIT_ASSERT_C(list.IsSuccess(), list.GetIssues().ToString());
    UNIT_ASSERT_VALUES_EQUAL(list.GetPublications().size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(list.GetPublications()[0].ExtPublicationId, extId);
    UNIT_ASSERT_VALUES_EQUAL(list.GetPublications()[0].IntPublicationId, begin.GetIntPublicationId());
}

Y_UNIT_TEST(PublishMakesDataVisible) {
    TTopicSdkTestSetup setup("PublishMakesDataVisible", MakeDeferredPublishEnabledSettings());
    TDriver driver(setup.MakeDriverConfig());
    NTopic::TTopicClient topicClient(driver);
    TTopicDeferredPublishClient deferredClient(driver);

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
    TTopicDeferredPublishClient deferredClient(driver);

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

Y_UNIT_TEST(CancelDiscardsData) {
    TTopicSdkTestSetup setup("CancelDiscardsData", MakeDeferredPublishEnabledSettings());
    TDriver driver(setup.MakeDriverConfig());
    NTopic::TTopicClient topicClient(driver);
    TTopicDeferredPublishClient deferredClient(driver);

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
    TTopicDeferredPublishClient deferredClient(driver);

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
    TTopicDeferredPublishClient deferredClient(driver);

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
    TTopicDeferredPublishClient deferredClient(driver);

    const std::string extId = "ext-sdk-cold-publish";
    const std::string payload = "sdk-cold-payload";
    const auto topicPath = setup.GetFullTopicPath();

    auto begin = deferredClient.BeginPublication(extId).GetValueSync();
    UNIT_ASSERT_C(begin.IsSuccess(), begin.GetIssues().ToString());
    const auto& hotPublication = begin.GetPublication();

    TDeferredWriteHelper writer(topicClient, topicPath);
    writer.WriteDeferred(payload, hotPublication);
    WaitForWriteAcks(writer.Session());

    // CLI-style cold handle: ids only, no local ack state.
    NTopic::TDeferredPublication coldPublication(hotPublication.IntPublicationId);
    auto publish = deferredClient.Publish(coldPublication).GetValueSync();
    UNIT_ASSERT_C(publish.IsSuccess(), publish.GetIssues().ToString());

    const auto messages = ReadMessages(topicClient, topicPath, TEST_CONSUMER, 1);
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(messages[0], payload);
}

Y_UNIT_TEST(PublishAfterIdleSessionClose) {
    TTopicSdkTestSetup setup("PublishAfterIdleSessionClose", MakeDeferredPublishEnabledSettings());
    TDriver driver(setup.MakeDriverConfig());
    NTopic::TTopicClient topicClient(driver);
    TTopicDeferredPublishClient deferredClient(driver);

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
    TTopicDeferredPublishClient deferredClient(driver);

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
    TTopicDeferredPublishClient deferredClient(driver);

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
    TTopicDeferredPublishClient deferredClient(driver);

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
