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
        if (Session_) {
            Session_->Close(TDuration::Seconds(10));
        }
    }

    void WriteDeferred(
        const std::string& payload,
        uint64_t intPublicationId,
        std::optional<std::string> extPublicationId = std::nullopt)
    {
        WaitForToken();
        NTopic::TWriteMessage message(payload);
        NTopic::TDeferredPublication deferred{
            .IntPublicationId = intPublicationId,
        };
        if (extPublicationId) {
            deferred.ExtPublicationId = *extPublicationId;
        }
        message.DeferredPublication(std::move(deferred));
        Session_->Write(std::move(*Token_), std::move(message));
        Token_.reset();
        WaitForAck();
    }

private:
    void WaitForToken() {
        while (!Token_.has_value()) {
            Session_->WaitEvent().Wait(TDuration::Seconds(30));
            for (auto& event : Session_->GetEvents()) {
                if (auto* ready = std::get_if<NTopic::TWriteSessionEvent::TReadyToAcceptEvent>(&event)) {
                    Token_ = std::move(ready->ContinuationToken);
                } else if (auto* closed = std::get_if<NTopic::TSessionClosedEvent>(&event)) {
                    UNIT_FAIL("Write session closed unexpectedly: " << closed->GetIssues().ToString());
                }
            }
        }
    }

    void WaitForAck() {
        bool acked = false;
        while (!acked) {
            Session_->WaitEvent().Wait(TDuration::Seconds(30));
            for (auto& event : Session_->GetEvents()) {
                if (auto* ready = std::get_if<NTopic::TWriteSessionEvent::TReadyToAcceptEvent>(&event)) {
                    Token_ = std::move(ready->ContinuationToken);
                } else if (auto* acks = std::get_if<NTopic::TWriteSessionEvent::TAcksEvent>(&event)) {
                    UNIT_ASSERT(!acks->Acks.empty());
                    acked = true;
                } else if (auto* closed = std::get_if<NTopic::TSessionClosedEvent>(&event)) {
                    UNIT_FAIL("Write session closed unexpectedly: " << closed->GetIssues().ToString());
                }
            }
        }
    }

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
            done.SetValue();
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
        done.SetValue();
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

    auto list = client.ListPublications().GetValueSync();
    UNIT_ASSERT_C(list.IsSuccess(), list.GetIssues().ToString());
    UNIT_ASSERT_VALUES_EQUAL(list.GetPublications().size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(list.GetPublications()[0].ExtPublicationId, extId);
    UNIT_ASSERT_VALUES_EQUAL(list.GetPublications()[0].IntPublicationId, begin.GetIntPublicationId());
}

Y_UNIT_TEST(PublishMakesDataVisible) {
    TTopicSdkTestSetup setup("PublishMakesDataVisible", MakeDeferredPublishEnabledSettings());
    auto topicClient = setup.MakeClient();
    TDriver driver(setup.MakeDriverConfig());
    TTopicDeferredPublishClient deferredClient(driver);

    const std::string extId = "ext-sdk-publish";
    const std::string payload = "sdk-deferred-payload";
    const auto topicPath = setup.GetFullTopicPath();

    auto begin = deferredClient.BeginPublication(extId).GetValueSync();
    UNIT_ASSERT_C(begin.IsSuccess(), begin.GetIssues().ToString());

    TDeferredWriteHelper(topicClient, topicPath).WriteDeferred(payload, begin.GetIntPublicationId(), extId);

    auto publish = deferredClient.Publish(begin.GetIntPublicationId()).GetValueSync();
    UNIT_ASSERT_C(publish.IsSuccess(), publish.GetIssues().ToString());

    const auto messages = ReadMessages(topicClient, topicPath, TEST_CONSUMER, 1);
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(messages[0], payload);
}

Y_UNIT_TEST(StreamWriteAllowsOmitExtPublicationId) {
    TTopicSdkTestSetup setup("StreamWriteAllowsOmitExtPublicationId", MakeDeferredPublishEnabledSettings());
    auto topicClient = setup.MakeClient();
    TDriver driver(setup.MakeDriverConfig());
    TTopicDeferredPublishClient deferredClient(driver);

    const std::string extId = "ext-sdk-omit-ext";
    const std::string payload = "sdk-omit-ext-payload";
    const auto topicPath = setup.GetFullTopicPath();

    auto begin = deferredClient.BeginPublication(extId).GetValueSync();
    UNIT_ASSERT_C(begin.IsSuccess(), begin.GetIssues().ToString());

    TDeferredWriteHelper(topicClient, topicPath).WriteDeferred(payload, begin.GetIntPublicationId());

    auto publish = deferredClient.Publish(begin.GetIntPublicationId()).GetValueSync();
    UNIT_ASSERT_C(publish.IsSuccess(), publish.GetIssues().ToString());

    const auto messages = ReadMessages(topicClient, topicPath, TEST_CONSUMER, 1);
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(messages[0], payload);
}

Y_UNIT_TEST(CancelDiscardsData) {
    TTopicSdkTestSetup setup("CancelDiscardsData", MakeDeferredPublishEnabledSettings());
    auto topicClient = setup.MakeClient();
    TDriver driver(setup.MakeDriverConfig());
    TTopicDeferredPublishClient deferredClient(driver);

    const std::string extId = "ext-sdk-cancel";
    const std::string payload = "sdk-cancel-payload";
    const auto topicPath = setup.GetFullTopicPath();

    auto begin = deferredClient.BeginPublication(extId).GetValueSync();
    UNIT_ASSERT_C(begin.IsSuccess(), begin.GetIssues().ToString());

    TDeferredWriteHelper(topicClient, topicPath).WriteDeferred(payload, begin.GetIntPublicationId(), extId);

    auto cancel = deferredClient.CancelPublication(begin.GetIntPublicationId()).GetValueSync();
    UNIT_ASSERT_C(cancel.IsSuccess(), cancel.GetIssues().ToString());

    const auto messages = ReadNoMessages(topicClient, topicPath, TEST_CONSUMER);
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 0u);
}

Y_UNIT_TEST(StagingNotVisibleBeforePublish) {
    TTopicSdkTestSetup setup("StagingNotVisibleBeforePublish", MakeDeferredPublishEnabledSettings());
    auto topicClient = setup.MakeClient();
    TDriver driver(setup.MakeDriverConfig());
    TTopicDeferredPublishClient deferredClient(driver);

    const std::string extId = "ext-sdk-staging";
    const std::string payload = "sdk-staging-payload";
    const auto topicPath = setup.GetFullTopicPath();

    auto begin = deferredClient.BeginPublication(extId).GetValueSync();
    UNIT_ASSERT_C(begin.IsSuccess(), begin.GetIssues().ToString());

    TDeferredWriteHelper(topicClient, topicPath).WriteDeferred(payload, begin.GetIntPublicationId(), extId);

    const auto messagesBeforePublish = ReadNoMessages(topicClient, topicPath, TEST_CONSUMER);
    UNIT_ASSERT_VALUES_EQUAL(messagesBeforePublish.size(), 0u);

    auto publish = deferredClient.Publish(begin.GetIntPublicationId()).GetValueSync();
    UNIT_ASSERT_C(publish.IsSuccess(), publish.GetIssues().ToString());

    const auto messagesAfterPublish = ReadMessages(topicClient, topicPath, TEST_CONSUMER, 1);
    UNIT_ASSERT_VALUES_EQUAL(messagesAfterPublish.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(messagesAfterPublish[0], payload);
}

Y_UNIT_TEST(RepeatFinalizeNotFound) {
    TTopicSdkTestSetup setup("RepeatFinalizeNotFound", MakeDeferredPublishEnabledSettings());
    auto topicClient = setup.MakeClient();
    TDriver driver(setup.MakeDriverConfig());
    TTopicDeferredPublishClient deferredClient(driver);

    const std::string extId = "ext-sdk-repeat-finalize";
    const auto topicPath = setup.GetFullTopicPath();

    auto begin = deferredClient.BeginPublication(extId).GetValueSync();
    UNIT_ASSERT_C(begin.IsSuccess(), begin.GetIssues().ToString());

    TDeferredWriteHelper(topicClient, topicPath).WriteDeferred(
        "payload", begin.GetIntPublicationId(), extId);

    auto publish = deferredClient.Publish(begin.GetIntPublicationId()).GetValueSync();
    UNIT_ASSERT_C(publish.IsSuccess(), publish.GetIssues().ToString());

    auto repeatPublish = deferredClient.Publish(begin.GetIntPublicationId()).GetValueSync();
    UNIT_ASSERT(!repeatPublish.IsSuccess());
    UNIT_ASSERT_VALUES_EQUAL(repeatPublish.GetStatus(), EStatus::NOT_FOUND);
}

Y_UNIT_TEST(StreamWriteRejectsDeferredPlusTx) {
    TTopicSdkTestSetup setup("StreamWriteRejectsDeferredPlusTx", MakeDeferredPublishEnabledSettings());
    auto topicClient = setup.MakeClient();
    TDriver driver(setup.MakeDriverConfig());
    TTopicDeferredPublishClient deferredClient(driver);

    auto begin = deferredClient.BeginPublication("ext-sdk-deferred-tx").GetValueSync();
    UNIT_ASSERT(begin.IsSuccess());

    NTopic::TWriteSessionSettings settings;
    settings.Path(setup.GetFullTopicPath());
    settings.ProducerId("deferred-tx-producer");
    auto session = topicClient.CreateWriteSession(settings);

    std::optional<NTopic::TContinuationToken> token;
    while (!token.has_value()) {
        session->WaitEvent().Wait(TDuration::Seconds(30));
        for (auto& event : session->GetEvents()) {
            if (auto* ready = std::get_if<NTopic::TWriteSessionEvent::TReadyToAcceptEvent>(&event)) {
                token = std::move(ready->ContinuationToken);
            }
        }
    }

    NTopic::TWriteMessage message("payload");
    NTopic::TDeferredPublication deferred{
        .IntPublicationId = begin.GetIntPublicationId(),
        .ExtPublicationId = "ext-sdk-deferred-tx",
    };
    message.DeferredPublication(std::move(deferred));

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

    session->Write(std::move(*token), std::move(message));

    bool closedWithBadRequest = false;
    session->WaitEvent().Wait(TDuration::Seconds(30));
    for (auto& event : session->GetEvents()) {
        if (auto* closed = std::get_if<NTopic::TSessionClosedEvent>(&event)) {
            closedWithBadRequest = closed->GetStatus() == EStatus::BAD_REQUEST;
        }
    }
    UNIT_ASSERT(closedWithBadRequest);
}

} // Y_UNIT_TEST_SUITE
