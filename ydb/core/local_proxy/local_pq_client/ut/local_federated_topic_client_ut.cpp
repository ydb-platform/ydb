#include "common.h"

#include <ydb/core/local_proxy/local_pq_client/local_federated_topic_client.h>

namespace NKikimr::NKqp::NLocalTopicTests {

namespace {

NFederatedTopic::TFederatedTopicClientSettings FederatedClientSettings() {
    return NFederatedTopic::TFederatedTopicClientSettings()
        .Database("/Root")
        .AuthToken(NACLib::TUserToken(BUILTIN_ACL_ROOT, {}).SerializeAsString());
}

} // namespace

Y_UNIT_TEST_SUITE(TLocalFederatedTopicClient) {
    Y_UNIT_TEST_F(WriteMessages, TLocalTopicClientFixture) {
        auto client = CreateLocalFederatedTopicClient(LocalClientSettings(), FederatedClientSettings());
        auto session = client->CreateWriteSession(NFederatedTopic::TFederatedWriteSessionSettings(WriteSettings("topic")));
        TTestWriter writer(session);
        AssertAck(writer.Write(TWriteMessage("first")), 1, 0);
        AssertAck(writer.Write(TWriteMessage("second")), 2, 1);
        CloseSession(*session);
        AssertTopicMessages({"first", "second"});
    }

    Y_UNIT_TEST_F(ResumeProducerSequenceNumber, TLocalTopicClientFixture) {
        auto client = CreateLocalFederatedTopicClient(LocalClientSettings(), FederatedClientSettings());
        const NFederatedTopic::TFederatedWriteSessionSettings settings(WriteSettings());
        {
            auto session = client->CreateWriteSession(settings);
            TTestWriter writer(session);
            auto message = TWriteMessage("persisted");
            message.SeqNo(42);
            AssertAck(writer.Write(std::move(message)), 42, 0);
            CloseSession(*session);
        }
        auto session = client->CreateWriteSession(settings);
        UNIT_ASSERT_VALUES_EQUAL(session->GetInitSeqNo().GetValue(TEST_TIMEOUT), 42);
        WaitForContinuationToken(*session);
        CloseSession(*session);
        AssertTopicMessages({"persisted"});
    }

    Y_UNIT_TEST_F(MissingTopicClosesWriteSession, TLocalTopicClientFixture) {
        auto client = CreateLocalFederatedTopicClient(LocalClientSettings(), FederatedClientSettings());
        auto session = client->CreateWriteSession(NFederatedTopic::TFederatedWriteSessionSettings(WriteSettings("missing-topic")));
        AssertSessionClosed(*session, EStatus::SCHEME_ERROR);
    }
}

} // namespace NKikimr::NKqp::NLocalTopicTests
