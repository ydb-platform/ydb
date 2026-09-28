#include "common.h"

#include <ydb/core/local_proxy/local_pq_client/local_topic_client_factory.h>

namespace NKikimr::NKqp::NLocalTopicTests {

Y_UNIT_TEST_SUITE(TLocalTopicClientFactory) {
    Y_UNIT_TEST_F(CreateTopicClient, TLocalTopicClientFixture) {
        auto factory = CreateLocalTopicClientFactory(LocalClientSettings());
        auto client = factory->CreateTopicClient(ClientSettings());
        factory.Reset();
        const auto result = client->DescribeTopic("topic").GetValue(TEST_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetTopicDescription().GetTotalPartitionsCount(), 1);
    }

    Y_UNIT_TEST_F(CreateFederatedTopicClient, TLocalTopicClientFixture) {
        auto factory = CreateLocalTopicClientFactory(LocalClientSettings());
        const auto settings = NFederatedTopic::TFederatedTopicClientSettings()
            .Database("/Root")
            .AuthToken(NACLib::TUserToken(BUILTIN_ACL_ROOT, {}).SerializeAsString());
        auto client = factory->CreateFederatedTopicClient(settings);
        factory.Reset();
        auto session = client->CreateWriteSession(NFederatedTopic::TFederatedWriteSessionSettings(WriteSettings()));
        TTestWriter writer(session);
        AssertAck(writer.Write(TWriteMessage("factory message")), 1, 0);
        CloseSession(*session);
        AssertTopicMessages({"factory message"});
    }

    Y_UNIT_TEST_F(CreateDeferredPublishClient, TLocalTopicClientFixture) {
        auto factory = CreateLocalTopicClientFactory(LocalClientSettings());
        auto client = factory->CreateDeferredPublishClient(ClientSettings());
        factory.Reset();
        const auto begin = client->BeginPublication("factory-publication").GetValue(TEST_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL_C(begin.GetStatus(), EStatus::SUCCESS, begin.GetIssues().ToString());
        const auto describe = DeferredClient->DescribePublication(begin.GetPublication()).GetValue(TEST_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL_C(describe.GetStatus(), EStatus::SUCCESS, describe.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(describe.GetPublication().ExtPublicationId, "factory-publication");
    }
}

} // namespace NKikimr::NKqp::NLocalTopicTests
