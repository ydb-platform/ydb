#include "common.h"

#include <ydb/core/local_proxy/local_pq_client/local_deferred_publish_client.h>

namespace NKikimr::NKqp::NLocalTopicTests {

Y_UNIT_TEST_SUITE(TLocalDeferredPublishClient) {
    Y_UNIT_TEST_F(BeginPublicationPreservesIdentity, TLocalTopicClientFixture) {
        auto client = CreateLocalDeferredPublishClient(LocalClientSettings(), ClientSettings());
        const auto begin = client->BeginPublication("publication", TBeginPublicationSettings()
            .WriterIdentity("writer")).GetValue(TEST_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL_C(begin.GetStatus(), EStatus::SUCCESS, begin.GetIssues().ToString());
        const auto& publication = begin.GetPublication();
        UNIT_ASSERT(publication.IntPublicationId > 0);
        UNIT_ASSERT_VALUES_EQUAL(publication.IntPublicationId, begin.GetIntPublicationId());
        UNIT_ASSERT(publication.ExtPublicationId);
        UNIT_ASSERT_VALUES_EQUAL(*publication.ExtPublicationId, "publication");

        const auto describe = DeferredClient->DescribePublication(publication).GetValue(TEST_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL_C(describe.GetStatus(), EStatus::SUCCESS, describe.GetIssues().ToString());
        const auto& info = describe.GetPublication();
        UNIT_ASSERT_VALUES_EQUAL(info.ExtPublicationId, "publication");
        UNIT_ASSERT_VALUES_EQUAL(info.WriterIdentity.value_or(""), "writer");
        UNIT_ASSERT_VALUES_EQUAL(info.CreatedBy.value_or(""), BUILTIN_ACL_ROOT);
        UNIT_ASSERT(info.Destinations.empty());
    }

    Y_UNIT_TEST_F(DuplicateExternalPublicationId, TLocalTopicClientFixture) {
        auto client = CreateLocalDeferredPublishClient(LocalClientSettings(), ClientSettings());
        const auto first = client->BeginPublication("duplicate").GetValue(TEST_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL_C(first.GetStatus(), EStatus::SUCCESS, first.GetIssues().ToString());
        const auto second = client->BeginPublication("duplicate").GetValue(TEST_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL(second.GetStatus(), EStatus::ALREADY_EXISTS);
    }

    Y_UNIT_TEST_F(PublishMakesDeferredWriteVisible, TLocalTopicClientFixture) {
        auto client = CreateLocalDeferredPublishClient(LocalClientSettings(), ClientSettings());
        const auto begin = client->BeginPublication("deferred-message").GetValue(TEST_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL_C(begin.GetStatus(), EStatus::SUCCESS, begin.GetIssues().ToString());
        const auto publication = begin.GetPublication();

        auto session = CreateWriteSession();
        TTestWriter writer(session);
        auto message = TWriteMessage("published message");
        message.DeferredPublication(publication);
        // The local publish client requires the caller to wait for write acks.
        const auto ack = writer.Write(std::move(message));
        UNIT_ASSERT_VALUES_EQUAL(ack.State, TWriteSessionEvent::TWriteAck::EES_WRITTEN_IN_TX);
        UNIT_ASSERT_VALUES_EQUAL(ack.SeqNo, 1);
        CloseSession(*session);
        AssertTopicEndOffset(0);

        const auto describe = DeferredClient->DescribePublication(publication).GetValue(TEST_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL_C(describe.GetStatus(), EStatus::SUCCESS, describe.GetIssues().ToString());
        const auto& destinations = describe.GetPublication().Destinations;
        UNIT_ASSERT_VALUES_EQUAL(destinations.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(destinations[0].TopicPath, TOPIC_PATH);
        UNIT_ASSERT(destinations[0].PartitionIds.empty());

        const auto publish = client->Publish(publication).GetValue(TEST_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL_C(publish.GetStatus(), EStatus::SUCCESS, publish.GetIssues().ToString());
        AssertTopicMessages({"published message"});
        AssertTopicEndOffset(1);

        const auto repeated = client->Publish(publication).GetValue(TEST_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL(repeated.GetStatus(), EStatus::NOT_FOUND);
    }

    Y_UNIT_TEST_F(PublishUnknownPublication, TLocalTopicClientFixture) {
        auto client = CreateLocalDeferredPublishClient(LocalClientSettings(), ClientSettings());
        // Initialize the registry before looking up an id which it has not issued.
        const auto begin = client->BeginPublication("known-publication").GetValue(TEST_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL_C(begin.GetStatus(), EStatus::SUCCESS, begin.GetIssues().ToString());
        const auto result = client->Publish(TDeferredPublication(begin.GetIntPublicationId() + 1)).GetValue(TEST_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::NOT_FOUND);
    }
}

} // namespace NKikimr::NKqp::NLocalTopicTests
