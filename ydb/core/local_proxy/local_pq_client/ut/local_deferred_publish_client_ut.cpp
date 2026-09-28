#include "common.h"

#include <ydb/core/local_proxy/local_pq_client/local_deferred_publish_client.h>

#include <algorithm>

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

    Y_UNIT_TEST_F(ListPublicationsBeforeBegin, TLocalTopicClientFixture) {
        auto client = CreateLocalDeferredPublishClient(LocalClientSettings(), ClientSettings());
        const auto result = client->ListPublications().GetValue(TEST_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT(result.GetPublications().empty());
    }

    Y_UNIT_TEST_F(ListPublicationsPreservesAndFiltersWriterIdentity, TLocalTopicClientFixture) {
        auto client = CreateLocalDeferredPublishClient(LocalClientSettings(), ClientSettings());
        const std::vector<std::optional<std::string>> identities = {std::nullopt, "", "writer", "writer", "other"};
        std::vector<TDeferredPublication> publications;
        for (size_t i = 0; i < identities.size(); ++i) {
            TBeginPublicationSettings settings;
            if (identities[i]) {
                settings.WriterIdentity(*identities[i]);
            }
            const auto begin = client->BeginPublication(TStringBuilder() << "publication-" << i, settings).GetValue(TEST_TIMEOUT);
            UNIT_ASSERT_VALUES_EQUAL_C(begin.GetStatus(), EStatus::SUCCESS, begin.GetIssues().ToString());
            publications.push_back(begin.GetPublication());
        }

        const auto result = client->ListPublications().GetValue(TEST_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        const auto& summaries = result.GetPublications();
        UNIT_ASSERT_VALUES_EQUAL(summaries.size(), publications.size());
        for (size_t i = 0; i < publications.size(); ++i) {
            const auto it = std::find_if(summaries.begin(), summaries.end(), [&](const auto& summary) {
                return summary.IntPublicationId == publications[i].IntPublicationId;
            });
            UNIT_ASSERT(it != summaries.end());
            UNIT_ASSERT_VALUES_EQUAL(it->ExtPublicationId, *publications[i].ExtPublicationId);
            UNIT_ASSERT(it->WriterIdentity == identities[i]);
        }

        for (const std::string writer : {"", "writer", "missing"}) {
            const auto filtered = client->ListPublications(TListPublicationsSettings().WriterIdentity(writer)).GetValue(TEST_TIMEOUT);
            UNIT_ASSERT_VALUES_EQUAL_C(filtered.GetStatus(), EStatus::SUCCESS, filtered.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(filtered.GetPublications().size(), std::count(identities.begin(), identities.end(), std::optional<std::string>(writer)));
            for (const auto& summary : filtered.GetPublications()) {
                UNIT_ASSERT(summary.WriterIdentity);
                UNIT_ASSERT_VALUES_EQUAL(*summary.WriterIdentity, writer);
            }
        }
    }

    Y_UNIT_TEST_F(CancelPublicationDiscardsDeferredWrite, TLocalTopicClientFixture) {
        auto client = CreateLocalDeferredPublishClient(LocalClientSettings(), ClientSettings());
        const auto begin = client->BeginPublication("canceled-message").GetValue(TEST_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL_C(begin.GetStatus(), EStatus::SUCCESS, begin.GetIssues().ToString());
        const auto publication = begin.GetPublication();

        auto session = CreateWriteSession();
        TTestWriter writer(session);
        auto message = TWriteMessage("canceled message");
        message.DeferredPublication(publication);
        // The local client requires the caller to wait for write acks before cancellation.
        const auto ack = writer.Write(std::move(message));
        UNIT_ASSERT_VALUES_EQUAL(ack.State, TWriteSessionEvent::TWriteAck::EES_WRITTEN_IN_TX);
        CloseSession(*session);
        AssertTopicEndOffset(0);

        const auto cancel = client->CancelPublication(publication).GetValue(TEST_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL_C(cancel.GetStatus(), EStatus::SUCCESS, cancel.GetIssues().ToString());
        AssertTopicEndOffset(0);

        const auto list = client->ListPublications().GetValue(TEST_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL_C(list.GetStatus(), EStatus::SUCCESS, list.GetIssues().ToString());
        UNIT_ASSERT(list.GetPublications().empty());

        const auto describe = DeferredClient->DescribePublication(publication).GetValue(TEST_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL(describe.GetStatus(), EStatus::NOT_FOUND);
        const auto publish = client->Publish(publication).GetValue(TEST_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL(publish.GetStatus(), EStatus::NOT_FOUND);
        const auto repeated = client->CancelPublication(publication).GetValue(TEST_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL(repeated.GetStatus(), EStatus::NOT_FOUND);

        WriteTopicMessages({"visible message"});
        AssertTopicMessages({"visible message"});
        AssertTopicEndOffset(1);

        const auto reused = client->BeginPublication("canceled-message").GetValue(TEST_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL_C(reused.GetStatus(), EStatus::SUCCESS, reused.GetIssues().ToString());
        UNIT_ASSERT(reused.GetIntPublicationId() != publication.IntPublicationId);
    }

    Y_UNIT_TEST_F(CancelUnknownPublication, TLocalTopicClientFixture) {
        auto client = CreateLocalDeferredPublishClient(LocalClientSettings(), ClientSettings());
        const auto begin = client->BeginPublication("known-publication").GetValue(TEST_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL_C(begin.GetStatus(), EStatus::SUCCESS, begin.GetIssues().ToString());
        const auto result = client->CancelPublication(TDeferredPublication(begin.GetIntPublicationId() + 1)).GetValue(TEST_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::NOT_FOUND);
    }

    Y_UNIT_TEST_F(ListAndCancelRequireAuthentication, TLocalTopicClientFixture) {
        const auto begin = DeferredClient->BeginPublication("authenticated-publication").GetValue(TEST_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL_C(begin.GetStatus(), EStatus::SUCCESS, begin.GetIssues().ToString());

        auto client = CreateLocalDeferredPublishClient(LocalClientSettings(), TCommonClientSettings().Database("/Root"));
        const auto list = client->ListPublications().GetValue(TEST_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL(list.GetStatus(), EStatus::UNAUTHORIZED);
        UNIT_ASSERT(!list.GetIssues().Empty());
        const auto cancel = client->CancelPublication(begin.GetPublication()).GetValue(TEST_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL(cancel.GetStatus(), EStatus::UNAUTHORIZED);
        UNIT_ASSERT(!cancel.GetIssues().Empty());

        const auto describe = DeferredClient->DescribePublication(begin.GetPublication()).GetValue(TEST_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL_C(describe.GetStatus(), EStatus::SUCCESS, describe.GetIssues().ToString());
    }
}

} // namespace NKikimr::NKqp::NLocalTopicTests
