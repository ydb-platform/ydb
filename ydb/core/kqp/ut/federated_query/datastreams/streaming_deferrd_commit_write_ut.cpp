#include "common.h"

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NTopic;

Y_UNIT_TEST_SUITE(KqpStreamingQueriesWithDeferredCommits) {
    Y_UNIT_TEST_TWIN_F(PqGatewayApiForDeferredCommits, LocalTopics, TStreamingTestFixture) {
        LogSettings.AddLogPriority(NKikimrServices::PERSQUEUE, NActors::NLog::PRI_DEBUG);
        SetupAppConfig().MutableFeatureFlags()->SetEnableTopicDeferredPublish(true);

        constexpr char outputTopicName[] = "pqGatewayApiForDeferredCommitsOutputTopicName";
        CreateTopic(outputTopicName, std::nullopt, LocalTopics);

        constexpr char pqSourceName[] = "pqSourceName";
        if constexpr (!LocalTopics) {
            CreatePqSource(pqSourceName);
        }

        constexpr char testUser[] = "test@builtin";
        if constexpr (LocalTopics) {
            ExecQuery(TStringBuilder() << "GRANT ALL ON `/Root` TO `" << testUser << "`");
        } else {
            ExecExternalQuery(TStringBuilder() << "GRANT ALL ON `/" << YDB_DATABASE << "` TO `" << testUser << "`");
        }

        TTopicClientSettings settings;
        settings.Database(TEST_DATABASE);
        if constexpr (LocalTopics) {
            settings.CredentialsProviderFactory(NYql::CreateStructuredTokenCredentialsFactory()->Create(
                NYql::ComposeStructuredTokenJsonForTransientTokenAuth(NACLib::TUserToken(testUser, {}).SerializeAsString())
            ));
        } else {
            settings.Database(YDB_DATABASE);
            settings.DiscoveryEndpoint(YDB_ENDPOINT);
            settings.AuthToken(testUser);
        }

        const TIntrusivePtr<NYql::IPqGateway> pqGateway = SetupRealPqGateway();
        const NYql::ITopicClient::TPtr topicClient = pqGateway->GetTopicClient(*PqGatewayDriver, settings);
        const NYql::IDeferredPublishClient::TPtr publishClient = pqGateway->GetDeferredPublishClient(*PqGatewayDriver, settings);

        constexpr char publicationExtId[] = "publicationId";
        constexpr char publicationWriter[] = "testWriter";
        TDeferredPublication publication;
        {
            const auto result = publishClient->BeginPublication(publicationExtId, TBeginPublicationSettings().WriterIdentity(publicationWriter)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
            publication = result.GetPublication();
            UNIT_ASSERT(publication.ExtPublicationId);
            UNIT_ASSERT_VALUES_EQUAL(*publication.ExtPublicationId, publicationExtId);
        }

        // Validate creation via sdk client
        const std::shared_ptr<TDeferredPublishClient> sdkClient = GetDeferredPublishClient(LocalTopics, testUser);
        {
            const auto result = sdkClient->DescribePublication(publication).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
            const auto& info = result.GetPublication();
            UNIT_ASSERT_VALUES_EQUAL(info.ExtPublicationId, publicationExtId);
            UNIT_ASSERT_VALUES_EQUAL(info.WriterIdentity, publicationWriter);
            UNIT_ASSERT_VALUES_EQUAL(info.CreatedBy, testUser);
            UNIT_ASSERT_VALUES_EQUAL(info.Destinations.size(), 0);
        }

        const auto writeSession = topicClient->CreateWriteSession(TWriteSessionSettings()
            .Codec(ECodec::RAW)
            .Path(outputTopicName));
        const auto disposition = TInstant::Now();

        bool dataWritten = false;
        while (true) {
            writeSession->WaitEvent().Wait();
            auto event = writeSession->GetEvent();
            UNIT_ASSERT(event);

            if (std::holds_alternative<TSessionClosedEvent>(*event)) {
                auto sessionClosedEvent = std::get<TSessionClosedEvent>(std::move(*event));
                UNIT_FAIL("Unexpected session closed event: " << sessionClosedEvent.DebugString());
            } else if (std::holds_alternative<TWriteSessionEvent::TReadyToAcceptEvent>(*event)) {
                auto readyToAcceptEvent = std::get<TWriteSessionEvent::TReadyToAcceptEvent>(std::move(*event));
                if (!dataWritten) {
                    dataWritten = true;
                    TWriteMessage message("test_data");
                    message.DeferredPublication(publication);
                    writeSession->Write(std::move(readyToAcceptEvent.ContinuationToken), std::move(message));
                }
            } else if (std::holds_alternative<TWriteSessionEvent::TAcksEvent>(*event)) {
                auto acksEvent = std::get<TWriteSessionEvent::TAcksEvent>(std::move(*event));
                UNIT_ASSERT(dataWritten);
                UNIT_ASSERT_VALUES_EQUAL(acksEvent.Acks.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(acksEvent.Acks[0].State, TWriteSessionEvent::TWriteAck::EEventState::EES_WRITTEN_IN_TX);
                break;
            } else {
                UNIT_FAIL("Unexpected event: " << event->index());
            }
        }

        writeSession->Close(TDuration::Zero());

        // Validate path registration
        {
            const auto result = sdkClient->DescribePublication(publication).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
            const auto& info = result.GetPublication();
            UNIT_ASSERT_VALUES_EQUAL(info.ExtPublicationId, publicationExtId);
            UNIT_ASSERT_VALUES_EQUAL(info.WriterIdentity, publicationWriter);
            UNIT_ASSERT_VALUES_EQUAL(info.Destinations.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(info.Destinations[0].TopicPath, outputTopicName);
        }

        Sleep(TDuration::Seconds(1));

        // Validate messages are not published
        {
            const std::shared_ptr<TTopicClient> topicClient = GetTopicClient(LocalTopics);
            const auto result = topicClient->DescribeTopic(outputTopicName, TDescribeTopicSettings().IncludeStats(true)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
            const auto& partitions = result.GetTopicDescription().GetPartitions();
            UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 1);
            const auto& stats = partitions[0].GetPartitionStats();
            UNIT_ASSERT(stats);
            UNIT_ASSERT_VALUES_EQUAL(stats->GetStartOffset(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats->GetEndOffset(), 0);
        }

        // Do publish
        {
            const auto result = publishClient->Publish(publication).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
        }

        // Validate messages are published
        ReadTopicMessage(outputTopicName, "test_data", disposition, LocalTopics);

        // Validate that next publish returns NOT_FOUND
        {
            const auto result = publishClient->Publish(publication).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::NOT_FOUND, result.GetIssues().ToOneLineString());
        }

        DropTopic(outputTopicName, LocalTopics);
        if constexpr (!LocalTopics) {
            DropSource(pqSourceName);
        }
    }
}

} // namespace NKikimr::NKqp
