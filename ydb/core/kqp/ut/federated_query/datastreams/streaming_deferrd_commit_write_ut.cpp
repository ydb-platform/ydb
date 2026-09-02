#include "common.h"

#include <ydb/core/kqp/ut/federated_query/common/common.h>

#include <fmt/format.h>

namespace NKikimr::NKqp {

using namespace fmt::literals;
using namespace NFederatedQueryTest;
using namespace NTestUtils;
using namespace NYdb;
using namespace NYdb::NTopic;

Y_UNIT_TEST_SUITE(KqpStreamingQueriesWithDeferredCommits) {
    template <typename TClient>
    TDeferredPublication CreatePublication(const TString& extId, const TString& writerIdentity, TClient& client) {
        const auto result = client.BeginPublication(extId, TBeginPublicationSettings().WriterIdentity(writerIdentity)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
        auto publication = result.GetPublication();
        UNIT_ASSERT(publication.ExtPublicationId);
        UNIT_ASSERT_VALUES_EQUAL(*publication.ExtPublicationId, extId);
        return publication;
    }

    template <typename TClient>
    void CommitPublication(const ui64 intId, TClient& client) {
        const auto result = client.Publish(TDeferredPublication(intId)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
    }

    Y_UNIT_TEST_TWIN_F(PqGatewayApiForDeferredCommits, LocalTopics, TStreamingTestFixture) {
        LogSettings.AddLogPriority(NKikimrServices::PERSQUEUE, NActors::NLog::PRI_DEBUG);
        SetupAppConfig().MutableFeatureFlags()->SetEnableTopicDeferredPublish(true);

        const auto outputTopicName = TStringBuilder() << Name_ << "OutputTopicName";
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
        TDeferredPublication publication = CreatePublication(publicationExtId, publicationWriter, *publishClient);

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

        EnsureTopicEndOffset(outputTopicName, /* endOffset */ 0, LocalTopics);
        CommitPublication(publication.IntPublicationId, *publishClient);

        // Validate messages are published
        ReadTopicMessage(outputTopicName, "test_data", disposition, LocalTopics);

        // Validate that next publish returns NOT_FOUND
        {
            const auto result = publishClient->Publish(publication).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::NOT_FOUND, result.GetIssues().ToOneLineString());
        }

        DropTopic(outputTopicName, LocalTopics);
    }

    std::vector<TPublicationSummary> ValidatePublicationsCount(const ui64 count, const TString& queryName, TDeferredPublishClient& client) {
        const auto result = client.ListPublications().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());

        std::vector<TPublicationSummary> found;
        found.reserve(count);
        for (const auto& publication : result.GetPublications()) {
            if (publication.ExtPublicationId.contains(queryName)) {
                found.emplace_back(publication);
                UNIT_ASSERT_STRING_CONTAINS(publication.WriterIdentity.value_or(""), queryName);
            }
        }

        if (found.size() != count) {
            TStringBuilder description;
            for (const auto& publication : found) {
                description << "{" << publication.IntPublicationId << " = " << publication.ExtPublicationId << "} " << Endl;
            }
            UNIT_ASSERT_VALUES_EQUAL_C(found.size(), count, description);
        }

        return found;
    }

    Y_UNIT_TEST_TWIN_F(StreamingQueryWithDeferredComitPublicationCreation, LocalTopics, TStreamingWithSchemaSecretsTestFixture) {
        InternalInitFederatedQuerySetupFactory = true;
        {
            auto& featureFlags = *SetupAppConfig().MutableFeatureFlags();
            featureFlags.SetEnableExactlyOnceTopicsWriting(true);
            featureFlags.SetEnableTopicDeferredPublish(true);
        }

        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");

        const auto inputTopicName = TStringBuilder() << Name_ << "InputTopicName";
        const auto firstOutputTopicName = TStringBuilder() << Name_ << "OutputTopicName1";
        const auto secondOutputTopicName = TStringBuilder() << Name_ << "OutputTopicName2";
        CreateTopic(inputTopicName, std::nullopt, LocalTopics);
        CreateTopic(firstOutputTopicName, std::nullopt, LocalTopics);
        CreateTopic(secondOutputTopicName, std::nullopt, LocalTopics);

        constexpr char pqSourceName[] = "pqSourceName";
        std::shared_ptr<TDeferredPublishClient> sdkClient;
        if constexpr (LocalTopics) {
            sdkClient = GetDeferredPublishClient(LocalTopics, BUILTIN_ACL_ROOT);
        } else {
            CreatePqSourceBasicAuth(pqSourceName, /* useSchemaSecrets  */ true);
            sdkClient = GetDeferredPublishClient(LocalTopics, "", NYdb::CreateLoginCredentialsProviderFactory({
                .User = "root",
                .Password = "1234"
            }));
        }

        constexpr TDuration CHECKPOINT_INTERVAL = TDuration::Seconds(10);
        const auto queryName = TStringBuilder() << Name_ << "StreamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` WITH (
                CHECKPOINT_INTERVAL = "PT{checkpoint_interval}S"
            ) AS
            DO BEGIN
                $data = SELECT * FROM {pq_source}`{input_topic}`;

                INSERT INTO {pq_source}`{first_output_topic}` WITH (
                    DELIVERY_GUARANTEE = "exactly_once"
                ) SELECT Data || "-first" FROM $data;

                INSERT INTO {pq_source}`{second_output_topic}` SELECT Data || "-second" FROM $data;
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = LocalTopics ? TStringBuilder() : TStringBuilder() << "`" << pqSourceName << "`.",
            "input_topic"_a = inputTopicName,
            "first_output_topic"_a = firstOutputTopicName,
            "second_output_topic"_a = secondOutputTopicName,
            "checkpoint_interval"_a = CHECKPOINT_INTERVAL.Seconds()
        ));

        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        const auto& checkpointId = GetStreamingQueryCheckpointId(queryName);

        constexpr ui64 TESTS_COUNT = 2;
        for (ui64 i = 0; i < TESTS_COUNT; ++i) {
            const auto disposition = TInstant::Now();
            WriteTopicMessage(inputTopicName, TStringBuilder() << "test-" << i, /* partition */ 0, LocalTopics);
            ReadTopicMessage(secondOutputTopicName, TStringBuilder() << "test-" << i << "-second", disposition, LocalTopics);

            CheckNoCheckpointUpdate(checkpointId, CHECKPOINT_INTERVAL / 2);
            ValidatePublicationsCount(/* count */ 1, queryName, *sdkClient);
            EnsureTopicEndOffset(firstOutputTopicName, /* endOffset */ i, LocalTopics);

            WaitCheckpointUpdate(checkpointId);
            ValidatePublicationsCount(/* count */ 0, queryName, *sdkClient);
            ReadTopicMessage(firstOutputTopicName, TStringBuilder() << "test-" << i << "-first", disposition, LocalTopics);
        }

        const auto disposition = TInstant::Now();
        WriteTopicMessage(inputTopicName, "test-f", /* partition */ 0, LocalTopics);
        ReadTopicMessage(secondOutputTopicName, "test-f-second", disposition, LocalTopics);

        Sleep(TDuration::Seconds(1));

        ExecQuery(fmt::format(R"(
            DROP STREAMING QUERY `{query_name}`;)",
            "query_name"_a = queryName
        ));

        EnsureTopicEndOffset(firstOutputTopicName, /* endOffset */ TESTS_COUNT, LocalTopics);

        DropTopic(inputTopicName, LocalTopics);
        DropTopic(firstOutputTopicName, LocalTopics);
        DropTopic(secondOutputTopicName, LocalTopics);
    }

    // Test what for graph without checkpoints exactly once write tx commited on finish
    Y_UNIT_TEST_TWIN_F(StreamingQueryDeferredComitPublicationWithoutCheckpoints, LocalTopics, TStreamingWithSchemaSecretsTestFixture) {
        InternalInitFederatedQuerySetupFactory = true;
        {
            auto& featureFlags = *SetupAppConfig().MutableFeatureFlags();
            featureFlags.SetEnableExactlyOnceTopicsWriting(true);
            featureFlags.SetEnableTopicDeferredPublish(true);
        }

        const auto inputTopicName = TStringBuilder() << Name_ << "InputTopicName";
        const auto outputTopicName = TStringBuilder() << Name_ << "OutputTopicName";
        CreateTopic(inputTopicName, std::nullopt, LocalTopics);
        CreateTopic(outputTopicName, std::nullopt, LocalTopics);

        constexpr char pqSourceName[] = "pqSourceName";
        std::shared_ptr<TDeferredPublishClient> sdkClient;
        if constexpr (LocalTopics) {
            sdkClient = GetDeferredPublishClient(LocalTopics, BUILTIN_ACL_ROOT);
        } else {
            CreatePqSourceBasicAuth(pqSourceName, /* useSchemaSecrets  */ true);
            sdkClient = GetDeferredPublishClient(LocalTopics, "", NYdb::CreateLoginCredentialsProviderFactory({
                .User = "root",
                .Password = "1234"
            }));
        }

        const auto queryName = TStringBuilder() << Name_ << "StreamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                PRAGMA ydb.DisableCheckpoints = "TRUE";
                INSERT INTO {pq_source}`{output_topic}` WITH (
                    DELIVERY_GUARANTEE = "exactly_once"
                ) SELECT * FROM {pq_source}`{input_topic}`
                LIMIT 2;
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = LocalTopics ? TStringBuilder() : TStringBuilder() << "`" << pqSourceName << "`.",
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        const auto disposition = TInstant::Now();
        WriteTopicMessage(inputTopicName, "message1", /* partition */ 0, LocalTopics);

        Sleep(TDuration::Seconds(2));

        ValidatePublicationsCount(/* count */ 1, queryName, *sdkClient);
        EnsureTopicEndOffset(outputTopicName, /* endOffset */ 0, LocalTopics);

        WriteTopicMessage(inputTopicName, "message2", /* partition */ 0, LocalTopics);
        ReadTopicMessages(outputTopicName, {"message1", "message2"}, disposition, /* sort */ false, LocalTopics);
        ValidatePublicationsCount(/* count */ 0, queryName, *sdkClient);

        Sleep(TDuration::Seconds(1));
        CheckScriptExecutionsCount(1, 0);

        DropTopic(inputTopicName, LocalTopics);
        DropTopic(outputTopicName, LocalTopics);
    }

    // If some exactly once egress task has no checkpoint dependencies, it effect should be commited on subgraph finish
    Y_UNIT_TEST_TWIN_F(StreamingQuerySubgraphWithoutCheckpoints, LocalTopics, TStreamingWithSchemaSecretsTestFixture) {
        InternalInitFederatedQuerySetupFactory = true;
        {
            auto& featureFlags = *SetupAppConfig().MutableFeatureFlags();
            featureFlags.SetEnableExactlyOnceTopicsWriting(true);
            featureFlags.SetEnableTopicDeferredPublish(true);
        }

        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");

        const auto inputTopicName = TStringBuilder() << Name_ << "InputTopicName";
        const auto outputTopicName = TStringBuilder() << Name_ << "OutputTopicName";
        const auto finiteOutputTopicName = TStringBuilder() << Name_ << "OutputTopicNameFinite";
        CreateTopic(inputTopicName, std::nullopt, LocalTopics);
        CreateTopic(outputTopicName, std::nullopt, LocalTopics);
        CreateTopic(finiteOutputTopicName, std::nullopt, LocalTopics);

        constexpr char tableName[] = "streamingQuerySubgraphWithoutCheckpointsInputTable";
        ExecQuery(fmt::format(R"(
                CREATE TABLE `{table_name}` (
                    Data String NOT NULL,
                    PRIMARY KEY (Data)
                );
            )",
            "table_name"_a = tableName
        ));
        ExecQuery(fmt::format(R"(
                UPSERT INTO `{table_name}` (Data) VALUES ("finite_message1"), ("finite_message2");
            )",
            "table_name"_a = tableName
        ));

        constexpr char pqSourceName[] = "pqSourceName";
        if constexpr (!LocalTopics) {
            CreatePqSourceBasicAuth(pqSourceName, /* useSchemaSecrets  */ true);
        }

        const auto disposition = TInstant::Now();
        const auto queryName = TStringBuilder() << Name_ << "StreamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                INSERT INTO {pq_source}`{output_topic}` SELECT * FROM {pq_source}`{input_topic}`;

                INSERT INTO {pq_source}`{finite_output_topic}` WITH (
                    DELIVERY_GUARANTEE = "exactly_once"
                ) SELECT * FROM {table};
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = LocalTopics ? TStringBuilder() : TStringBuilder() << "`" << pqSourceName << "`.",
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName,
            "finite_output_topic"_a = finiteOutputTopicName,
            "table"_a = tableName
        ));

        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(inputTopicName, "message1", /* partition */ 0, LocalTopics);
        ReadTopicMessage(outputTopicName, "message1", disposition, LocalTopics);
        ReadTopicMessages(finiteOutputTopicName, {"finite_message1", "finite_message2"}, disposition, /* sort */ true, LocalTopics);

        ValidateStreamingQueryAst(queryName, AstChecker(/* txCount */ 1, /* stagesCount */ 2));

        DropTopic(inputTopicName, LocalTopics);
        DropTopic(outputTopicName, LocalTopics);
        DropTopic(finiteOutputTopicName, LocalTopics);
    }

    // When query has checkpoints, finite results must be published only on final checkpoint
    Y_UNIT_TEST_QUAD_F(StreamingQueryWithFiniteResultAndCheckpoints, LocalTopics, ModernChannels, TStreamingWithSchemaSecretsTestFixture) {
        NodeCount = 2;
        DqChannelsVersion = ModernChannels ? 2 : 1;
        InternalInitFederatedQuerySetupFactory = true;
        {
            auto& featureFlags = *SetupAppConfig().MutableFeatureFlags();
            featureFlags.SetEnableExactlyOnceTopicsWriting(true);
            featureFlags.SetEnableTopicDeferredPublish(true);
        }

        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");

        const auto inputTopicName = TStringBuilder() << Name_ << "InputTopicName";
        const auto firstOutputTopicName = TStringBuilder() << Name_ << "OutputTopicName1";
        const auto secondOutputTopicName = TStringBuilder() << Name_ << "OutputTopicName2";
        CreateTopic(inputTopicName, TCreateTopicSettings().PartitioningSettings(2, 2), LocalTopics);
        CreateTopic(firstOutputTopicName, std::nullopt, LocalTopics);
        CreateTopic(secondOutputTopicName, std::nullopt, LocalTopics);

        constexpr char tableName[] = "outputTable";
        ExecQuery(fmt::format(R"(
                CREATE TABLE `{table_name}` (
                    Data String NOT NULL,
                    PRIMARY KEY (Data)
                );
            )",
            "table_name"_a = tableName
        ));

        constexpr char pqSourceName[] = "pqSourceName";
        std::shared_ptr<TDeferredPublishClient> sdkClient;
        if constexpr (LocalTopics) {
            sdkClient = GetDeferredPublishClient(LocalTopics, BUILTIN_ACL_ROOT);
        } else {
            CreatePqSourceBasicAuth(pqSourceName, /* useSchemaSecrets  */ true);
            sdkClient = GetDeferredPublishClient(LocalTopics, "", NYdb::CreateLoginCredentialsProviderFactory({
                .User = "root",
                .Password = "1234"
            }));
        }

        constexpr TDuration CHECKPOINT_INTERVAL = TDuration::Seconds(10);
        const auto queryName = TStringBuilder() << Name_ << "StreamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` WITH (
                CHECKPOINT_INTERVAL = "PT{checkpoint_interval}S"
            ) AS
            DO BEGIN
                PRAGMA ydb.OptValidateStreamingCheckpoints = "FALSE"; -- Enable `LIMIT` operator in at-least-once semantic
                PRAGMA ydb.MaxTasksPerStage = "2"; -- Ensure configuration where one of chanels will be remote, and one local
                PRAGMA ydb.OverridePlanner = @@ [
                    {{ "tx": 0, "stage": 0, "tasks": 2 }}
                ] @@;

                $data = SELECT * FROM {pq_source}`{input_topic}` WITH (
                    FORMAT = "json_each_row",
                    SCHEMA (
                        time String NOT NULL,
                        event String
                    )
                ) LIMIT 2;

                $grouped = SELECT
                    event,
                    CAST(SOME(time) AS String) AS time,
                    CAST(COUNT(*) AS String) AS count
                FROM $data
                GROUP BY
                    HOP (CAST(time AS Timestamp), "PT1H", "PT1H", "PT0H"),
                    event;

                $processed = SELECT Unwrap(event || "-" || time || "-" || count) AS Data FROM $grouped;

                INSERT INTO {pq_source}`{first_output_topic}` WITH (
                    DELIVERY_GUARANTEE = "exactly_once"
                ) SELECT * FROM $processed;

                INSERT INTO {pq_source}`{second_output_topic}` SELECT * FROM $processed;

                UPSERT INTO `{table}` SELECT * FROM $processed;
            END DO;)",
            "query_name"_a = queryName,
            "checkpoint_interval"_a = CHECKPOINT_INTERVAL.Seconds(),
            "pq_source"_a = LocalTopics ? TStringBuilder() : TStringBuilder() << "`" << pqSourceName << "`.",
            "input_topic"_a = inputTopicName,
            "first_output_topic"_a = firstOutputTopicName,
            "second_output_topic"_a = secondOutputTopicName,
            "table"_a = tableName
        ));

        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        const auto disposition = TInstant::Now();
        const auto& checkpointId = GetStreamingQueryCheckpointId(queryName);
        WriteTopicMessage(inputTopicName, R"({"time": "2025-08-24T00:00:00.000000Z", "event": "A"})", /* partition */ 0, LocalTopics);
        WriteTopicMessage(inputTopicName, R"({"time": "2025-08-25T00:00:00.000000Z", "event": "A"})", /* partition */ 1, LocalTopics);
        ReadTopicMessages(secondOutputTopicName, {"A-2025-08-24T00:00:00.000000Z-1", "A-2025-08-25T00:00:00.000000Z-1"}, disposition, /* sort */ true, LocalTopics);

        // Check table data (should be flushed on finish)
        {
            Sleep(TDuration::Seconds(1));
            const auto& results = ExecQuery(fmt::format(R"(
                SELECT * FROM `{table_name}` ORDER BY Data;)",
                "table_name"_a = tableName
            ));
            UNIT_ASSERT_VALUES_EQUAL(results.size(), 1);

            ui64 index = 0;
            const std::vector expected = {"A-2025-08-24T00:00:00.000000Z-1", "A-2025-08-25T00:00:00.000000Z-1"};
            CheckScriptResult(results[0], 1, 2, [&](TResultSetParser& resultSet) {
                UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser("Data").GetString(), expected[index++]);
            });
        }

        // Expected graph state now:
        // [source stage, finished] -> [limit stage, finished] -> [group by stage + 2x PQ sinks, waiting sink{1}] -> [table sink stage, finished]
        // Checkpoint will be injected into source stage and must pass all stages

        CheckNoCheckpointUpdate(checkpointId, CHECKPOINT_INTERVAL / 2);
        ValidatePublicationsCount(/* count */ 1, queryName, *sdkClient);
        EnsureTopicEndOffset(firstOutputTopicName, /* endOffset */ 0, LocalTopics);

        WaitCheckpointUpdate(checkpointId);
        ValidatePublicationsCount(/* count */ 0, queryName, *sdkClient);
        ReadTopicMessages(firstOutputTopicName, {"A-2025-08-24T00:00:00.000000Z-1", "A-2025-08-25T00:00:00.000000Z-1"}, disposition, /* sort */ true, LocalTopics);

        Sleep(TDuration::Seconds(1));
        CheckScriptExecutionsCount(1, 0);

        ValidateStreamingQueryAst(queryName, AstChecker(/* txCount */ 1, /* stagesCount */ 4));

        DropTopic(inputTopicName, LocalTopics);
        DropTopic(firstOutputTopicName, LocalTopics);
        DropTopic(secondOutputTopicName, LocalTopics);
    }

    Y_UNIT_TEST_TWIN_F(ExactlyOnceWritingWithMultipleSinks, LocalTopics, TStreamingWithSchemaSecretsTestFixture) {
        InternalInitFederatedQuerySetupFactory = true;
        {
            auto& featureFlags = *SetupAppConfig().MutableFeatureFlags();
            featureFlags.SetEnableExactlyOnceTopicsWriting(true);
            featureFlags.SetEnableTopicDeferredPublish(true);
        }

        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");

        const auto inputTopicName = TStringBuilder() << Name_ << "InputTopicName";
        const auto outputTopicName1 = TStringBuilder() << Name_ << "OutputTopicName1";
        const auto outputTopicName2 = TStringBuilder() << Name_ << "OutputTopicName2";
        const auto outputTopicName3 = TStringBuilder() << Name_ << "OutputTopicName3";
        CreateTopic(inputTopicName, TCreateTopicSettings().PartitioningSettings(2, 2), LocalTopics);
        CreateTopic(outputTopicName1, std::nullopt, LocalTopics);
        CreateTopic(outputTopicName2, std::nullopt, LocalTopics);
        CreateTopic(outputTopicName3, std::nullopt, LocalTopics);

        constexpr char tableName[] = "exactlyOnceWritingWithMultipleSinkOutputTable";
        ExecQuery(fmt::format(R"(
                CREATE TABLE `{table_name}` (
                    Data String NOT NULL,
                    PRIMARY KEY (Data)
                );
            )",
            "table_name"_a = tableName
        ));

        constexpr char pqSourceName[] = "pqSourceName";
        std::shared_ptr<TDeferredPublishClient> sdkClient;
        if constexpr (LocalTopics) {
            sdkClient = GetDeferredPublishClient(LocalTopics, BUILTIN_ACL_ROOT);
        } else {
            CreatePqSourceBasicAuth(pqSourceName, /* useSchemaSecrets  */ true);
            sdkClient = GetDeferredPublishClient(LocalTopics, "", NYdb::CreateLoginCredentialsProviderFactory({
                .User = "root",
                .Password = "1234"
            }));
        }

        constexpr TDuration CHECKPOINT_INTERVAL = TDuration::Seconds(10);
        const auto queryName = TStringBuilder() << Name_ << "StreamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` WITH (
                CHECKPOINT_INTERVAL = "PT{checkpoint_interval}S"
            ) AS
            DO BEGIN
                PRAGMA ydb.MaxTasksPerStage = "2";
                PRAGMA ydb.OverridePlanner = @@ [
                    {{ "tx": 0, "stage": 0, "tasks": 2 }}
                ] @@;

                $data = SELECT * FROM {pq_source}`{input_topic}`;

                INSERT INTO {pq_source}`{output_topic1}` WITH (
                    DELIVERY_GUARANTEE = "exactly_once"
                ) SELECT * FROM $data;

                INSERT INTO {pq_source}`{output_topic2}` WITH (
                    DELIVERY_GUARANTEE = "exactly_once"
                ) SELECT * FROM $data;

                INSERT INTO {pq_source}`{output_topic3}` SELECT * FROM $data;

                UPSERT INTO `{table}` SELECT * FROM $data;
            END DO;)",
            "query_name"_a = queryName,
            "checkpoint_interval"_a = CHECKPOINT_INTERVAL.Seconds(),
            "pq_source"_a = LocalTopics ? TStringBuilder() : TStringBuilder() << "`" << pqSourceName << "`.",
            "input_topic"_a = inputTopicName,
            "output_topic1"_a = outputTopicName1,
            "output_topic2"_a = outputTopicName2,
            "output_topic3"_a = outputTopicName3,
            "table"_a = tableName
        ));

        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));
        const auto& checkpointId = GetStreamingQueryCheckpointId(queryName);

        const auto disposition = TInstant::Now();
        WriteTopicMessage(inputTopicName, "message1", /* partition */ 0, LocalTopics);
        WriteTopicMessage(inputTopicName, "message2", /* partition */ 1, LocalTopics);
        ReadTopicMessages(outputTopicName3, {"message1", "message2"}, disposition, /* sort */ true, LocalTopics);
        CheckNoCheckpointUpdate(checkpointId, CHECKPOINT_INTERVAL / 2);
        ValidatePublicationsCount(/* count */ 4, queryName, *sdkClient);

        EnsureTopicEndOffset(outputTopicName1, /* endOffset */ 0, LocalTopics);
        EnsureTopicEndOffset(outputTopicName2, /* endOffset */ 0, LocalTopics);
        WaitCheckpointUpdate(checkpointId);
        ValidatePublicationsCount(/* count */ 0, queryName, *sdkClient);

        ReadTopicMessages(outputTopicName1, {"message1", "message2"}, disposition, /* sort */ true, LocalTopics);
        ReadTopicMessages(outputTopicName2, {"message1", "message2"}, disposition, /* sort */ true, LocalTopics);

        const auto& results = ExecQuery(fmt::format(R"(
            SELECT * FROM `{table_name}` ORDER BY Data;)",
            "table_name"_a = tableName
        ));
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 1);

        ui64 index = 0;
        CheckScriptResult(results[0], 1, 2, [&](TResultSetParser& resultSet) {
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser("Data").GetString(), TStringBuilder() << "message" << ++index);
        });

        ValidateStreamingQueryAst(queryName, AstChecker(/* txCount */ 1, /* stagesCount */ 2));

        DropTopic(inputTopicName, LocalTopics);
        DropTopic(outputTopicName1, LocalTopics);
        DropTopic(outputTopicName2, LocalTopics);
        DropTopic(outputTopicName3, LocalTopics);
    }

    Y_UNIT_TEST_F(ExactlyOnceWritingWithMultipleSinksAndSameTopic, TStreamingWithSchemaSecretsTestFixture) {
        InternalInitFederatedQuerySetupFactory = true;
        {
            auto& featureFlags = *SetupAppConfig().MutableFeatureFlags();
            featureFlags.SetEnableExactlyOnceTopicsWriting(true);
            featureFlags.SetEnableTopicDeferredPublish(true);
        }

        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");

        const auto inputTopicName = TStringBuilder() << Name_ << "InputTopicName";
        const auto outputTopicName = TStringBuilder() << Name_ << "OutputTopicName";
        CreateTopic(inputTopicName, TCreateTopicSettings().PartitioningSettings(2, 2));
        CreateTopic(outputTopicName);

        constexpr char tableName[] = "exactlyOnceWritingWithMultipleSinksAndSameTopicOutputTable";
        ExecQuery(fmt::format(R"(
                CREATE TABLE `{table_name}` (
                    Data String NOT NULL,
                    PRIMARY KEY (Data)
                );
            )",
            "table_name"_a = tableName
        ));

        constexpr char pqSourceName1[] = "pqSourceName1";
        constexpr char pqSourceName2[] = "pqSourceName2";
        constexpr char pqSourceName3[] = "pqSourceName3";
        CreatePqSourceBasicAuth(pqSourceName1, /* useSchemaSecrets  */ true);
        CreatePqSourceBasicAuth(pqSourceName2, /* useSchemaSecrets  */ true, /* createSecrets  */ false);
        CreatePqSourceBasicAuth(pqSourceName3, /* useSchemaSecrets  */ true, /* createSecrets  */ false);
        std::shared_ptr<TDeferredPublishClient> sdkClient = GetDeferredPublishClient(/* local */ false, "", NYdb::CreateLoginCredentialsProviderFactory({
            .User = "root",
            .Password = "1234"
        }));

        constexpr TDuration CHECKPOINT_INTERVAL = TDuration::Seconds(10);
        const auto queryName = TStringBuilder() << Name_ << "StreamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` WITH (
                CHECKPOINT_INTERVAL = "PT{checkpoint_interval}S"
            ) AS
            DO BEGIN
                PRAGMA ydb.MaxTasksPerStage = "2";
                PRAGMA ydb.OverridePlanner = @@ [
                    {{ "tx": 0, "stage": 0, "tasks": 2 }}
                ] @@;

                $data = SELECT * FROM `{pq_source1}`.`{input_topic}`;

                INSERT INTO `{pq_source1}`.`{output_topic}` WITH (
                    DELIVERY_GUARANTEE = "exactly_once"
                ) SELECT Data || "-1" FROM $data;

                INSERT INTO `{pq_source2}`.`{output_topic}` WITH (
                    DELIVERY_GUARANTEE = "exactly_once"
                ) SELECT Data || "-2" FROM $data;

                INSERT INTO `{pq_source3}`.`{output_topic}` SELECT * FROM $data;

                UPSERT INTO `{table}` SELECT * FROM $data;
            END DO;)",
            "query_name"_a = queryName,
            "checkpoint_interval"_a = CHECKPOINT_INTERVAL.Seconds(),
            "pq_source1"_a = pqSourceName1,
            "pq_source2"_a = pqSourceName2,
            "pq_source3"_a = pqSourceName3,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName,
            "table"_a = tableName
        ));

        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));
        const auto& checkpointId = GetStreamingQueryCheckpointId(queryName);

        const auto disposition = TInstant::Now();
        WriteTopicMessage(inputTopicName, "message1", /* partition */ 0);
        WriteTopicMessage(inputTopicName, "message2", /* partition */ 1);
        ReadTopicMessages(outputTopicName, {"message1", "message2"}, disposition, /* sort */ true);
        CheckNoCheckpointUpdate(checkpointId, CHECKPOINT_INTERVAL / 2);
        ValidatePublicationsCount(/* count */ 4, queryName, *sdkClient);

        EnsureTopicEndOffset(outputTopicName, /* endOffset  */ 2);
        WaitCheckpointUpdate(checkpointId);
        ValidatePublicationsCount(/* count */ 0, queryName, *sdkClient);

        ReadTopicMessages(outputTopicName, {"message1", "message2", "message1-1", "message2-1", "message1-2", "message2-2"}, disposition, /* sort */ true);

        const auto& results = ExecQuery(fmt::format(R"(
            SELECT * FROM `{table_name}` ORDER BY Data;)",
            "table_name"_a = tableName
        ));
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 1);

        ui64 index = 0;
        CheckScriptResult(results[0], 1, 2, [&](TResultSetParser& resultSet) {
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser("Data").GetString(), TStringBuilder() << "message" << ++index);
        });

        ValidateStreamingQueryAst(queryName, AstChecker(/* txCount */ 1, /* stagesCount */ 2));

        DropTopic(inputTopicName);
        DropTopic(outputTopicName);
    }

    Y_UNIT_TEST_QUAD_F(RestartStreamingQueryWithExactlyOnceWriting, LocalTopics, ModernChannels, TStreamingWithSchemaSecretsTestFixture) {
        InternalInitFederatedQuerySetupFactory = true;
        DqChannelsVersion = ModernChannels ? 2 : 1;
        {
            auto& featureFlags = *SetupAppConfig().MutableFeatureFlags();
            featureFlags.SetEnableExactlyOnceTopicsWriting(true);
            featureFlags.SetEnableTopicDeferredPublish(true);
        }

        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");

        const auto inputTopicName = TStringBuilder() << Name_ << "InputTopicName";
        const auto firstOutputTopicName = TStringBuilder() << Name_ << "OutputTopicName1";
        const auto secondOutputTopicName = TStringBuilder() << Name_ << "OutputTopicName2";
        CreateTopic(inputTopicName, std::nullopt, LocalTopics);
        CreateTopic(firstOutputTopicName, std::nullopt, LocalTopics);
        CreateTopic(secondOutputTopicName, std::nullopt, LocalTopics);

        constexpr char pqSourceName[] = "pqSourceName";
        std::shared_ptr<TDeferredPublishClient> sdkClient;
        if constexpr (LocalTopics) {
            sdkClient = GetDeferredPublishClient(LocalTopics, BUILTIN_ACL_ROOT);
        } else {
            CreatePqSourceBasicAuth(pqSourceName, /* useSchemaSecrets  */ true);
            sdkClient = GetDeferredPublishClient(LocalTopics, "", NYdb::CreateLoginCredentialsProviderFactory({
                .User = "root",
                .Password = "1234"
            }));
        }

        constexpr TDuration CHECKPOINT_INTERVAL = TDuration::Seconds(10);
        const auto queryName = TStringBuilder() << Name_ << "StreamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` WITH (
                CHECKPOINT_INTERVAL = "PT{checkpoint_interval}S"
            ) AS
            DO BEGIN
                -- Test that offsets are recovered
                $pq_source = SELECT * FROM {pq_source}`{input_topic}` WITH (
                    FORMAT = "json_each_row",
                    SCHEMA (
                        time String NOT NULL,
                        event String
                    )
                );

                -- Test that state also recovered
                $grouped = SELECT
                    event,
                    CAST(SOME(time) AS String) AS time,
                    CAST(COUNT(*) AS String) AS count
                FROM $pq_source
                GROUP BY
                    HOP (CAST(time AS Timestamp), "PT1H", "PT1H", "PT0H"),
                    event;

                INSERT INTO {pq_source}`{first_output_topic}` WITH (DELIVERY_GUARANTEE = "exactly_once")
                SELECT Unwrap(event || "-" || time || "-" || count) FROM $grouped;

                INSERT INTO {pq_source}`{second_output_topic}`
                SELECT Unwrap(event || "-" || time || "-" || count) FROM $grouped;
            END DO;)",
            "query_name"_a = queryName,
            "checkpoint_interval"_a = CHECKPOINT_INTERVAL.Seconds(),
            "pq_source"_a = LocalTopics ? TStringBuilder() : TStringBuilder() << "`" << pqSourceName << "`.",
            "input_topic"_a = inputTopicName,
            "first_output_topic"_a = firstOutputTopicName,
            "second_output_topic"_a = secondOutputTopicName
        ));

        TInstant dispositionFirst = TInstant::Now();
        TInstant dispositionSecond = TInstant::Now();
        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        const auto& checkpointId = GetStreamingQueryCheckpointId(queryName);

        // Write and read first message
        {
            WriteTopicMessages(inputTopicName, {
                R"({"time": "2025-08-24T00:00:00.000000Z", "event": "A"})",
                R"({"time": "2025-08-25T00:00:00.000000Z", "event": "A"})",
            }, /* partition */ 0, LocalTopics);
            ReadTopicMessage(secondOutputTopicName, "A-2025-08-24T00:00:00.000000Z-1", dispositionSecond, LocalTopics);
            dispositionSecond = TInstant::Now();

            CheckNoCheckpointUpdate(checkpointId, CHECKPOINT_INTERVAL / 2);
            ValidatePublicationsCount(/* count */ 1, queryName, *sdkClient);
            EnsureTopicEndOffset(firstOutputTopicName, /* endOffset */ 0, LocalTopics);

            WaitCheckpointUpdate(checkpointId);
            ValidatePublicationsCount(/* count */ 0, queryName, *sdkClient);
            ReadTopicMessage(firstOutputTopicName, "A-2025-08-24T00:00:00.000000Z-1", dispositionFirst, LocalTopics);
            dispositionFirst = TInstant::Now();
        }

        Sleep(TDuration::Seconds(1));
        const ui64 checkpointSeqNo = GetLastCheckpointSeqNo(checkpointId);

        // Write second message
        {
            WriteTopicMessage(inputTopicName, R"({"time": "2025-08-26T00:00:00.000000Z", "event": "A"})", /* partition */ 0, LocalTopics);
            ReadTopicMessage(secondOutputTopicName, "A-2025-08-25T00:00:00.000000Z-1", dispositionSecond, LocalTopics);
            dispositionSecond = TInstant::Now();

            CheckNoCheckpointUpdate(checkpointId, CHECKPOINT_INTERVAL / 4);
            ValidatePublicationsCount(/* count */ 1, queryName, *sdkClient);
            EnsureTopicEndOffset(firstOutputTopicName, /* endOffset */ 1, LocalTopics);
        }

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = FALSE
            );)",
            "query_name"_a = queryName
        ));
        CheckScriptExecutionsCount(1, 0);
        UNIT_ASSERT_VALUES_EQUAL(GetLastCheckpointSeqNo(checkpointId), checkpointSeqNo);

        Sleep(TDuration::Seconds(1));
        WriteTopicMessage(inputTopicName, R"({"time": "2025-08-26T00:00:00.000000Z", "event": "B"})", /* partition */ 0, LocalTopics);

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = TRUE
            );)",
            "query_name"_a = queryName
        ));
        CheckScriptExecutionsCount(2, 1);
        Sleep(TDuration::Seconds(1));

        // Write and read third message
        {
            WriteTopicMessage(inputTopicName, R"({"time": "2025-08-27T00:00:00.000000Z", "event": "A"})", /* partition */ 0, LocalTopics);
            ReadTopicMessages(secondOutputTopicName, {
                "A-2025-08-25T00:00:00.000000Z-1", // Duplicated
                "A-2025-08-26T00:00:00.000000Z-1",
                "B-2025-08-26T00:00:00.000000Z-1"
            }, dispositionSecond, /* sort */ true, LocalTopics);

            CheckNoCheckpointUpdate(checkpointId, CHECKPOINT_INTERVAL / 2);
            ValidatePublicationsCount(/* count */ 2, queryName, *sdkClient);
            EnsureTopicEndOffset(firstOutputTopicName, /* endOffset */ 1, LocalTopics);

            WaitCheckpointUpdate(checkpointId);
            ValidatePublicationsCount(/* count */ 1, queryName, *sdkClient);
            ReadTopicMessages(firstOutputTopicName, {
                "A-2025-08-25T00:00:00.000000Z-1",
                "A-2025-08-26T00:00:00.000000Z-1",
                "B-2025-08-26T00:00:00.000000Z-1"
            }, dispositionFirst, /* sort */ true, LocalTopics);
        }

        DropTopic(inputTopicName, LocalTopics);
        DropTopic(firstOutputTopicName, LocalTopics);
        DropTopic(secondOutputTopicName, LocalTopics);
    }

    Y_UNIT_TEST_F(RecoveryStreamingQueryWithExactlyOnceWriting, TStreamingWithSchemaSecretsTestFixture) {
        SetupAppConfig().MutableFeatureFlags()->SetEnableExactlyOnceTopicsWriting(true);

        auto pqGateway = SetupMockPqGateway();

        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");

        const auto inputTopic = TStringBuilder() << Name_ << "InputTopicName";
        const auto outputTopic = TStringBuilder() << Name_ << "OutputTopicName";
        constexpr char pqSourceName[] = "pqSourceName";
        CreateTopic(inputTopic);
        CreateTopic(outputTopic);
        CreatePqSourceBasicAuth(pqSourceName, /* useSchemaSecrets  */ true);

        constexpr TDuration CHECKPOINT_INTERVAL = TDuration::Seconds(10);
        const auto queryName = TStringBuilder() << Name_ << "StreamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` WITH (
                CHECKPOINT_INTERVAL = "PT{checkpoint_interval}S"
            ) AS
            DO BEGIN
                INSERT INTO `{pq_source}`.`{output_topic}` WITH (DELIVERY_GUARANTEE = "exactly_once")
                SELECT * FROM `{pq_source}`.`{input_topic}`;
            END DO;)",
            "query_name"_a = queryName,
            "checkpoint_interval"_a = CHECKPOINT_INTERVAL.Seconds(),
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopic,
            "output_topic"_a = outputTopic
        ));

        CheckScriptExecutionsCount(1, 1);

        const auto readSession = pqGateway->WaitReadSession(inputTopic);
        auto writeSession = pqGateway->WaitWriteSession(outputTopic);
        auto& publicationController = pqGateway->GetDeferredPublishClientController();
        const auto& checkpointId = GetStreamingQueryCheckpointId(queryName);

        readSession->AddDataReceivedEvent(0, "test_message1");
        CheckNoCheckpointUpdate(checkpointId, CHECKPOINT_INTERVAL / 2);
        publicationController.EnsureOpenedPublications(/* count */ 1, queryName);
        writeSession->EnsureEmpty();

        WaitCheckpointUpdate(checkpointId);
        publicationController.EnsureOpenedPublications(/* count */ 0, queryName);
        writeSession->ExpectMessage("test_message1");

        readSession->AddDataReceivedEvent(0, "test_message2");
        CheckNoCheckpointUpdate(checkpointId, CHECKPOINT_INTERVAL / 2);
        publicationController.EnsureOpenedPublications(/* count */ 1, queryName);
        writeSession->EnsureEmpty();

        readSession->AddCloseSessionEvent(EStatus::UNAVAILABLE, {NIssue::TIssue("Test pq session failure")});

        pqGateway->WaitReadSession(inputTopic)->AddDataReceivedEvent(0, "test_message3");
        writeSession = pqGateway->WaitWriteSession(outputTopic);
        CheckNoCheckpointUpdate(checkpointId, CHECKPOINT_INTERVAL / 2);
        publicationController.EnsureOpenedPublications(/* count */ 2, queryName);
        writeSession->EnsureEmpty();

        WaitCheckpointUpdate(checkpointId);
        publicationController.EnsureOpenedPublications(/* count */ 1, queryName);
        writeSession->ExpectMessage("test_message3");

        DropTopic(inputTopic);
        DropTopic(outputTopic);
    }

    Y_UNIT_TEST_TWIN_F(CommitRetryOnStreamingQueryRecovery, CloseReadSession, TStreamingWithSchemaSecretsTestFixture) {
        SetupAppConfig().MutableFeatureFlags()->SetEnableExactlyOnceTopicsWriting(true);

        auto pqGateway = SetupMockPqGateway();

        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");

        const auto inputTopic = TStringBuilder() << Name_ << "InputTopicName";
        const auto outputTopic = TStringBuilder() << Name_ << "OutputTopicName";
        constexpr char pqSourceName[] = "pqSourceName";
        CreateTopic(inputTopic);
        CreateTopic(outputTopic);
        CreatePqSourceBasicAuth(pqSourceName, /* useSchemaSecrets  */ true);

        constexpr TDuration CHECKPOINT_INTERVAL = TDuration::Seconds(10);
        const auto queryName = TStringBuilder() << Name_ << "StreamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` WITH (
                CHECKPOINT_INTERVAL = "PT{checkpoint_interval}S"
            ) AS
            DO BEGIN
                INSERT INTO `{pq_source}`.`{output_topic}` WITH (DELIVERY_GUARANTEE = "exactly_once")
                SELECT * FROM `{pq_source}`.`{input_topic}`;
            END DO;)",
            "query_name"_a = queryName,
            "checkpoint_interval"_a = CHECKPOINT_INTERVAL.Seconds(),
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopic,
            "output_topic"_a = outputTopic
        ));

        CheckScriptExecutionsCount(1, 1);

        const auto readSession = pqGateway->WaitReadSession(inputTopic);
        auto writeSession = pqGateway->WaitWriteSession(outputTopic);
        auto& publicationController = pqGateway->GetDeferredPublishClientController();
        const auto& checkpointId = GetStreamingQueryCheckpointId(queryName);

        publicationController.LockCommits();

        readSession->AddDataReceivedEvent(0, "test_message1");
        const auto seqNo = CheckNoCheckpointUpdate(checkpointId, CHECKPOINT_INTERVAL / 2);
        publicationController.EnsureOpenedPublications(/* count */ 1, queryName);

        publicationController.WaitCommits(/* count */ 1);
        CheckNoCheckpointUpdate(checkpointId, CHECKPOINT_INTERVAL, seqNo); // Checkpoint waits for publication commit
        publicationController.ClearCommits();
        writeSession->EnsureEmpty();

        if constexpr (CloseReadSession) {
            readSession->AddCloseSessionEvent(EStatus::UNAVAILABLE, {NIssue::TIssue("Test pq session failure")});
            readSession->ExpectSessionClosed();
        } else {
            writeSession->AddCloseSessionEvent(EStatus::UNAVAILABLE, {NIssue::TIssue("Test pq session failure")});
            writeSession->ExpectSessionClosed();
        }

        // Commit must be retried with correct publication id
        publicationController.WaitCommits(/* count */ 1);
        publicationController.UnlockCommits();
        publicationController.EnsureOpenedPublications(/* count */ 0, queryName);
        writeSession->ExpectMessage("test_message1");

        // Check that checkpointing works for restarted query
        pqGateway->WaitReadSession(inputTopic)->AddDataReceivedEvent(0, "test_message2");
        writeSession = pqGateway->WaitWriteSession(outputTopic);
        CheckNoCheckpointUpdate(checkpointId, CHECKPOINT_INTERVAL / 4);
        publicationController.EnsureOpenedPublications(/* count */ 1, queryName);
        writeSession->EnsureEmpty();

        WaitCheckpointUpdate(checkpointId);
        publicationController.EnsureOpenedPublications(/* count */ 0, queryName);
        writeSession->ExpectMessage("test_message2");

        Sleep(TDuration::Seconds(1));

        {
            const auto& issues = GetStreamingQueryIssues(queryName);
            UNIT_ASSERT_STRING_CONTAINS(issues, "Test pq session failure");

            if constexpr (CloseReadSession) {
                UNIT_ASSERT_STRING_CONTAINS(issues, TStringBuilder() << "Read session to topic \\\"" << inputTopic << "\\\" was closed");
            } else {
                UNIT_ASSERT_STRING_CONTAINS(issues, TStringBuilder() << "Write session to topic \\\"" << outputTopic << "\\\" was closed. Status: UNAVAILABLE");
            }
        }

        DropTopic(inputTopic);
        DropTopic(outputTopic);
    }

    Y_UNIT_TEST_TWIN_F(DeferredPublicationCreationFailure, LocalTopics, TStreamingWithSchemaSecretsTestFixture) {
        InternalInitFederatedQuerySetupFactory = true;
        {
            auto& featureFlags = *SetupAppConfig().MutableFeatureFlags();
            featureFlags.SetEnableExactlyOnceTopicsWriting(true);
            featureFlags.SetEnableTopicDeferredPublish(true);
        }

        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");

        const auto inputTopicName = TStringBuilder() << Name_ << "InputTopicName";
        const auto outputTopicName = TStringBuilder() << Name_ << "OutputTopicName";
        CreateTopic(inputTopicName, std::nullopt, LocalTopics);
        CreateTopic(outputTopicName, std::nullopt, LocalTopics);

        constexpr char pqSourceName[] = "pqSourceName";
        std::shared_ptr<TDeferredPublishClient> sdkClient;
        if constexpr (LocalTopics) {
            sdkClient = GetDeferredPublishClient(LocalTopics, BUILTIN_ACL_ROOT);
        } else {
            CreatePqSourceBasicAuth(pqSourceName, /* useSchemaSecrets  */ true);
            sdkClient = GetDeferredPublishClient(LocalTopics, "", NYdb::CreateLoginCredentialsProviderFactory({
                .User = "root",
                .Password = "1234"
            }));
        }

        constexpr TDuration CHECKPOINT_INTERVAL = TDuration::Seconds(10);
        const auto queryName = TStringBuilder() << Name_ << "StreamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` WITH (
                CHECKPOINT_INTERVAL = "PT{checkpoint_interval}S"
            ) AS
            DO BEGIN
                INSERT INTO {pq_source}`{output_topic}` WITH (DELIVERY_GUARANTEE = "exactly_once")
                SELECT * FROM {pq_source}`{input_topic}`;
            END DO;)",
            "query_name"_a = queryName,
            "checkpoint_interval"_a = CHECKPOINT_INTERVAL.Seconds(),
            "pq_source"_a = LocalTopics ? TStringBuilder() : TStringBuilder() << "`" << pqSourceName << "`.",
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        TInstant disposition = TInstant::Now();
        const auto& checkpointId = GetStreamingQueryCheckpointId(queryName);

        WriteTopicMessage(inputTopicName, "message1", /* partition */ 0, LocalTopics);
        CheckNoCheckpointUpdate(checkpointId, CHECKPOINT_INTERVAL / 2);
        const std::vector<TPublicationSummary>& publications = ValidatePublicationsCount(/* count */ 1, queryName, *sdkClient);
        EnsureTopicEndOffset(outputTopicName, /* endOffset */ 0, LocalTopics);

        // Externally create publication with next seq-no
        TString testExtId;
        {
            const TStringBuf extId = publications[0].ExtPublicationId;
            const auto sepPos = extId.rfind(":");
            UNIT_ASSERT(sepPos != TStringBuf::npos);
            UNIT_ASSERT_LE(sepPos + 1, extId.size());

            testExtId = TStringBuilder() << extId.SubString(0, sepPos) << ":" << FromString<ui64>(extId.SubString(sepPos + 1, extId.size() - sepPos)) + 1;
            CreatePublication(testExtId, testExtId, *sdkClient);
            ValidatePublicationsCount(/* count */ 2, queryName, *sdkClient);
            ValidatePublicationsCount(/* count */ 1, testExtId, *sdkClient);
        }

        WaitCheckpointUpdate(checkpointId);
        ValidatePublicationsCount(/* count */ 1, queryName, *sdkClient);
        ValidatePublicationsCount(/* count */ 1, testExtId, *sdkClient);
        ReadTopicMessage(outputTopicName, "message1", disposition, LocalTopics);

        disposition = TInstant::Now();
        WriteTopicMessage(inputTopicName, "message2", /* partition */ 0, LocalTopics);
        CheckNoCheckpointUpdate(checkpointId, CHECKPOINT_INTERVAL / 2);
        ValidatePublicationsCount(/* count */ 2, queryName, *sdkClient);
        ValidatePublicationsCount(/* count */ 1, testExtId, *sdkClient);
        EnsureTopicEndOffset(outputTopicName, /* endOffset */ 1, LocalTopics);

        WaitCheckpointUpdate(checkpointId);
        ValidatePublicationsCount(/* count */ 1, queryName, *sdkClient);
        ValidatePublicationsCount(/* count */ 1, testExtId, *sdkClient);
        ReadTopicMessage(outputTopicName, "message2", disposition, LocalTopics);

        {
            const auto& issues = GetStreamingQueryIssues(queryName);
            UNIT_ASSERT_STRING_CONTAINS(issues, "Failed to create deferred publication. Status: ALREADY_EXISTS");
            UNIT_ASSERT_STRING_CONTAINS(issues, "Conflict with existing key.");
        }

        DropTopic(inputTopicName, LocalTopics);
        DropTopic(outputTopicName, LocalTopics);
    }

    Y_UNIT_TEST_TWIN_F(DeferredPublicationDoubleCommitIsOk, LocalTopics, TStreamingTestFixture) {
        InternalInitFederatedQuerySetupFactory = true;
        {
            auto& featureFlags = *SetupAppConfig().MutableFeatureFlags();
            featureFlags.SetEnableExactlyOnceTopicsWriting(true);
            featureFlags.SetEnableTopicDeferredPublish(true);
        }

        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");

        const auto inputTopicName = TStringBuilder() << Name_ << "InputTopicName";
        const auto outputTopicName = TStringBuilder() << Name_ << "OutputTopicName";
        CreateTopic(inputTopicName, std::nullopt, LocalTopics);
        CreateTopic(outputTopicName, std::nullopt, LocalTopics);

        constexpr char pqSourceName[] = "pqSourceName";
        std::shared_ptr<TDeferredPublishClient> sdkClient;
        if constexpr (LocalTopics) {
            sdkClient = GetDeferredPublishClient(LocalTopics, BUILTIN_ACL_ROOT);
        } else {
            CreatePqSourceBasicAuth(pqSourceName, /* useSchemaSecrets  */ true);
            sdkClient = GetDeferredPublishClient(LocalTopics, "", NYdb::CreateLoginCredentialsProviderFactory({
                .User = "root",
                .Password = "1234"
            }));
        }

        constexpr TDuration CHECKPOINT_INTERVAL = TDuration::Seconds(10);
        const auto queryName = TStringBuilder() << Name_ << "StreamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` WITH (
                CHECKPOINT_INTERVAL = "PT{checkpoint_interval}S"
            ) AS
            DO BEGIN
                INSERT INTO {pq_source}`{output_topic}` WITH (DELIVERY_GUARANTEE = "exactly_once")
                SELECT * FROM {pq_source}`{input_topic}`;
            END DO;)",
            "query_name"_a = queryName,
            "checkpoint_interval"_a = CHECKPOINT_INTERVAL.Seconds(),
            "pq_source"_a = LocalTopics ? TStringBuilder() : TStringBuilder() << "`" << pqSourceName << "`.",
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        TInstant disposition = TInstant::Now();
        const auto& checkpointId = GetStreamingQueryCheckpointId(queryName);

        WriteTopicMessage(inputTopicName, "message1", /* partition */ 0, LocalTopics);
        CheckNoCheckpointUpdate(checkpointId, CHECKPOINT_INTERVAL / 2);
        const std::vector<TPublicationSummary>& publications = ValidatePublicationsCount(/* count */ 1, queryName, *sdkClient);
        EnsureTopicEndOffset(outputTopicName, /* endOffset */ 0, LocalTopics);

        // Commit publication in front of query
        {
            const auto checkpointSeqNo = GetLastCheckpointSeqNo(checkpointId);
            CommitPublication(publications[0].IntPublicationId, *sdkClient);
            ValidatePublicationsCount(/* count */ 0, queryName, *sdkClient);
            ReadTopicMessage(outputTopicName, "message1", disposition, LocalTopics);
            UNIT_ASSERT_EQUAL(GetLastCheckpointSeqNo(checkpointId), checkpointSeqNo);
        }

        WaitCheckpointUpdate(checkpointId);
        ValidatePublicationsCount(/* count */ 0, queryName, *sdkClient);
        EnsureTopicEndOffset(outputTopicName, /* endOffset */ 1, LocalTopics);

        disposition = TInstant::Now();
        WriteTopicMessage(inputTopicName, "message2", /* partition */ 0, LocalTopics);
        CheckNoCheckpointUpdate(checkpointId, CHECKPOINT_INTERVAL / 2);
        ValidatePublicationsCount(/* count */ 1, queryName, *sdkClient);
        EnsureTopicEndOffset(outputTopicName, /* endOffset */ 1, LocalTopics);

        WaitCheckpointUpdate(checkpointId);
        ValidatePublicationsCount(/* count */ 0, queryName, *sdkClient);
        ReadTopicMessage(outputTopicName, "message2", disposition, LocalTopics);

        // Query should not fail on double publication commit
        UNIT_ASSERT_VALUES_EQUAL(GetStreamingQueryIssues(queryName), "{}");

        DropTopic(inputTopicName, LocalTopics);
        DropTopic(outputTopicName, LocalTopics);
    }

    Y_UNIT_TEST_F(DeferredPublicationCommitFailure, TStreamingWithSchemaSecretsTestFixture) {
        SetupAppConfig().MutableFeatureFlags()->SetEnableExactlyOnceTopicsWriting(true);
        const auto pqGateway = SetupMockPqGateway();

        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");

        const auto inputTopicName = TStringBuilder() << Name_ << "InputTopicName";
        const auto outputTopicName = TStringBuilder() << Name_ << "OutputTopicName";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "pqSourceName";
        CreatePqSourceBasicAuth(pqSourceName, /* useSchemaSecrets  */ true);

        constexpr TDuration CHECKPOINT_INTERVAL = TDuration::Seconds(10);
        const auto queryName = TStringBuilder() << Name_ << "StreamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` WITH (
                CHECKPOINT_INTERVAL = "PT{checkpoint_interval}S"
            ) AS
            DO BEGIN
                INSERT INTO `{pq_source}`.`{output_topic}` WITH (DELIVERY_GUARANTEE = "exactly_once")
                SELECT * FROM `{pq_source}`.`{input_topic}`;
            END DO;)",
            "query_name"_a = queryName,
            "checkpoint_interval"_a = CHECKPOINT_INTERVAL.Seconds(),
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        const auto& checkpointId = GetStreamingQueryCheckpointId(queryName);
        auto& publicationController = pqGateway->GetDeferredPublishClientController();

        publicationController.LockCommits();
        pqGateway->WaitReadSession(inputTopicName)->AddDataReceivedEvent(0, "message1");
        const auto seqNo = CheckNoCheckpointUpdate(checkpointId, CHECKPOINT_INTERVAL / 2);
        publicationController.EnsureOpenedPublications(/* count */ 1, queryName);

        publicationController.WaitCommits(/* count */ 1);
        CheckNoCheckpointUpdate(checkpointId, CHECKPOINT_INTERVAL, seqNo); // Checkpoint waits for publication commit
        auto writeSession = pqGateway->ExtractWriteSession(outputTopicName);
        writeSession->EnsureEmpty();
        publicationController.AcceptCommits(EStatus::UNAVAILABLE, {NIssue::TIssue("Test commit failure")});

        // Commit must be retried after query restart with correct publication id
        publicationController.WaitCommits(/* count */ 1);
        publicationController.UnlockCommits();
        publicationController.EnsureOpenedPublications(/* count */ 0, queryName);
        writeSession->ExpectMessage("message1");
        writeSession->ExpectSessionClosed();

        // Check that checkpointing works for restarted query
        pqGateway->WaitReadSession(inputTopicName)->AddDataReceivedEvent(0, "message2");
        writeSession = pqGateway->WaitWriteSession(outputTopicName);
        CheckNoCheckpointUpdate(checkpointId, CHECKPOINT_INTERVAL / 4);
        publicationController.EnsureOpenedPublications(/* count */ 1, queryName);
        writeSession->EnsureEmpty();

        WaitCheckpointUpdate(checkpointId);
        publicationController.EnsureOpenedPublications(/* count */ 0, queryName);
        writeSession->ExpectMessage("message2");

        {
            const auto& issues = GetStreamingQueryIssues(queryName);
            UNIT_ASSERT_STRING_CONTAINS(issues, "Failed to commit deferred publication #1. Status: UNAVAILABLE");
            UNIT_ASSERT_STRING_CONTAINS(issues, "Test commit failure");
        }

        DropTopic(inputTopicName);
        DropTopic(outputTopicName);
    }

    Y_UNIT_TEST_F(MessageWriteFailure, TStreamingWithSchemaSecretsTestFixture) {
        SetupAppConfig().MutableFeatureFlags()->SetEnableExactlyOnceTopicsWriting(true);
        const auto pqGateway = SetupMockPqGateway(TMockPqGatewaySettings{.LockWritingByDefault = true});

        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");

        const auto inputTopicName = TStringBuilder() << Name_ << "InputTopicName";
        const auto outputTopicName = TStringBuilder() << Name_ << "OutputTopicName";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "pqSourceName";
        CreatePqSourceBasicAuth(pqSourceName, /* useSchemaSecrets  */ true);

        constexpr TDuration CHECKPOINT_INTERVAL = TDuration::Seconds(10);
        const auto queryName = TStringBuilder() << Name_ << "StreamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` WITH (
                CHECKPOINT_INTERVAL = "PT{checkpoint_interval}S"
            ) AS
            DO BEGIN
                INSERT INTO `{pq_source}`.`{output_topic}` WITH (DELIVERY_GUARANTEE = "exactly_once")
                SELECT * FROM `{pq_source}`.`{input_topic}`;
            END DO;)",
            "query_name"_a = queryName,
            "checkpoint_interval"_a = CHECKPOINT_INTERVAL.Seconds(),
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        const auto& checkpointId = GetStreamingQueryCheckpointId(queryName);
        auto& publicationController = pqGateway->GetDeferredPublishClientController();

        pqGateway->WaitReadSession(inputTopicName)->AddDataReceivedEvent(0, "message1");
        const auto seqNo = CheckNoCheckpointUpdate(checkpointId, CHECKPOINT_INTERVAL / 2);
        publicationController.EnsureOpenedPublications(/* count */ 1, queryName);

        auto writeSession = pqGateway->WaitWriteSession(outputTopicName);
        writeSession->LockAcks();
        writeSession->Unlock();
        CheckNoCheckpointUpdate(checkpointId, CHECKPOINT_INTERVAL, seqNo); // Checkpoint waits for acks
        writeSession->EnsureEmpty();
        writeSession->UnlockAcks(TWriteSessionEvent::TWriteAck::EES_DISCARDED);

        // Check that checkpointing works for restarted query
        pqGateway->WaitReadSession(inputTopicName)->AddDataReceivedEvent(0, "message2");
        auto newWriteSession = pqGateway->WaitWriteSession(outputTopicName);
        newWriteSession->Unlock();
        CheckNoCheckpointUpdate(checkpointId, CHECKPOINT_INTERVAL / 4);
        publicationController.EnsureOpenedPublications(/* count */ 2, queryName);
        newWriteSession->EnsureEmpty();

        WaitCheckpointUpdate(checkpointId);
        publicationController.EnsureOpenedPublications(/* count */ 1, queryName);
        newWriteSession->ExpectMessage("message2");
        writeSession->EnsureEmpty(); // There is no commits on failed publication

        UNIT_ASSERT_STRING_CONTAINS(GetStreamingQueryIssues(queryName), "Message with seqNo 0 was discarded");

        DropTopic(inputTopicName);
        DropTopic(outputTopicName);
    }

    Y_UNIT_TEST_F(DeferredPublicationRefreshOnCheckpoint, TStreamingWithSchemaSecretsTestFixture) {
        SetupAppConfig().MutableFeatureFlags()->SetEnableExactlyOnceTopicsWriting(true);
        const auto pqGateway = SetupMockPqGateway(TMockPqGatewaySettings{.LockWritingByDefault = true});

        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");

        const auto inputTopicName = TStringBuilder() << Name_ << "InputTopicName";
        const auto outputTopicName = TStringBuilder() << Name_ << "OutputTopicName";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "pqSourceName";
        CreatePqSourceBasicAuth(pqSourceName, /* useSchemaSecrets  */ true);

        constexpr TDuration CHECKPOINT_INTERVAL = TDuration::Seconds(10);
        const auto queryName = TStringBuilder() << Name_ << "StreamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` WITH (
                CHECKPOINT_INTERVAL = "PT{checkpoint_interval}S"
            ) AS
            DO BEGIN
                INSERT INTO `{pq_source}`.`{output_topic}` WITH (DELIVERY_GUARANTEE = "exactly_once")
                SELECT * FROM `{pq_source}`.`{input_topic}`;
            END DO;)",
            "query_name"_a = queryName,
            "checkpoint_interval"_a = CHECKPOINT_INTERVAL.Seconds(),
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        const auto& checkpointId = GetStreamingQueryCheckpointId(queryName);
        auto& publicationController = pqGateway->GetDeferredPublishClientController();

        const auto readSession = pqGateway->WaitReadSession(inputTopicName);
        readSession->AddDataReceivedEvent(0, "message1");
        auto seqNo = CheckNoCheckpointUpdate(checkpointId, CHECKPOINT_INTERVAL / 2);
        publicationController.EnsureOpenedPublications(/* count */ 1, queryName);

        const auto writeSession = pqGateway->WaitWriteSession(outputTopicName);
        writeSession->LockAcks();
        writeSession->Unlock();
        writeSession->WaitAcks(/* count */ 1);
        writeSession->EnsureEmpty();

        writeSession->Lock();
        readSession->AddDataReceivedEvent(1, "message2");
        readSession->AddDataReceivedEvent(2, "message3");
        writeSession->WaitAcks(/* count */ 2);
        UNIT_ASSERT_VALUES_EQUAL(GetLastCheckpointSeqNo(checkpointId), seqNo);

        // Wait for new checkpoint start
        WaitFor(CHECKPOINT_INTERVAL, "checkpoint start", [&]() {
            return GetLastCheckpointSeqNo(checkpointId) != seqNo;
        });

        Sleep(TDuration::Seconds(1));
        publicationController.EnsureOpenedPublications(/* count */ 1, queryName);
        writeSession->EnsureEmpty();

        // Add few messages after pending checkpoint
        readSession->AddDataReceivedEvent(3, "message4");
        readSession->AddDataReceivedEvent(4, "message5");
        CheckNoCheckpointUpdate(checkpointId, CHECKPOINT_INTERVAL, seqNo);

        publicationController.LockCommits();
        writeSession->UnlockAcks();
        writeSession->Unlock();
        publicationController.WaitCommits(/* count */ 1);
        publicationController.EnsureOpenedPublications(/* count */ 2, queryName); // There should be another publication for new messages
        seqNo = CheckNoCheckpointUpdate(checkpointId, CHECKPOINT_INTERVAL, seqNo);
        writeSession->EnsureEmpty();

        publicationController.AcceptCommits(EStatus::SUCCESS);
        publicationController.WaitCommits(/* count */ 1);
        publicationController.EnsureOpenedPublications(/* count */ 1, queryName);
        writeSession->ExpectMessages({"message1", "message2", "message3"});
        UNIT_ASSERT(GetLastCheckpointSeqNo(checkpointId) == seqNo + 1);

        CheckNoCheckpointUpdate(checkpointId, CHECKPOINT_INTERVAL, seqNo);
        publicationController.UnlockCommits();
        publicationController.EnsureOpenedPublications(/* count */ 0, queryName);
        writeSession->ExpectMessages({"message4", "message5"});

        DropTopic(inputTopicName);
        DropTopic(outputTopicName);
    }
}

} // namespace NKikimr::NKqp
