#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/persqueue/public/schema/schema.h>
#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/secret/secret.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NSecret {
namespace {

struct TExportSecretEnv {
    static NReplication::NTestHelpers::TFeatureFlags FeatureFlags() {
        NReplication::NTestHelpers::TFeatureFlags flags;
        flags.FeatureFlags.SetEnableSchemaSecrets(true);
        return flags;
    }

    TExportSecretEnv()
        : env(FeatureFlags())
        , tableClient(env.GetDriver())
        , session(tableClient.CreateSession().GetValueSync().GetSession())
        , client(env.GetDriver(), NYdb::TCommonClientSettings().AuthToken("root@builtin"))
        , schemeClient(env.GetDriver(), NYdb::TCommonClientSettings().AuthToken("root@builtin"))
        , topicClient(env.GetDriver(), NYdb::NTopic::TTopicClientSettings().AuthToken("root@builtin"))
    {
        env.GetRuntime().GetAppData(0).AdministrationAllowedSIDs = {"root@builtin"};
    }

    void Execute(const TString& query) {
        const auto status = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    }

    void SetReaderPermissions(const std::vector<std::string>& secretPermissions) {
        const auto database = schemeClient.ModifyPermissions("/Root", NYdb::NScheme::TModifyPermissionsSettings()
            .AddGrantPermissions(NYdb::NScheme::TPermissions("reader@builtin",
                {"ydb.database.connect", "ydb.granular.describe_schema"}))).GetValueSync();
        UNIT_ASSERT_C(database.IsSuccess(), database.GetIssues().ToString());
        auto secretSettings = NYdb::NScheme::TModifyPermissionsSettings()
            .AddClearAcl()
            .AddInterruptInheritance(true);
        if (!secretPermissions.empty()) {
            secretSettings.AddGrantPermissions(NYdb::NScheme::TPermissions("reader@builtin", secretPermissions));
        }
        const auto secret = schemeClient.ModifyPermissions("/Root/source", secretSettings).GetValueSync();
        UNIT_ASSERT_C(secret.IsSuccess(), secret.GetIssues().ToString());
        const auto topic = schemeClient.ModifyPermissions("/Root/export-topic", NYdb::NScheme::TModifyPermissionsSettings()
            .AddGrantPermissions(NYdb::NScheme::TPermissions("reader@builtin", {"ydb.granular.update_row"}))).GetValueSync();
        UNIT_ASSERT_C(topic.IsSuccess(), topic.GetIssues().ToString());
    }

    void CreateTopic(const NYdb::NTopic::TCreateTopicSettings& settings) {
        const auto created = topicClient.CreateTopic("/Root/export-topic", settings).GetValueSync();
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
    }

    void CreateTopic(ui32 partitions = 1, bool important = true) {
        CreateTopic(NYdb::NTopic::TCreateTopicSettings()
            .BeginConfigurePartitioningSettings()
                .MinActivePartitions(partitions)
                .MaxActivePartitions(partitions)
            .EndConfigurePartitioningSettings()
            .BeginAddConsumer("replicationConsumer")
                .Important(important)
            .EndAddConsumer());
    }

    auto DescribeTopic(const TString& path = "/Root/export-topic") {
        auto result = topicClient.DescribeTopic(path,
            NYdb::NTopic::TDescribeTopicSettings().IncludeStats(true)).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        return result;
    }

    void AssertNoExportTopic(const TString& parent) {
        UNIT_ASSERT_VALUES_EQUAL(schemeClient.DescribePath(parent).GetValueSync().GetStatus(), NYdb::EStatus::SCHEME_ERROR);
        UNIT_ASSERT_VALUES_EQUAL(topicClient.DescribeTopic(parent + "/export-topic").GetValueSync().GetStatus(),
            NYdb::EStatus::SCHEME_ERROR);
    }

    void AssertOffsets(const NYdb::NTopic::TTopicDescription& topic, ui32 partitions, ui64 endOffset) {
        UNIT_ASSERT_VALUES_EQUAL(topic.GetPartitions().size(), partitions);
        for (const auto& partition : topic.GetPartitions()) {
            UNIT_ASSERT(partition.GetPartitionStats());
            UNIT_ASSERT_VALUES_EQUAL(partition.GetPartitionStats()->GetEndOffset(),
                partition.GetPartitionId() == 0 ? endOffset : 0);
        }
    }

    auto ReadRecords() {
        auto reader = topicClient.CreateReadSession(NYdb::NTopic::TReadSessionSettings()
            .ConsumerName("replicationConsumer")
            .AppendTopics(NYdb::NTopic::TTopicReadSettings("/Root/export-topic").AppendPartitionIds(0)));
        THashMap<TString, TString> records;
        while (records.size() < 2) {
            auto event = reader->GetEvent(true);
            UNIT_ASSERT(event);
            if (auto* start = std::get_if<NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&*event)) {
                start->Confirm();
            } else if (auto* data = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*event)) {
                for (const auto& message : data->GetMessages()) {
                    NKikimrReplication::TSecretRecord record;
                    UNIT_ASSERT_VALUES_EQUAL(message.GetSeqNo(), 1);
                    UNIT_ASSERT(record.ParseFromString(TString{message.GetData()}));
                    UNIT_ASSERT(record.HasValue());
                    UNIT_ASSERT(records.emplace(record.GetSourcePath(), record.GetValue()).second);
                }
                data->Commit();
            } else if (std::holds_alternative<NYdb::NTopic::TSessionClosedEvent>(*event)) {
                UNIT_FAIL("Secret export read session closed unexpectedly");
            }
        }
        reader->Close(TDuration::Seconds(5));
        return records;
    }

    NReplication::NTestHelpers::TEnv<> env;
    NYdb::NTable::TTableClient tableClient;
    NYdb::NTable::TSession session;
    NYdb::NSecret::TSecretClient client;
    NYdb::NScheme::TSchemeClient schemeClient;
    NYdb::NTopic::TTopicClient topicClient;
};

}

Y_UNIT_TEST_SUITE(ExportSecret) {
    Y_UNIT_TEST(RetriesTopicCreationAndDelayedPublication) {
        NReplication::NTestHelpers::TEnv<> env(true, NReplication::NTestHelpers::ERuntimeMode::Simulated);
        auto& runtime = env.GetRuntime();
        runtime.GetAppData(0).FeatureFlags.SetEnableSchemaSecrets(true);
        runtime.GetAppData(0).AdministrationAllowedSIDs = {"root@builtin"};
        env.RunCall([&] {
            NYdb::NTable::TTableClient tableClient(env.GetDriver());
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            const auto created = session.ExecuteSchemeQuery("CREATE SECRET `source` WITH (value = 'one');").GetValueSync();
            UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
        });

        using TNavigate = NSchemeCache::TSchemeCacheNavigate;
        const TVector<TNavigate::EStatus> transientStatuses = {
            // First retry creation after its ambiguous result, then wait for publication.
            TNavigate::EStatus::PathErrorUnknown,
            TNavigate::EStatus::PathErrorUnknown,
            TNavigate::EStatus::TableCreationNotComplete,
            TNavigate::EStatus::LookupError,
        };
        size_t injectedStatuses = 0;
        TActorId exporter;
        auto navigationObserver = runtime.AddObserver<TEvTxProxySchemeCache::TEvNavigateKeySetResult>([&](auto& ev) {
            auto& request = *ev->Get()->Request;
            if (request.ResultSet.size() != 1) {
                return;
            }
            auto& entry = request.ResultSet.front();
            if (entry.Operation != TNavigate::OpTopic || entry.Path != TVector<TString>{"Root", "export-topic"}) {
                return;
            }
            exporter = ev->GetRecipientRewrite();
            if (entry.Status == TNavigate::EStatus::Ok && injectedStatuses < transientStatuses.size()) {
                entry.Status = transientStatuses[injectedStatuses++];
                request.ErrorCount = 1;
            }
        });
        bool creationResponseLost = false;
        auto creationObserver = runtime.AddObserver<NPQ::NSchema::TEvSchemaOperationResponse>([&](auto& ev) {
            if (ev->GetRecipientRewrite() == exporter && !creationResponseLost
                && ev->Get()->Status == Ydb::StatusIds::SUCCESS)
            {
                // The topic exists, but the caller must recover from an ambiguous creation result.
                creationResponseLost = true;
                ev->Get()->Status = Ydb::StatusIds::UNAVAILABLE;
            }
        });

        NYdb::NSecret::TSecretClient client(env.GetDriver(), NYdb::TCommonClientSettings().AuthToken("root@builtin"));
        const auto exported = env.RunCall([&] {
            return client.ExportSecret("/Root/source", "/Root/export-topic").GetValueSync();
        });
        UNIT_ASSERT_C(exported.IsSuccess(), exported.GetIssues().ToString());
        UNIT_ASSERT(creationResponseLost);
        UNIT_ASSERT_VALUES_EQUAL(injectedStatuses, transientStatuses.size());
    }

    Y_UNIT_TEST(ExportsDifferentSecretsConcurrentlyToOnePartition) {
        TExportSecretEnv setup;
        setup.Execute("CREATE SECRET `source/first` WITH (value = 'one');");
        setup.Execute("CREATE SECRET `source/second` WITH (value = '');");
        UNIT_ASSERT_VALUES_EQUAL(setup.topicClient.DescribeTopic("/Root/export-topic").GetValueSync().GetStatus(),
            NYdb::EStatus::SCHEME_ERROR);

        const auto first = setup.client.ExportSecret("/Root/source/first", "/Root/export-topic");
        const auto second = setup.client.ExportSecret("/Root/source/second", "/Root/export-topic");
        for (const auto& result : {first, second}) {
            const auto status = result.GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }
        for (const auto& path : {"source/first", "/Root/source//second"}) {
            const auto repeated = setup.client.ExportSecret(path, "/Root/export-topic").GetValueSync();
            UNIT_ASSERT_C(repeated.IsSuccess(), repeated.GetIssues().ToString());
        }

        setup.AssertOffsets(setup.DescribeTopic().GetTopicDescription(), 1, 2);
        const auto records = setup.ReadRecords();
        UNIT_ASSERT_VALUES_EQUAL(records.at("/Root/source/first"), "one");
        UNIT_ASSERT_VALUES_EQUAL(records.at("/Root/source/second"), "");
    }

    Y_UNIT_TEST(RejectsNonAdminWithSecretPermissions) {
        TExportSecretEnv setup;
        setup.Execute("CREATE SECRET `source` WITH (value = 'one');");
        setup.CreateTopic();
        setup.SetReaderPermissions({"ydb.generic.full"});
        NYdb::NSecret::TSecretClient reader(setup.env.GetDriver(),
            NYdb::TCommonClientSettings().AuthToken("reader@builtin"));

        const auto described = reader.DescribeSecret("/Root/source").GetValueSync();
        UNIT_ASSERT_C(described.IsSuccess(), described.GetIssues().ToString());
        const auto exported = reader.ExportSecret("/Root/source", "/Root/export-topic").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(exported.GetStatus(), NYdb::EStatus::UNAUTHORIZED);
        UNIT_ASSERT_STRING_CONTAINS(exported.GetIssues().ToString(), "Secret export requires a cluster administrator");
        setup.AssertOffsets(setup.DescribeTopic().GetTopicDescription(), 1, 0);

        const auto create = reader.ExportSecret("/Root/source", "/Root/secretImpl/export-topic").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(create.GetStatus(), NYdb::EStatus::UNAUTHORIZED);
        UNIT_ASSERT_STRING_CONTAINS(create.GetIssues().ToString(), "Secret export requires a cluster administrator");
        setup.AssertNoExportTopic("/Root/secretImpl");
    }

    Y_UNIT_TEST(ExportsForAdminWithoutSecretPermissions) {
        TExportSecretEnv setup;
        setup.Execute("CREATE SECRET `source` WITH (value = 'one');");
        setup.CreateTopic();
        setup.SetReaderPermissions({});
        setup.env.GetRuntime().GetAppData(0).AdministrationAllowedSIDs = {"root@builtin", "reader@builtin"};
        NYdb::NSecret::TSecretClient reader(setup.env.GetDriver(),
            NYdb::TCommonClientSettings().AuthToken("reader@builtin"));

        const auto described = reader.DescribeSecret("/Root/source").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(described.GetStatus(), NYdb::EStatus::UNAUTHORIZED);
        const auto exported = reader.ExportSecret("/Root/source", "/Root/export-topic").GetValueSync();
        UNIT_ASSERT_C(exported.IsSuccess(), exported.GetIssues().ToString());
        setup.AssertOffsets(setup.DescribeTopic().GetTopicDescription(), 1, 1);
    }

    Y_UNIT_TEST(RejectsDisabledSchemaSecretsBeforeTopicCreation) {
        TExportSecretEnv setup;
        setup.Execute("CREATE SECRET `source` WITH (value = 'one');");
        setup.env.GetRuntime().GetAppData(0).FeatureFlags.SetEnableSchemaSecrets(false);

        const auto exported = setup.client.ExportSecret("/Root/source", "/Root/secretImpl/export-topic").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(exported.GetStatus(), NYdb::EStatus::UNSUPPORTED);
        setup.AssertNoExportTopic("/Root/secretImpl");
    }

    Y_UNIT_TEST(CreatesPrivateTopicAndParentsOnSource) {
        TExportSecretEnv setup;
        setup.Execute("CREATE SECRET `source` WITH (value = 'one');");
        setup.AssertNoExportTopic("/Root/secretImpl");
        const auto exported = setup.client.ExportSecret("/Root/source", "/Root/secretImpl/nested/export-topic").GetValueSync();
        UNIT_ASSERT_C(exported.IsSuccess(), exported.GetIssues().ToString());
        for (const auto& path : {"/Root/secretImpl", "/Root/secretImpl/nested"}) {
            const auto directory = setup.schemeClient.DescribePath(path).GetValueSync();
            UNIT_ASSERT_C(directory.IsSuccess(), directory.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(directory.GetEntry().Type, NYdb::NScheme::ESchemeEntryType::Directory);
        }
        const auto described = setup.DescribeTopic("/Root/secretImpl/nested/export-topic");
        const auto& topic = described.GetTopicDescription();
        UNIT_ASSERT_VALUES_EQUAL(topic.GetOwner(), "root@builtin");
        UNIT_ASSERT(topic.GetInterruptInheritance());
        UNIT_ASSERT_VALUES_EQUAL(topic.GetPermissions().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(topic.GetPermissions().front().Subject, "root@builtin");
        UNIT_ASSERT_VALUES_EQUAL(topic.GetTotalPartitionsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(topic.GetConsumers().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(topic.GetConsumers().front().GetConsumerName(), "replicationConsumer");
        UNIT_ASSERT(topic.GetConsumers().front().GetImportant());
        UNIT_ASSERT_VALUES_EQUAL(topic.GetRetentionPeriod(), TDuration::Seconds(
            setup.env.GetRuntime().GetAppData(0).ReplicationConfig.GetRetentionPeriodSeconds()));
        setup.AssertOffsets(topic, 1, 1);
    }

    Y_UNIT_TEST(RejectsNonAdminCallerCreatingTopic) {
        TExportSecretEnv setup;
        setup.Execute("CREATE SECRET `source` WITH (value = 'one');");
        setup.env.GetRuntime().GetAppData(0).AdministrationAllowedSIDs = {"admin@builtin"};
        const auto exported = setup.client.ExportSecret("/Root/source", "/Root/secretImpl/export-topic").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(exported.GetStatus(), NYdb::EStatus::UNAUTHORIZED);
        setup.AssertNoExportTopic("/Root/secretImpl");
    }

    Y_UNIT_TEST(RejectsNonAdminOwnerEvenForAdminCaller) {
        TExportSecretEnv setup;
        setup.Execute("CREATE SECRET `source` WITH (value = 'one');");
        setup.CreateTopic();
        const auto changed = setup.schemeClient.ModifyPermissions("/Root/export-topic", NYdb::NScheme::TModifyPermissionsSettings()
            .AddGrantPermissions(NYdb::NScheme::TPermissions("root@builtin", {"ydb.generic.full"}))
            .AddChangeOwner("ordinary@builtin")).GetValueSync();
        UNIT_ASSERT_C(changed.IsSuccess(), changed.GetIssues().ToString());
        const auto exported = setup.client.ExportSecret("/Root/source", "/Root/export-topic").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(exported.GetStatus(), NYdb::EStatus::UNAUTHORIZED);
        const auto described = setup.DescribeTopic();
        UNIT_ASSERT_VALUES_EQUAL(described.GetTopicDescription().GetOwner(), "ordinary@builtin");
        setup.AssertOffsets(described.GetTopicDescription(), 1, 0);
    }

    Y_UNIT_TEST(RejectsExistingTopicWithOrdinaryConsumer) {
        TExportSecretEnv setup;
        setup.Execute("CREATE SECRET `source` WITH (value = 'one');");
        setup.CreateTopic(1, false);
        const auto exported = setup.client.ExportSecret("/Root/source", "/Root/export-topic").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(exported.GetStatus(), NYdb::EStatus::PRECONDITION_FAILED);
        const auto described = setup.DescribeTopic();
        const auto& topic = described.GetTopicDescription();
        UNIT_ASSERT_VALUES_EQUAL(topic.GetOwner(), "root@builtin");
        UNIT_ASSERT_VALUES_EQUAL(topic.GetConsumers().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(topic.GetConsumers().front().GetConsumerName(), "replicationConsumer");
        UNIT_ASSERT(!topic.GetConsumers().front().GetImportant());
        setup.AssertOffsets(topic, 1, 0);
    }

    Y_UNIT_TEST(RejectsExistingTopicWithoutReplicationConsumer) {
        TExportSecretEnv setup;
        setup.Execute("CREATE SECRET `source` WITH (value = 'one');");
        setup.CreateTopic(NYdb::NTopic::TCreateTopicSettings()
            .BeginAddConsumer("otherConsumer")
                .Important(true)
            .EndAddConsumer());
        const auto exported = setup.client.ExportSecret("/Root/source", "/Root/export-topic").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(exported.GetStatus(), NYdb::EStatus::PRECONDITION_FAILED);
        setup.AssertOffsets(setup.DescribeTopic().GetTopicDescription(), 1, 0);
    }

    Y_UNIT_TEST(RejectsExistingTopicWithSharedReplicationConsumer) {
        TExportSecretEnv setup;
        setup.Execute("CREATE SECRET `source` WITH (value = 'one');");
        setup.CreateTopic(NYdb::NTopic::TCreateTopicSettings()
            .BeginAddSharedConsumer("replicationConsumer")
                .Important(true)
            .EndAddConsumer());
        const auto exported = setup.client.ExportSecret("/Root/source", "/Root/export-topic").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(exported.GetStatus(), NYdb::EStatus::PRECONDITION_FAILED);
        const auto described = setup.DescribeTopic();
        const auto& topic = described.GetTopicDescription();
        UNIT_ASSERT_VALUES_EQUAL(topic.GetConsumers().size(), 1);
        UNIT_ASSERT(topic.GetConsumers().front().GetConsumerType() == NYdb::NTopic::EConsumerType::Shared);
        UNIT_ASSERT(topic.GetConsumers().front().GetImportant());
        setup.AssertOffsets(topic, 1, 0);
    }

    Y_UNIT_TEST(RejectsExistingTopicWithFilteredReplicationConsumer) {
        TExportSecretEnv setup;
        setup.Execute("CREATE SECRET `source` WITH (value = 'one');");
        setup.CreateTopic(NYdb::NTopic::TCreateTopicSettings()
            .BeginAddConsumer("replicationConsumer")
                .Important(true)
                .ReadFrom(TInstant::Now() + TDuration::Days(1))
            .EndAddConsumer());
        const auto exported = setup.client.ExportSecret("/Root/source", "/Root/export-topic").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(exported.GetStatus(), NYdb::EStatus::PRECONDITION_FAILED);
        setup.AssertOffsets(setup.DescribeTopic().GetTopicDescription(), 1, 0);
    }

    Y_UNIT_TEST(DeduplicatesCanonicalPathAndPreservesEmptyValue) {
        TExportSecretEnv setup;
        setup.Execute("CREATE SECRET `source/first` WITH (value = 'one');");
        setup.Execute("CREATE SECRET `source/nested/second` WITH (value = '');");
        setup.CreateTopic();
        for (const auto& path : {"/Root/source/first", "/Root/source/nested/second", "source/first", "/Root/source//first"}) {
            const auto status = setup.client.ExportSecret(path, "/Root/export-topic").GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }
        UNIT_ASSERT_VALUES_EQUAL(setup.client.ExportSecret("/Other/secret", "/Root/export-topic").GetValueSync().GetStatus(),
            NYdb::EStatus::BAD_REQUEST);
        UNIT_ASSERT_VALUES_EQUAL(setup.client.ExportSecret("/Root/source/first", "/Other/export-topic").GetValueSync().GetStatus(),
            NYdb::EStatus::BAD_REQUEST);
        UNIT_ASSERT(!setup.client.ExportSecret("/Root/source/first", "/Root/source/nested/second").GetValueSync().IsSuccess());
        UNIT_ASSERT(!setup.client.ExportSecret("/Root/missing", "/Root/missingImpl/export-topic").GetValueSync().IsSuccess());
        setup.AssertNoExportTopic("/Root/missingImpl");
        const auto described = setup.DescribeTopic();
        const auto& topic = described.GetTopicDescription();
        UNIT_ASSERT_VALUES_EQUAL(topic.GetTotalPartitionsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(topic.GetConsumers().size(), 1);
        UNIT_ASSERT(topic.GetConsumers().front().GetImportant());
        UNIT_ASSERT_VALUES_EQUAL(topic.GetOwner(), "root@builtin");
        UNIT_ASSERT(!topic.GetInterruptInheritance());
        setup.AssertOffsets(topic, 1, 2);
        const auto records = setup.ReadRecords();
        UNIT_ASSERT_VALUES_EQUAL(records.at("/Root/source/first"), "one");
        UNIT_ASSERT_VALUES_EQUAL(records.at("/Root/source/nested/second"), "");
    }

    Y_UNIT_TEST(RejectsExistingTopicWithMultiplePartitions) {
        TExportSecretEnv setup;
        setup.Execute("CREATE SECRET `source/first` WITH (value = 'one');");
        setup.CreateTopic(4);
        const auto exported = setup.client.ExportSecret("/Root/source/first", "/Root/export-topic").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(exported.GetStatus(), NYdb::EStatus::PRECONDITION_FAILED);
        const auto described = setup.DescribeTopic();
        UNIT_ASSERT(!described.GetTopicDescription().GetInterruptInheritance());
        setup.AssertOffsets(described.GetTopicDescription(), 4, 0);
    }

    Y_UNIT_TEST(RejectsExistingTopicWithAutoPartitioning) {
        TExportSecretEnv setup;
        setup.Execute("CREATE SECRET `source` WITH (value = 'one');");
        setup.CreateTopic(NYdb::NTopic::TCreateTopicSettings()
            .BeginConfigurePartitioningSettings()
                .MinActivePartitions(1)
                .MaxActivePartitions(2)
                .BeginConfigureAutoPartitioningSettings()
                    .Strategy(NYdb::NTopic::EAutoPartitioningStrategy::ScaleUp)
                .EndConfigureAutoPartitioningSettings()
            .EndConfigurePartitioningSettings()
            .BeginAddConsumer("replicationConsumer")
                .Important(true)
            .EndAddConsumer());
        const auto exported = setup.client.ExportSecret("/Root/source", "/Root/export-topic").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(exported.GetStatus(), NYdb::EStatus::PRECONDITION_FAILED);
        setup.AssertOffsets(setup.DescribeTopic().GetTopicDescription(), 1, 0);
    }
}

}
