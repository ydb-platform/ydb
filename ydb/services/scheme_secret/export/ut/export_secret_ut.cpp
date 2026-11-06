#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/tx/replication/ut_helpers/test_env.h>
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

    void CreateTopic(ui32 partitions = 1, bool important = true) {
        const auto created = topicClient.CreateTopic("/Root/export-topic", NYdb::NTopic::TCreateTopicSettings()
            .BeginConfigurePartitioningSettings()
                .MinActivePartitions(partitions)
                .MaxActivePartitions(partitions)
            .EndConfigurePartitioningSettings()
            .BeginAddConsumer("replicationConsumer")
                .Important(important)
            .EndAddConsumer()).GetValueSync();
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
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
    Y_UNIT_TEST(CreatesPrivateTopicAndParentOnSource) {
        TExportSecretEnv setup;
        setup.Execute("CREATE SECRET `source` WITH (value = 'one');");
        setup.AssertNoExportTopic("/Root/secretImpl");
        const auto exported = setup.client.ExportSecret("/Root/source", "/Root/secretImpl/export-topic").GetValueSync();
        UNIT_ASSERT_C(exported.IsSuccess(), exported.GetIssues().ToString());
        const auto directory = setup.schemeClient.DescribePath("/Root/secretImpl").GetValueSync();
        UNIT_ASSERT_C(directory.IsSuccess(), directory.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(directory.GetEntry().Type, NYdb::NScheme::ESchemeEntryType::Directory);
        const auto described = setup.DescribeTopic("/Root/secretImpl/export-topic");
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
        setup.AssertOffsets(topic, 1, 2);
        const auto records = setup.ReadRecords();
        UNIT_ASSERT_VALUES_EQUAL(records.at("/Root/source/first"), "one");
        UNIT_ASSERT_VALUES_EQUAL(records.at("/Root/source/nested/second"), "");
    }

    Y_UNIT_TEST(ReusesExistingTopicAndWritesToPartitionZero) {
        TExportSecretEnv setup;
        setup.Execute("CREATE SECRET `source/first` WITH (value = 'one');");
        setup.CreateTopic(4);
        for (const auto& path : {"/Root/source/first", "source//first"}) {
            const auto exported = setup.client.ExportSecret(path, "/Root/export-topic").GetValueSync();
            UNIT_ASSERT_C(exported.IsSuccess(), exported.GetIssues().ToString());
        }
        const auto described = setup.DescribeTopic();
        UNIT_ASSERT(!described.GetTopicDescription().GetInterruptInheritance());
        setup.AssertOffsets(described.GetTopicDescription(), 4, 1);
    }
}

}
