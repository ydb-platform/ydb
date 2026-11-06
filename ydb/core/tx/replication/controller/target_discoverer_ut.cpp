#include "private_events.h"
#include "target_discoverer.h"
#include "target_table.h"
#include "target_transfer.h"
#include "public_events.h"

#include <ydb/core/protos/metrics_config.pb.h>
#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/replication/ut_helpers/test_table.h>
#include <ydb/core/tx/replication/ut_helpers/test_topic.h>
#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/core/tx/replication/controller/protos/secret_copy.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/secret/secret.h>
#include <ydb/services/scheme_secret/resolver.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/string/builder.h>
#include <util/string/printf.h>
#include <util/system/datetime.h>

#include <atomic>


namespace NKikimr::NReplication::NController {

NKikimrReplication::TReplicationConfig CreateConfig(const TVector<std::pair<TString, TString>>& paths) {
    NKikimrReplication::TReplicationConfig config;

    auto& specific = *config.MutableSpecific();
    for (const auto& [src, dst] : paths) {
        auto& t = *specific.AddTargets();
        t.SetSrcPath(src);
        t.SetDstPath(dst);
    }

    return config;
}

NKikimrReplication::TReplicationConfig CreateTransferConfig(const std::tuple<TString, TString, TString>& path) {
    NKikimrReplication::TReplicationConfig config;

    const auto& [src, dst, lambda] = path;
    auto& specific = *config.MutableTransferSpecific();
    auto& t = *specific.MutableTarget();
    t.SetSrcPath(src);
    t.SetDstPath(dst);
    t.SetTransformLambda(lambda);

    return config;
}

Y_UNIT_TEST_SUITE(TargetDiscoverer) {
    using namespace NTestHelpers;

    TTestTableDescription DummyTable() {
        return TTestTableDescription{
            .Name = "Table",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "key", .Type = "Uint32"},
                {.Name = "value", .Type = "Uint32"},
            },
            .ReplicationConfig = Nothing(),
        };
    }

    TTestTopicDescription DummyTopic() {
        return TTestTopicDescription{
            .Name = "Topic",
        };
    }

    Y_UNIT_TEST(Basic) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        env.CreateTable("/Root", *MakeTableDescription(DummyTable()));

        env.GetRuntime().Register(CreateTargetDiscoverer(env.GetSender(), 1, env.GetYdbProxy(),
            CreateConfig(TVector<std::pair<TString, TString>>{
                {"/Root", "/Root/Replicated"},
            })
        ));

        auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvDiscoveryTargetsResult>(env.GetSender());
        UNIT_ASSERT(ev->Get()->IsSuccess());

        const auto& toAdd = ev->Get()->ToAdd;
        UNIT_ASSERT_VALUES_EQUAL(toAdd.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(toAdd.at(0).Config->GetSrcPath(), "/Root/Table");
        UNIT_ASSERT_VALUES_EQUAL(toAdd.at(0).Config->GetDstPath(), "/Root/Replicated/Table");
        UNIT_ASSERT_VALUES_EQUAL(toAdd.at(0).Kind, TReplication::ETargetKind::Table);
    }

    Y_UNIT_TEST(DiscoversSecretsInDirectory) {
        TFeatureFlags flags;
        flags.FeatureFlags.SetEnableSchemaSecrets(true);
        TEnv env(flags);
        NYdb::NTable::TTableClient client(env.GetDriver());
        auto session = client.CreateSession().GetValueSync().GetSession();
        for (const auto& query : {"CREATE SECRET first WITH (value = 'one');",
                "CREATE SECRET `nested/second` WITH (value = '');"})
        {
            const auto status = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }

        env.GetRuntime().Register(CreateTargetDiscoverer(env.GetSender(), 1, env.GetYdbProxy(),
            CreateConfig({{"/Root", "/Root/Replicated"}})));
        auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvDiscoveryTargetsResult>(env.GetSender());
        UNIT_ASSERT(ev->Get()->IsSuccess());
        THashMap<TString, TString> paths;
        for (const auto& target : ev->Get()->ToAdd) {
            paths.emplace(target.Config->GetSrcPath(), target.Config->GetDstPath());
        }
        UNIT_ASSERT_VALUES_EQUAL(paths.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(paths.at("/Root/first"), "/Root/Replicated/first");
        UNIT_ASSERT_VALUES_EQUAL(paths.at("/Root/nested/second"), "/Root/Replicated/nested/second");
    }

    void CreateSecretExportTopic(TEnv<>& env, const TString& path, ui32 partitions = 1) {
        env.GetRuntime().GetAppData(0).AdministrationAllowedSIDs = {"root@builtin"};
        NYdb::NTopic::TTopicClient topicClient(env.GetDriver(),
            NYdb::NTopic::TTopicClientSettings().AuthToken("root@builtin"));
        const auto created = topicClient.CreateTopic(path, NYdb::NTopic::TCreateTopicSettings()
            .BeginConfigurePartitioningSettings()
                .MinActivePartitions(partitions)
                .MaxActivePartitions(partitions)
            .EndConfigurePartitioningSettings()
            .BeginAddConsumer("replicationConsumer")
                .Important(true)
            .EndAddConsumer()).GetValueSync();
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
    }

    Y_UNIT_TEST(ExportSecretRequiresExistingTopic) {
        TFeatureFlags flags;
        flags.FeatureFlags.SetEnableSchemaSecrets(true);
        TEnv env(flags);
        NYdb::NTable::TTableClient tableClient(env.GetDriver());
        auto session = tableClient.CreateSession().GetValueSync().GetSession();
        const auto created = session.ExecuteSchemeQuery(
            "CREATE SECRET `source` WITH (value = 'one');").GetValueSync();
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
        NYdb::NSecret::TSecretClient client(env.GetDriver(), NYdb::TCommonClientSettings().AuthToken("root@builtin"));
        const auto exported = client.ExportSecret("/Root/source", "/Root/missing-topic").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(exported.GetStatus(), NYdb::EStatus::SCHEME_ERROR);
        NYdb::NTopic::TTopicClient topicClient(env.GetDriver(),
            NYdb::NTopic::TTopicClientSettings().AuthToken("root@builtin"));
        UNIT_ASSERT_VALUES_EQUAL(topicClient.DescribeTopic("/Root/missing-topic").GetValueSync().GetStatus(),
            NYdb::EStatus::SCHEME_ERROR);
    }

    Y_UNIT_TEST(ExportSecretRejectsNonAdminOwnerEvenForAdminCaller) {
        TFeatureFlags flags;
        flags.FeatureFlags.SetEnableSchemaSecrets(true);
        TEnv env(flags);
        NYdb::NTable::TTableClient tableClient(env.GetDriver());
        auto session = tableClient.CreateSession().GetValueSync().GetSession();
        const auto created = session.ExecuteSchemeQuery(
            "CREATE SECRET `source` WITH (value = 'one');").GetValueSync();
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
        CreateSecretExportTopic(env, "/Root/export-topic");
        NYdb::NScheme::TSchemeClient schemeClient(env.GetDriver(),
            NYdb::TCommonClientSettings().AuthToken("root@builtin"));
        const auto changed = schemeClient.ModifyPermissions("/Root/export-topic", NYdb::NScheme::TModifyPermissionsSettings()
            .AddGrantPermissions(NYdb::NScheme::TPermissions("root@builtin", {"ydb.generic.full"}))
            .AddChangeOwner("ordinary@builtin")).GetValueSync();
        UNIT_ASSERT_C(changed.IsSuccess(), changed.GetIssues().ToString());
        NYdb::NSecret::TSecretClient client(env.GetDriver(), NYdb::TCommonClientSettings().AuthToken("root@builtin"));
        const auto exported = client.ExportSecret("/Root/source", "/Root/export-topic").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(exported.GetStatus(), NYdb::EStatus::UNAUTHORIZED);
        NYdb::NTopic::TTopicClient topicClient(env.GetDriver(),
            NYdb::NTopic::TTopicClientSettings().AuthToken("root@builtin"));
        const auto described = topicClient.DescribeTopic("/Root/export-topic",
            NYdb::NTopic::TDescribeTopicSettings().IncludeStats(true)).GetValueSync();
        UNIT_ASSERT_C(described.IsSuccess(), described.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(described.GetTopicDescription().GetOwner(), "ordinary@builtin");
        UNIT_ASSERT(described.GetTopicDescription().GetPartitions().front().GetPartitionStats());
        UNIT_ASSERT_VALUES_EQUAL(described.GetTopicDescription().GetPartitions().front().GetPartitionStats()->GetEndOffset(), 0);
    }

    Y_UNIT_TEST(ExportsDifferentSecretsIntoOnePartitionAndDeduplicatesCanonicalPath) {
        TFeatureFlags flags;
        flags.FeatureFlags.SetEnableSchemaSecrets(true);
        TEnv env(flags);
        NYdb::NTable::TTableClient tableClient(env.GetDriver());
        auto session = tableClient.CreateSession().GetValueSync().GetSession();
        for (const auto& query : {"CREATE SECRET `source/first` WITH (value = 'one');",
                "CREATE SECRET `source/nested/second` WITH (value = '');"})
        {
            const auto status = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }
        CreateSecretExportTopic(env, "/Root/export-topic");
        NYdb::NSecret::TSecretClient client(env.GetDriver(), NYdb::TCommonClientSettings().AuthToken("root@builtin"));
        for (const auto& path : {"/Root/source/first", "/Root/source/nested/second",
                "source/first", "/Root/source//first"})
        {
            const auto status = client.ExportSecret(path, "/Root/export-topic").GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }
        const auto outside = client.ExportSecret("/Other/secret", "/Root/export-topic").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(outside.GetStatus(), NYdb::EStatus::BAD_REQUEST);
        const auto outsideTopic = client.ExportSecret("/Root/source/first", "/Other/export-topic").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(outsideTopic.GetStatus(), NYdb::EStatus::BAD_REQUEST);
        const auto notTopic = client.ExportSecret("/Root/source/first", "/Root/source/nested/second").GetValueSync();
        UNIT_ASSERT(!notTopic.IsSuccess());
        const auto missing = client.ExportSecret("/Root/missing", "/Root/missing-topic").GetValueSync();
        UNIT_ASSERT(!missing.IsSuccess());

        NYdb::NTopic::TTopicClient topicClient(env.GetDriver(),
            NYdb::NTopic::TTopicClientSettings().AuthToken("root@builtin"));
        UNIT_ASSERT_VALUES_EQUAL(topicClient.DescribeTopic("/Root/missing-topic").GetValueSync().GetStatus(),
            NYdb::EStatus::SCHEME_ERROR);
        const auto described = topicClient.DescribeTopic("/Root/export-topic",
            NYdb::NTopic::TDescribeTopicSettings().IncludeStats(true)).GetValueSync();
        UNIT_ASSERT_C(described.IsSuccess(), described.GetIssues().ToString());
        const auto& topic = described.GetTopicDescription();
        UNIT_ASSERT_VALUES_EQUAL(topic.GetTotalPartitionsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(topic.GetConsumers().size(), 1);
        UNIT_ASSERT(topic.GetConsumers().front().GetImportant());
        UNIT_ASSERT_VALUES_EQUAL(topic.GetOwner(), "root@builtin");
        UNIT_ASSERT(topic.GetPartitions().front().GetPartitionStats());
        UNIT_ASSERT_VALUES_EQUAL(topic.GetPartitions().front().GetPartitionStats()->GetEndOffset(), 2);

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
        UNIT_ASSERT_VALUES_EQUAL(records.at("/Root/source/first"), "one");
        UNIT_ASSERT_VALUES_EQUAL(records.at("/Root/source/nested/second"), "");
    }

    Y_UNIT_TEST(ExportSecretReusesExistingTopicAndWritesToPartitionZero) {
        TFeatureFlags flags;
        flags.FeatureFlags.SetEnableSchemaSecrets(true);
        TEnv env(flags);
        NYdb::NTable::TTableClient tableClient(env.GetDriver());
        auto session = tableClient.CreateSession().GetValueSync().GetSession();
        const auto created = session.ExecuteSchemeQuery(
            "CREATE SECRET `source/first` WITH (value = 'one');").GetValueSync();
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
        NYdb::NSecret::TSecretClient client(env.GetDriver(), NYdb::TCommonClientSettings().AuthToken("root@builtin"));
        NYdb::NTopic::TTopicClient topicClient(env.GetDriver(),
            NYdb::NTopic::TTopicClientSettings().AuthToken("root@builtin"));
        CreateSecretExportTopic(env, "/Root/export-topic", 4);
        for (const auto& path : {"/Root/source/first", "source//first"}) {
            const auto exported = client.ExportSecret(path, "/Root/export-topic").GetValueSync();
            UNIT_ASSERT_C(exported.IsSuccess(), exported.GetIssues().ToString());
        }
        const auto described = topicClient.DescribeTopic("/Root/export-topic",
            NYdb::NTopic::TDescribeTopicSettings().IncludeStats(true)).GetValueSync();
        UNIT_ASSERT_C(described.IsSuccess(), described.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(described.GetTopicDescription().GetPartitions().size(), 4);
        UNIT_ASSERT(!described.GetTopicDescription().GetInterruptInheritance());
        for (const auto& partition : described.GetTopicDescription().GetPartitions()) {
            UNIT_ASSERT(partition.GetPartitionStats());
            UNIT_ASSERT_VALUES_EQUAL(partition.GetPartitionStats()->GetEndOffset(),
                partition.GetPartitionId() == 0 ? 1 : 0);
        }
    }

    Y_UNIT_TEST(CopiesSecretsThroughOnePartitionAndKeepsTopic) {
        TFeatureFlags flags;
        flags.FeatureFlags.SetEnableSchemaSecrets(true);
        TEnv env(flags);
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_INFO);
        NYdb::NTable::TTableClient client(env.GetDriver());
        auto session = client.CreateSession().GetValueSync().GetSession();
        auto execute = [&](const TString& query) {
            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        };
        execute("CREATE SECRET `source/first` WITH (value = 'one');");
        execute("CREATE SECRET `source/nested/second` WITH (value = '');");

        std::atomic<ui32> preparedTopics = 0;
        std::atomic<bool> earlyExport = false;
        auto observer = env.GetRuntime().AddObserver([&](IEventHandle::TPtr& ev) {
            if (ev->GetTypeRewrite() == TEvYdbProxy::TEvCreateTopicResponse::EventType) {
                const auto& status = ev->Get<TEvYdbProxy::TEvCreateTopicResponse>()->Result;
                if (status.IsSuccess() || status.GetStatus() == NYdb::EStatus::ALREADY_EXISTS) {
                    ++preparedTopics;
                }
            } else if (ev->GetTypeRewrite() == TEvYdbProxy::TEvExportSecretRequest::EventType && preparedTopics < 2) {
                earlyExport = true;
            }
        });

        execute(TStringBuilder() << "CREATE ASYNC REPLICATION secret_replication "
            << "FOR source AS copy WITH ("
            << "CONNECTION_STRING = 'grpc://" << env.GetEndpoint() << "/?database=/Root', "
            << "TOKEN = 'root@builtin');");

        const auto deadline = TInstant::Now() + TDuration::Seconds(60);
        bool copied = false;
        while (TInstant::Now() < deadline) {
            const auto first = env.GetDescription("/Root/copy/first");
            const auto second = env.GetDescription("/Root/copy/nested/second");
            copied = first.GetSchemeStatus() == NKikimrScheme::StatusSuccess
                && second.GetSchemeStatus() == NKikimrScheme::StatusSuccess
                && first.GetPathDescription().GetSelf().GetCreateFinished()
                && second.GetPathDescription().GetSelf().GetCreateFinished();
            if (copied) {
                break;
            }
            Sleep(TDuration::MilliSeconds(100));
        }
        UNIT_ASSERT_C(copied, env.GetDescription("/Root/secret_replication").ShortDebugString());
        UNIT_ASSERT(!earlyExport);
        UNIT_ASSERT(preparedTopics >= 2);
        const auto values = NSecret::DescribeSecret({"/Root/copy/first", "/Root/copy/nested/second"},
            MakeIntrusiveConst<NACLib::TUserToken>("root@builtin", TVector<NACLib::TSID>()),
            "/Root", env.GetRuntime().GetActorSystem(0)).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(values.Status, Ydb::StatusIds::SUCCESS, values.Issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(values.SecretValues.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(values.SecretValues[0], "one");
        UNIT_ASSERT_VALUES_EQUAL(values.SecretValues[1], "");

        const auto description = env.GetDescription("/Root/secret_replication");
        auto request = MakeHolder<TEvController::TEvDescribeReplication>();
        env.GetPathId("/Root/secret_replication").ToProto(request->Record.MutablePathId());
        const auto state = env.Send<TEvController::TEvDescribeReplicationResult>(
            description.GetPathDescription().GetReplicationDescription().GetControllerId(), std::move(request));
        UNIT_ASSERT(state->Get()->Record.GetStatus() == NKikimrReplication::TEvDescribeReplicationResult::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(state->Get()->Record.TargetsSize(), 2);
        THashSet<TString> topics;
        for (const auto& target : state->Get()->Record.GetTargets()) {
            UNIT_ASSERT(!target.GetSrcStreamName().empty());
            topics.insert(target.GetSrcStreamName());
        }
        UNIT_ASSERT_VALUES_EQUAL(topics.size(), 1);
        const auto& topic = *topics.begin();
        NYdb::NTopic::TTopicClient topicClient(env.GetDriver(),
            NYdb::NTopic::TTopicClientSettings().AuthToken("root@builtin"));
        const auto exported = topicClient.DescribeTopic(topic,
            NYdb::NTopic::TDescribeTopicSettings().IncludeStats(true)).GetValueSync();
        UNIT_ASSERT_C(exported.IsSuccess(), exported.GetIssues().ToString());
        const auto& topicDescription = exported.GetTopicDescription();
        UNIT_ASSERT_VALUES_EQUAL(topicDescription.GetTotalPartitionsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(topicDescription.GetPartitions().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(topicDescription.GetConsumers().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(topicDescription.GetConsumers().front().GetConsumerName(), "replicationConsumer");
        UNIT_ASSERT(topicDescription.GetConsumers().front().GetImportant());
        for (const auto& partition : topicDescription.GetPartitions()) {
            UNIT_ASSERT(partition.GetPartitionStats());
            UNIT_ASSERT_VALUES_EQUAL(partition.GetPartitionStats()->GetStartOffset(), 0);
            UNIT_ASSERT_VALUES_EQUAL(partition.GetPartitionId(), 0);
            UNIT_ASSERT_VALUES_EQUAL(partition.GetPartitionStats()->GetEndOffset(), 2);
        }

        bool committed = false;
        while (TInstant::Now() < deadline) {
            const auto result = topicClient.DescribeConsumer(topic, "replicationConsumer",
                NYdb::NTopic::TDescribeConsumerSettings().IncludeStats(true)).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            const auto& partitions = result.GetConsumerDescription().GetPartitions();
            committed = partitions.size() == 1;
            for (const auto& partition : partitions) {
                committed &= partition.GetPartitionConsumerStats()
                    && partition.GetPartitionConsumerStats()->GetCommittedOffset() == 2;
            }
            if (committed) {
                break;
            }
            Sleep(TDuration::MilliSeconds(100));
        }
        UNIT_ASSERT_C(committed, "Secret records were not committed");
        execute("ALTER SECRET `source/first` WITH (value = 'changed');");
        const auto unchanged = NSecret::DescribeSecret({"/Root/copy/first"},
            MakeIntrusiveConst<NACLib::TUserToken>("root@builtin", TVector<NACLib::TSID>()),
            "/Root", env.GetRuntime().GetActorSystem(0)).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(unchanged.Status, Ydb::StatusIds::SUCCESS, unchanged.Issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(unchanged.SecretValues.at(0), "one");
        UNIT_ASSERT(topicClient.DescribeTopic(topic).GetValueSync().IsSuccess());

        execute("DROP ASYNC REPLICATION secret_replication;");
        const auto dropDeadline = TInstant::Now() + TDuration::Seconds(30);
        bool removed = false;
        while (TInstant::Now() < dropDeadline) {
            if (topicClient.DescribeTopic(topic).GetValueSync().GetStatus() == NYdb::EStatus::SCHEME_ERROR) {
                removed = true;
                break;
            }
            Sleep(TDuration::MilliSeconds(100));
        }
        UNIT_ASSERT_C(removed, "DROP REPLICATION did not remove the secret topic");
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/copy/first").GetSchemeStatus(),
            NKikimrScheme::StatusSuccess);
    }

    Y_UNIT_TEST(IndexedTable) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        env.CreateTableWithIndex("/Root", *MakeTableDescription(DummyTable()),
             "Index", TVector<TString>{"value"}, NKikimrSchemeOp::EIndexTypeGlobal);

        env.GetRuntime().Register(CreateTargetDiscoverer(env.GetSender(), 1, env.GetYdbProxy(),
            CreateConfig(TVector<std::pair<TString, TString>>{
                {"/Root", "/Root/Replicated"},
            })
        ));

        auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvDiscoveryTargetsResult>(env.GetSender());
        UNIT_ASSERT(ev->Get()->IsSuccess());

        const auto& toAdd = ev->Get()->ToAdd;
        UNIT_ASSERT_VALUES_EQUAL(toAdd.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(toAdd.at(1).Config->GetSrcPath(), "/Root/Table/Index");
        UNIT_ASSERT_VALUES_EQUAL(toAdd.at(1).Config->GetDstPath(), "/Root/Replicated/Table/Index/indexImplTable");
        UNIT_ASSERT_VALUES_EQUAL(toAdd.at(1).Kind, TReplication::ETargetKind::IndexTable);
    }

    Y_UNIT_TEST(Transfer) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        env.CreateTopic("/Root", *MakeTopicDescription(DummyTopic()));

        env.GetRuntime().Register(CreateTargetDiscoverer(env.GetSender(), 1, env.GetYdbProxy(),
            CreateTransferConfig(std::tuple<TString, TString, TString>{
                "/Root/Topic", "/Root/Replicated/Table", "lambda body"
            })
        ));

        auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvDiscoveryTargetsResult>(env.GetSender());
        UNIT_ASSERT(ev->Get()->IsSuccess());

        const auto& toAdd = ev->Get()->ToAdd;
        UNIT_ASSERT_VALUES_EQUAL(toAdd.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(toAdd.at(0).Config->GetSrcPath(), "/Root/Topic");
        UNIT_ASSERT_VALUES_EQUAL(toAdd.at(0).Config->GetDstPath(), "/Root/Replicated/Table");
        UNIT_ASSERT_VALUES_EQUAL(toAdd.at(0).Kind, TReplication::ETargetKind::Transfer);

        auto p = std::dynamic_pointer_cast<const TTargetTransfer::TTransferConfig>(toAdd.at(0).Config);
        UNIT_ASSERT(p);
        UNIT_ASSERT_VALUES_EQUAL(p->GetTransformLambda(), "lambda body");
    }

    Y_UNIT_TEST(MissingTransferSourceDnsNameRecoversAfterRetryBudget) {
        TEnv env(true, ERuntimeMode::Simulated);
        auto& runtime = env.GetRuntime();
        const auto created = env.RunCall([&] {
            return env.CreateTopic("/Root", *MakeTopicDescription(DummyTopic()));
        });
        UNIT_ASSERT_VALUES_EQUAL(created, NMsgBusProxy::MSTATUS_OK);

        TActorId discoverer;
        TVector<TInstant> attempts;
        auto observer = runtime.AddObserver<TEvYdbProxy::TEvDescribePathRequest>(
            [&](TEvYdbProxy::TEvDescribePathRequest::TPtr& ev) {
                if (ev->Sender != discoverer || ev->GetRecipientRewrite() != env.GetYdbProxy()
                    || std::get<0>(ev->Get()->GetArgs()) != "/Root/Topic")
                {
                    return;
                }

                attempts.push_back(runtime.GetCurrentTime());
                if (attempts.size() > 8) {
                    return;
                }

                NYdb::NIssue::TIssues issues;
                issues.AddIssue("GRpc error: (14): DNS resolution failed: Domain name not found");
                runtime.Send(new IEventHandle(ev->Sender, env.GetYdbProxy(),
                    new TEvYdbProxy::TEvDescribePathResponse(
                        NYdb::TStatus(NYdb::EStatus::TRANSPORT_UNAVAILABLE, std::move(issues)),
                        NYdb::NScheme::TSchemeEntry()),
                    0, ev->Cookie));
                ev.Reset();
            });

        discoverer = runtime.Register(CreateTargetDiscoverer(env.GetSender(), 1, env.GetYdbProxy(),
            CreateTransferConfig(std::tuple<TString, TString, TString>{
                "/Root/Topic", "/Root/Replicated/Table", "lambda body"
            })));
        runtime.EnableScheduleForActor(discoverer);

        const auto result = runtime.GrabEdgeEvent<TEvPrivate::TEvDiscoveryTargetsResult>(
            env.GetSender(), TDuration::Minutes(3));
        UNIT_ASSERT(result);
        UNIT_ASSERT_C(result->Get()->IsSuccess(),
            "Recoverable transfer source DNS failure must not exhaust the discovery retry budget");
        UNIT_ASSERT_VALUES_EQUAL(attempts.size(), 9);
        for (size_t i = 1; i < attempts.size(); ++i) {
            UNIT_ASSERT(attempts[i] > attempts[i - 1]);
        }

        const auto& targets = result->Get()->ToAdd;
        UNIT_ASSERT_VALUES_EQUAL(targets.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(targets.at(0).Kind, TReplication::ETargetKind::Transfer);
        UNIT_ASSERT_VALUES_EQUAL(targets.at(0).Config->GetSrcPath(), "/Root/Topic");
        UNIT_ASSERT_VALUES_EQUAL(targets.at(0).Config->GetDstPath(), "/Root/Replicated/Table");
        const auto config = std::dynamic_pointer_cast<const TTargetTransfer::TTransferConfig>(targets.at(0).Config);
        UNIT_ASSERT(config);
        UNIT_ASSERT_VALUES_EQUAL(config->GetTransformLambda(), "lambda body");
    }

    Y_UNIT_TEST(Negative) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        env.GetRuntime().Register(CreateTargetDiscoverer(env.GetSender(), 1, env.GetYdbProxy(),
            CreateConfig(TVector<std::pair<TString, TString>>{
                {"/Root/Table", "/Root/ReplicatedTable"},
            })
        ));

        auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvDiscoveryTargetsResult>(env.GetSender());
        UNIT_ASSERT(!ev->Get()->IsSuccess());

        const auto& failed = ev->Get()->Failed;
        UNIT_ASSERT_VALUES_EQUAL(failed.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(failed.at(0).SrcPath, "/Root/Table");
        UNIT_ASSERT_VALUES_EQUAL(failed.at(0).Error.GetStatus(), NYdb::EStatus::SCHEME_ERROR);
    }

    Y_UNIT_TEST(Dirs) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        env.MkDir("/Root", "Dir");
        env.CreateTable("/Root/Dir", *MakeTableDescription(DummyTable()));

        env.GetRuntime().Register(CreateTargetDiscoverer(env.GetSender(), 1, env.GetYdbProxy(),
            CreateConfig(TVector<std::pair<TString, TString>>{
                {"/Root", "/Root/Replicated"},
            })
        ));

        auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvDiscoveryTargetsResult>(env.GetSender());
        UNIT_ASSERT(ev->Get()->IsSuccess());

        const auto& toAdd = ev->Get()->ToAdd;
        UNIT_ASSERT_VALUES_EQUAL(toAdd.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(toAdd.at(0).Config->GetSrcPath(), "/Root/Dir/Table");
        UNIT_ASSERT_VALUES_EQUAL(toAdd.at(0).Config->GetDstPath(), "/Root/Replicated/Dir/Table");
    }

    Y_UNIT_TEST(InternalObjects) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        env.CreateTable("/Root", *MakeTableDescription(DummyTable()));
        for (const auto& directory : {
                 "~my_directory", "~backup", "user~backup", "~backup_12345",
                 ".sys_health", ".sys_user", ".metadata", ".metadata_user", ".tmp", ".tmp_user",
                 ".backups", ".backups_user", "export-100500", "export-user"}) {
            env.MkDir("/Root", directory);
            UNIT_ASSERT_VALUES_EQUAL_C(
                env.CreateTable(TStringBuilder() << "/Root/" << directory, *MakeTableDescription(DummyTable())),
                NMsgBusProxy::EResponseStatus::MSTATUS_OK, directory);
        }

        env.GetRuntime().Register(CreateTargetDiscoverer(env.GetSender(), 1, env.GetYdbProxy(),
            CreateConfig(TVector<std::pair<TString, TString>>{
                {"/Root", "/Root/Replicated"},
            })
        ));

        auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvDiscoveryTargetsResult>(env.GetSender());
        UNIT_ASSERT(ev->Get()->IsSuccess());

        const auto& toAdd = ev->Get()->ToAdd;
        const TVector<std::pair<TString, TString>> expectedPaths = {
            {"/Root/Table", "/Root/Replicated/Table"},
            {"/Root/~my_directory/Table", "/Root/Replicated/~my_directory/Table"},
            {"/Root/~backup/Table", "/Root/Replicated/~backup/Table"},
            {"/Root/user~backup/Table", "/Root/Replicated/user~backup/Table"},
        };
        UNIT_ASSERT_VALUES_EQUAL(toAdd.size(), expectedPaths.size());
        for (const auto& [src, dst] : expectedPaths) {
            UNIT_ASSERT_C(AnyOf(toAdd, [&](const auto& target) {
                return target.Config->GetSrcPath() == src && target.Config->GetDstPath() == dst;
            }), "Missing target: " << src << " -> " << dst);
        }
    }

    Y_UNIT_TEST(InvalidCredentials) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        env.CreateTable("/Root", *MakeTableDescription(DummyTable()));

        // create aux proxy
        NKikimrReplication::TStaticCredentials staticCreds;
        staticCreds.SetUser("user");
        staticCreds.SetPassword("password");
        const auto ydbProxy = env.GetRuntime().Register(CreateYdbProxy(
            env.GetEndpoint(), env.GetDatabase(), false /* ssl */, "" /* cert */, staticCreds));

        env.GetRuntime().Register(CreateTargetDiscoverer(env.GetSender(), 1, ydbProxy,
            CreateConfig(TVector<std::pair<TString, TString>>{
                {"/Root", "/Root/Replicated"},
            })
        ));

        auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvDiscoveryTargetsResult>(env.GetSender());
        UNIT_ASSERT(!ev->Get()->IsSuccess());

        const auto& failed = ev->Get()->Failed;
        UNIT_ASSERT_VALUES_EQUAL(failed.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(failed.at(0).Error.GetStatus(), NYdb::EStatus::CLIENT_UNAUTHENTICATED);
    }

    Y_UNIT_TEST(MissingSourceDnsNameRecoversAfterRetryBudget) {
        TEnv env(true, ERuntimeMode::Simulated);
        auto& runtime = env.GetRuntime();
        env.CreateTable("/Root", *MakeTableDescription(DummyTable()));
        auto otherTable = DummyTable();
        otherTable.Name = "OtherTable";
        env.CreateTable("/Root", *MakeTableDescription(otherTable));

        TActorId discoverer;
        TVector<TInstant> attempts;
        auto observer = runtime.AddObserver<TEvYdbProxy::TEvDescribePathRequest>(
            [&](TEvYdbProxy::TEvDescribePathRequest::TPtr& ev) {
                if (ev->Sender != discoverer || ev->GetRecipientRewrite() != env.GetYdbProxy()
                    || std::get<0>(ev->Get()->GetArgs()) != "/Root/Table")
                {
                    return;
                }

                // Keep the nonzero cookie so the other source can finish while
                // this source endpoint is temporarily absent from DNS.
                UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, 1);
                attempts.push_back(runtime.GetCurrentTime());
                if (attempts.size() > 8) {
                    return;
                }

                NYdb::NIssue::TIssues issues;
                issues.AddIssue("GRpc error: (14): DNS resolution failed: C-ares status is not ARES_SUCCESS qtype=AAAA: Domain name not found");
                runtime.Send(new IEventHandle(ev->Sender, env.GetYdbProxy(),
                    new TEvYdbProxy::TEvDescribePathResponse(
                        NYdb::TStatus(NYdb::EStatus::TRANSPORT_UNAVAILABLE, std::move(issues)),
                        NYdb::NScheme::TSchemeEntry()),
                    0, ev->Cookie));
                ev.Reset();
            });

        discoverer = runtime.Register(CreateTargetDiscoverer(env.GetSender(), 1, env.GetYdbProxy(),
            CreateConfig(TVector<std::pair<TString, TString>>{
                {"/Root/OtherTable", "/Root/OtherReplica"},
                {"/Root/Table", "/Root/Replica"},
            })));
        runtime.EnableScheduleForActor(discoverer);

        const auto result = runtime.GrabEdgeEvent<TEvPrivate::TEvDiscoveryTargetsResult>(
            env.GetSender(), TDuration::Minutes(3));
        UNIT_ASSERT(result);
        UNIT_ASSERT_C(result->Get()->IsSuccess(),
            "Recoverable source DNS failure must not exhaust the discovery retry budget");
        UNIT_ASSERT_VALUES_EQUAL(attempts.size(), 9);
        for (size_t i = 1; i < attempts.size(); ++i) {
            UNIT_ASSERT(attempts[i] > attempts[i - 1]);
        }

        const auto& targets = result->Get()->ToAdd;
        UNIT_ASSERT_VALUES_EQUAL(targets.size(), 2);
        UNIT_ASSERT(AnyOf(targets, [](const auto& target) {
            return target.Config->GetSrcPath() == "/Root/OtherTable"
                && target.Config->GetDstPath() == "/Root/OtherReplica";
        }));
        UNIT_ASSERT(AnyOf(targets, [](const auto& target) {
            return target.Config->GetSrcPath() == "/Root/Table"
                && target.Config->GetDstPath() == "/Root/Replica";
        }));
    }

    Y_UNIT_TEST(DropCancelsEndpointRetriesBeforeTargetDiscovery) {
        TEnv env(true, ERuntimeMode::Simulated);
        auto& runtime = env.GetRuntime();
        env.CreateTable("/Root", *MakeTableDescription(DummyTable()));
        NYdb::NTable::TTableClient client(env.GetDriver(), NYdb::NTable::TClientSettings()
            .DiscoveryEndpoint(env.GetEndpoint()).DiscoveryMode(NYdb::EDiscoveryMode::Async).Database(env.GetDatabase()));
        const auto sessionResult = env.RunCall([&] { return client.CreateSession().GetValueSync(); });
        UNIT_ASSERT_C(sessionResult.IsSuccess(), sessionResult.GetIssues().ToString());
        auto session = sessionResult.GetSession();

        ui64 requests = 0;
        auto observer = runtime.AddObserver<TEvYdbProxy::TEvDescribePathRequest>(
            [&](TEvYdbProxy::TEvDescribePathRequest::TPtr& ev) {
                if (std::get<0>(ev->Get()->GetArgs()) != "/Root/Table") {
                    return;
                }
                ++requests;
                runtime.EnableScheduleForActor(ev->Sender);
                NYdb::NIssue::TIssues issues;
                issues.AddIssue("Domain name not found");
                runtime.Send(new IEventHandle(ev->Sender, ev->Recipient,
                    new TEvYdbProxy::TEvDescribePathResponse(
                        NYdb::TStatus(NYdb::EStatus::TRANSPORT_UNAVAILABLE, std::move(issues)),
                        NYdb::NScheme::TSchemeEntry()), 0, ev->Cookie));
                ev.Reset();
            });
        const auto created = env.RunCall([&] {
            return session.ExecuteSchemeQuery(Sprintf(R"(
                CREATE ASYNC REPLICATION `/Root/replication`
                FOR `/Root/Table` AS `/Root/Replica`
                WITH (CONNECTION_STRING = "grpc://%s/?database=/Root", TOKEN = "root@builtin");
            )", env.GetEndpoint().c_str())).GetValueSync();
        });
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
        runtime.WaitFor("source discovery waiting for endpoint recovery", [&] { return requests > 0; }, TDuration::Seconds(30));

        const auto dropped = env.RunCall([&] {
            return session.ExecuteSchemeQuery("DROP ASYNC REPLICATION `/Root/replication` CASCADE;").GetValueSync();
        });
        UNIT_ASSERT_C(dropped.IsSuccess(), dropped.GetIssues().ToString());
        const auto requestsAfterDrop = requests;
        const auto edge = runtime.AllocateEdgeActor();
        runtime.Schedule(new IEventHandle(edge, env.GetSender(), new TEvents::TEvWakeup()), TDuration::Minutes(3));
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvents::TEvWakeup>(edge, TDuration::Minutes(4)));
        UNIT_ASSERT_VALUES_EQUAL(requests, requestsAfterDrop);

        const auto description = env.Send<TEvYdbProxy::TEvDescribePathResponse>(env.GetYdbProxy(),
            new TEvYdbProxy::TEvDescribePathRequest("/Root/replication", {}));
        UNIT_ASSERT_VALUES_EQUAL(description->Get()->Result.GetStatus(), NYdb::EStatus::SCHEME_ERROR);
    }

    void TerminalSourceDiscoveryError(NYdb::EStatus statusCode, const TString& message) {
        TEnv env(true, ERuntimeMode::Simulated);
        auto& runtime = env.GetRuntime();
        TActorId discoverer;
        size_t attempts = 0;
        auto observer = runtime.AddObserver<TEvYdbProxy::TEvDescribePathRequest>(
            [&](TEvYdbProxy::TEvDescribePathRequest::TPtr& ev) {
                if (ev->Sender != discoverer || ev->GetRecipientRewrite() != env.GetYdbProxy()) {
                    return;
                }

                ++attempts;
                NYdb::NIssue::TIssues issues;
                issues.AddIssue(message);
                runtime.Send(new IEventHandle(ev->Sender, env.GetYdbProxy(),
                    new TEvYdbProxy::TEvDescribePathResponse(
                        NYdb::TStatus(statusCode, std::move(issues)), NYdb::NScheme::TSchemeEntry()),
                    0, ev->Cookie));
                ev.Reset();
            });

        discoverer = runtime.Register(CreateTargetDiscoverer(env.GetSender(), 1, env.GetYdbProxy(),
            CreateConfig(TVector<std::pair<TString, TString>>{
                {"/Root/Table", "/Root/Replica"},
            })));
        runtime.EnableScheduleForActor(discoverer);

        const auto result = runtime.GrabEdgeEvent<TEvPrivate::TEvDiscoveryTargetsResult>(
            env.GetSender(), TDuration::Minutes(1));
        UNIT_ASSERT(result);
        UNIT_ASSERT(!result->Get()->IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(attempts, 1);
        const auto& failed = result->Get()->Failed;
        UNIT_ASSERT_VALUES_EQUAL(failed.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(failed[0].SrcPath, "/Root/Table");
        UNIT_ASSERT_VALUES_EQUAL(failed[0].Error.GetStatus(), statusCode);
        UNIT_ASSERT_VALUES_EQUAL(failed[0].Error.GetIssues().Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(failed[0].Error.GetIssues().begin()->GetMessage(), message);
    }

    Y_UNIT_TEST(MalformedSourceDomainRemainsTerminal) {
        TerminalSourceDiscoveryError(NYdb::EStatus::TRANSPORT_UNAVAILABLE,
            "GRpc error: (14): DNS resolution failed: Misformatted domain name");
    }

    Y_UNIT_TEST(SourceAuthenticationFailureRemainsTerminal) {
        TerminalSourceDiscoveryError(NYdb::EStatus::CLIENT_UNAUTHENTICATED, "Invalid source credentials");
    }

    Y_UNIT_TEST(SourceAuthorizationFailureRemainsTerminal) {
        TerminalSourceDiscoveryError(NYdb::EStatus::UNAUTHORIZED, "Source access denied");
    }

    Y_UNIT_TEST(InvalidSourceSettingsRemainTerminal) {
        TerminalSourceDiscoveryError(NYdb::EStatus::BAD_REQUEST, "Invalid source settings");
    }

    Y_UNIT_TEST(RetryableError) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        // create aux proxy
        const auto ydbProxy = env.GetRuntime().AllocateEdgeActor();

        auto discoveryActorId = env.GetRuntime().Register(CreateTargetDiscoverer(env.GetSender(), 1, ydbProxy,
            CreateConfig(TVector<std::pair<TString, TString>>{
                {"/Root", "/Root/Replicated"},
            })
        ));

        for (size_t i = 0; i <= 5; ++i) {
            NYdb::NIssue::TIssues issues;
            issues.AddIssue(TStringBuilder() << "iteration-" << i);
            NYdb::TStatus status(NYdb::EStatus::UNAVAILABLE, std::move(issues));
            env.SendAsync(discoveryActorId, new TEvYdbProxy::TEvDescribePathResponse(std::move(status), NYdb::NScheme::TSchemeEntry()));

            env.SendAsync(discoveryActorId, new TEvents::TEvWakeup());
        }

        auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvDiscoveryTargetsResult>(env.GetSender());
        UNIT_ASSERT(!ev->Get()->IsSuccess());

        const auto& failed = ev->Get()->Failed;
        UNIT_ASSERT_VALUES_EQUAL(failed.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(failed.at(0).Error.GetStatus(), NYdb::EStatus::UNAVAILABLE);
        UNIT_ASSERT_VALUES_EQUAL(failed.at(0).Error.GetIssues().ToOneLineString(), "{ <main>: Error: iteration-5 }");
    }
}

}
