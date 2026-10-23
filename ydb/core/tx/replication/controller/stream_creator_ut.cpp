#include "dst_creator.h"
#include "private_events.h"
#include "stream_creator.h"
#include "target_table.h"
#include "target_transfer.h"

#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/replication/ut_helpers/test_table.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NReplication::NController {

Y_UNIT_TEST_SUITE(StreamCreator) {
    using namespace NTestHelpers;

    enum class EAcknowledgedSource { Enabled, Disabled };
    enum class EAcknowledgedConsistency { Row, Global };
    enum class EAcknowledgedQuota { Disabled, Enabled };

    void AcknowledgedRetentionCreationAndFallback(EAcknowledgedSource source,
            EAcknowledgedConsistency consistency,
            NKikimrReplication::TReplicationDefaults::ESourceTopicRetentionPolicy retentionPolicy,
            EAcknowledgedQuota quota = EAcknowledgedQuota::Enabled) {
        TEnv env(true, ERuntimeMode::Simulated);
        auto& runtime = env.GetRuntime();
        for (ui32 node = 0; node < runtime.GetNodeCount(); ++node) {
            runtime.GetAppData(node).FeatureFlags.SetEnableReplicationTopicAcknowledgedRetention(source == EAcknowledgedSource::Enabled);
            runtime.GetAppData(node).FeatureFlags.SetEnableTopicDiskSubDomainQuota(quota == EAcknowledgedQuota::Enabled);
        }
        env.CreateTable("/Root", *MakeTableDescription(TTestTableDescription{
            .Name = "Table", .KeyColumns = {"key"}, .Columns = {{.Name = "key", .Type = "Uint32"}},
            .ReplicationConfig = Nothing(),
        }));
        ui64 addConsumers = 0;
        auto observe = runtime.AddObserver<TEvYdbProxy::TEvAlterTopicRequest>([&](auto&) { ++addConsumers; });
        const auto creator = runtime.Register(CreateStreamCreator(
            env.GetSender(), env.GetYdbProxy(), 1, 1,
            std::make_shared<TTargetTable::TTableConfig>("/Root/Table", "/Root/Replica"),
            "Stream", "replicationConsumer", TDuration::Days(1),
            consistency == EAcknowledgedConsistency::Global ? std::make_optional(TDuration::Seconds(10)) : std::nullopt,
            false, true, false, true, false, retentionPolicy));
        runtime.EnableScheduleForActor(creator);
        const auto requested = runtime.GrabEdgeEvent<TEvPrivate::TEvRequestCreateStream>(env.GetSender(), TDuration::Seconds(30));
        UNIT_ASSERT(requested);
        runtime.Send(creator, env.GetSender(), new TEvPrivate::TEvAllowCreateStream());
        const auto result = runtime.GrabEdgeEvent<TEvPrivate::TEvCreateStreamResult>(env.GetSender(), TDuration::Seconds(60));
        UNIT_ASSERT(result);
        UNIT_ASSERT_C(result->Get()->IsSuccess(), result->Get()->Status.GetIssues().ToString());
        const auto config = env.GetDescription("/Root/Table/Stream/streamImpl")
            .GetPathDescription().GetPersQueueGroup().GetPQTabletConfig();
        UNIT_ASSERT_VALUES_EQUAL(config.GetPartitionConfig().GetLifetimeSeconds(), 86400);
        UNIT_ASSERT_VALUES_EQUAL(config.ConsumersSize(), 1);
        const bool acknowledged = source == EAcknowledgedSource::Enabled
            && quota == EAcknowledgedQuota::Enabled
            && retentionPolicy == NKikimrReplication::TReplicationDefaults::SOURCE_TOPIC_RETENTION_ACKNOWLEDGED;
        UNIT_ASSERT_VALUES_EQUAL(config.GetConsumers(0).GetImportant(), acknowledged);
        UNIT_ASSERT_VALUES_EQUAL(addConsumers, acknowledged ? 0 : 1);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<i32>(config.GetPartitionConfig().GetRetentionPolicy()), static_cast<i32>(acknowledged
            ? NKikimrPQ::TPartitionConfig::RETENTION_POLICY_REPLICATION_ACKNOWLEDGED
            : NKikimrPQ::TPartitionConfig::RETENTION_POLICY_STANDARD));
        if (!acknowledged) {
            UNIT_ASSERT(!config.GetPartitionConfig().HasRetentionConsumer());
            UNIT_ASSERT_VALUES_EQUAL(config.GetConsumers(0).GetReadFromTimestampsMs(), 0);
        }
        if (acknowledged) {
            // Reproduce an old destination replaying Creating after the source
            // already committed the atomic ACK topic configuration.
            const auto legacy = runtime.Register(CreateStreamCreator(
                env.GetSender(), env.GetYdbProxy(), 1, 1,
                std::make_shared<TTargetTable::TTableConfig>("/Root/Table", "/Root/Replica"),
                "Stream", "replicationConsumer", TDuration::Days(1),
                consistency == EAcknowledgedConsistency::Global ? std::make_optional(TDuration::Seconds(10)) : std::nullopt,
                false, true, false, true, false,
                NKikimrReplication::TReplicationDefaults::SOURCE_TOPIC_RETENTION_STANDARD));
            runtime.EnableScheduleForActor(legacy);
            UNIT_ASSERT(runtime.GrabEdgeEvent<TEvPrivate::TEvRequestCreateStream>(env.GetSender(), TDuration::Seconds(30)));
            runtime.Send(legacy, env.GetSender(), new TEvPrivate::TEvAllowCreateStream());
            const auto replayed = runtime.GrabEdgeEvent<TEvPrivate::TEvCreateStreamResult>(env.GetSender(), TDuration::Seconds(60));
            UNIT_ASSERT(replayed);
            UNIT_ASSERT_C(replayed->Get()->IsSuccess(), replayed->Get()->Status.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(addConsumers, 1);
            const auto preserved = env.GetDescription("/Root/Table/Stream/streamImpl")
                .GetPathDescription().GetPersQueueGroup().GetPQTabletConfig();
            UNIT_ASSERT_VALUES_EQUAL(preserved.ConsumersSize(), 1);
            UNIT_ASSERT(preserved.GetConsumers(0).GetImportant());
            UNIT_ASSERT_VALUES_EQUAL(static_cast<i32>(preserved.GetPartitionConfig().GetRetentionPolicy()), static_cast<i32>(NKikimrPQ::TPartitionConfig::RETENTION_POLICY_REPLICATION_ACKNOWLEDGED));
        }
    }

    Y_UNIT_TEST(AcknowledgedRetentionCreationAndFallback) {
        for (auto source : {EAcknowledgedSource::Enabled, EAcknowledgedSource::Disabled}) {
            for (auto consistency : {EAcknowledgedConsistency::Row, EAcknowledgedConsistency::Global}) {
                AcknowledgedRetentionCreationAndFallback(source, consistency,
                    NKikimrReplication::TReplicationDefaults::SOURCE_TOPIC_RETENTION_ACKNOWLEDGED);
            }
        }
        AcknowledgedRetentionCreationAndFallback(EAcknowledgedSource::Enabled, EAcknowledgedConsistency::Row,
            NKikimrReplication::TReplicationDefaults::SOURCE_TOPIC_RETENTION_STANDARD);
    }

    Y_UNIT_TEST(AcknowledgedRetentionQuotaDisabledFallsBack) {
        for (auto consistency : {EAcknowledgedConsistency::Row, EAcknowledgedConsistency::Global}) {
            AcknowledgedRetentionCreationAndFallback(EAcknowledgedSource::Enabled, consistency,
                NKikimrReplication::TReplicationDefaults::SOURCE_TOPIC_RETENTION_ACKNOWLEDGED,
                EAcknowledgedQuota::Disabled);
        }
    }

    Y_UNIT_TEST(AcknowledgedRetentionExcludedTargetsRemainStandard) {
        for (auto kind : {TReplication::ETargetKind::IndexTable, TReplication::ETargetKind::Transfer}) {
            TEnv env(true, ERuntimeMode::Simulated);
            auto& runtime = env.GetRuntime();
            for (ui32 node = 0; node < runtime.GetNodeCount(); ++node) {
                runtime.GetAppData(node).FeatureFlags.SetEnableReplicationTopicAcknowledgedRetention(true);
                runtime.GetAppData(node).FeatureFlags.SetEnableChangefeedsOnIndexTables(true);
                runtime.GetAppData(node).FeatureFlags.SetEnableTopicDiskSubDomainQuota(false);
            }
            TReplication::ITarget::IConfig::TPtr config;
            TString topic;
            if (kind == TReplication::ETargetKind::IndexTable) {
                NYdb::NTable::TTableClient client(env.GetDriver());
                const auto created = env.RunCall([&] {
                    auto result = client.CreateSession().GetValueSync();
                    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                    auto session = result.GetSession();
                    return session.ExecuteSchemeQuery(R"(
                        CREATE TABLE `/Root/Table` (key Uint32, value Utf8, PRIMARY KEY(key),
                            INDEX by_value GLOBAL ON(value));
                    )").GetValueSync();
                });
                UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
                config = std::make_shared<TTargetIndexTable::TIndexTableConfig>("/Root/Table/by_value", "/Root/Replica/by_value");
                topic = "/Root/Table/by_value/indexImplTable/Stream/streamImpl";
            } else {
                NYdb::NTopic::TTopicClient client(env.GetDriver());
                const auto created = env.RunCall([&] { return client.CreateTopic("/Root/Topic", {}).GetValueSync(); });
                UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
                config = std::make_shared<TTargetTransfer::TTransferConfig>("/Root/Topic", "/Root/Replica",
                    NKikimrReplication::TReplicationConfig());
                topic = "/Root/Topic";
            }
            const auto creator = runtime.Register(CreateStreamCreator(env.GetSender(), env.GetYdbProxy(), 1, 1,
                config, "Stream", "replicationConsumer", TDuration::Days(1), std::nullopt,
                false, true, false, false, false,
                NKikimrReplication::TReplicationDefaults::SOURCE_TOPIC_RETENTION_ACKNOWLEDGED));
            runtime.EnableScheduleForActor(creator);
            UNIT_ASSERT(runtime.GrabEdgeEvent<TEvPrivate::TEvRequestCreateStream>(env.GetSender(), TDuration::Seconds(30)));
            runtime.Send(creator, env.GetSender(), new TEvPrivate::TEvAllowCreateStream());
            const auto result = runtime.GrabEdgeEvent<TEvPrivate::TEvCreateStreamResult>(env.GetSender(), TDuration::Seconds(60));
            UNIT_ASSERT(result);
            UNIT_ASSERT_C(result->Get()->IsSuccess(), result->Get()->Status.GetIssues().ToString());
            const auto persisted = env.GetDescription(topic).GetPathDescription().GetPersQueueGroup().GetPQTabletConfig();
            UNIT_ASSERT_VALUES_EQUAL(static_cast<i32>(persisted.GetPartitionConfig().GetRetentionPolicy()), static_cast<i32>(NKikimrPQ::TPartitionConfig::RETENTION_POLICY_STANDARD));
            UNIT_ASSERT_VALUES_EQUAL(persisted.ConsumersSize(), 1);
            UNIT_ASSERT(!persisted.GetConsumers(0).GetImportant());
        }
    }

    enum class EBootstrapStage {
        Changefeed,
        Consumer,
    };

    void MissingSourceDnsName(EBootstrapStage stage) {
        TEnv env(true, ERuntimeMode::Simulated);
        auto& runtime = env.GetRuntime();
        env.CreateTable("/Root", *MakeTableDescription(TTestTableDescription{
            .Name = "Table",
            .KeyColumns = {"key"},
            .Columns = {{.Name = "key", .Type = "Uint32"}},
            .ReplicationConfig = Nothing(),
        }));

        TActorId creator;
        TVector<TInstant> attempts;
        const auto requestType = stage == EBootstrapStage::Changefeed
            ? TEvYdbProxy::TEvAlterTableRequest::EventType
            : TEvYdbProxy::TEvAlterTopicRequest::EventType;
        auto observer = runtime.AddObserver([&](IEventHandle::TPtr& ev) {
            if (ev->Sender != creator || ev->GetTypeRewrite() != requestType) {
                return;
            }
            attempts.push_back(runtime.GetCurrentTime());
            if (attempts.size() > 2) {
                return;
            }
            NYdb::NIssue::TIssues issues;
            issues.AddIssue("GRpc error: (14): DNS resolution failed: C-ares status is not ARES_SUCCESS qtype=AAAA: Domain name not found");
            NYdb::TStatus status(NYdb::EStatus::TRANSPORT_UNAVAILABLE, std::move(issues));
            IEventBase* response = stage == EBootstrapStage::Changefeed
                ? static_cast<IEventBase*>(new TEvYdbProxy::TEvAlterTableResponse(std::move(status)))
                : static_cast<IEventBase*>(new TEvYdbProxy::TEvAlterTopicResponse(std::move(status)));
            runtime.Send(new IEventHandle(creator, env.GetYdbProxy(), response, 0, ev->Cookie));
            ev.Reset();
        });

        creator = runtime.Register(CreateStreamCreator(
            env.GetSender(), env.GetYdbProxy(), 1, 1,
            std::make_shared<TTargetTable::TTableConfig>("/Root/Table", "/Root/Replica"),
            "Stream", "replicationConsumer", TDuration::Hours(1), TDuration::Seconds(10)));
        runtime.EnableScheduleForActor(creator);
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvPrivate::TEvRequestCreateStream>(env.GetSender(), TDuration::Seconds(30)));
        runtime.Send(creator, env.GetSender(), new TEvPrivate::TEvAllowCreateStream());
        const auto result = runtime.GrabEdgeEvent<TEvPrivate::TEvCreateStreamResult>(
            env.GetSender(), TDuration::Seconds(60));
        UNIT_ASSERT(result);
        UNIT_ASSERT_C(result->Get()->IsSuccess(), result->Get()->Status.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(attempts.size(), 3);
        UNIT_ASSERT(attempts[1] - attempts[0] >= TDuration::Seconds(10));
        UNIT_ASSERT(attempts[2] - attempts[1] >= TDuration::Seconds(10));

        const auto desc = env.GetDescription("/Root/Table");
        const auto& streams = desc.GetPathDescription().GetTable().GetCdcStreams();
        UNIT_ASSERT_VALUES_EQUAL(streams.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(streams.at(0).GetName(), "Stream");
        env.SendAsync(env.GetYdbProxy(), new TEvYdbProxy::TEvDescribeTopicRequest("/Root/Table/Stream", {}));
        const auto topic = runtime.GrabEdgeEvent<TEvYdbProxy::TEvDescribeTopicResponse>(env.GetSender(), TDuration::Seconds(30));
        UNIT_ASSERT(topic);
        UNIT_ASSERT_C(topic->Get()->Result.IsSuccess(), topic->Get()->Result.GetIssues().ToString());
        const auto& consumers = topic->Get()->Result.GetTopicDescription().GetConsumers();
        UNIT_ASSERT_VALUES_EQUAL(consumers.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(consumers[0].GetConsumerName(), "replicationConsumer");
    }

    Y_UNIT_TEST(MissingSourceDnsNameDuringChangefeedCreationRetries) {
        MissingSourceDnsName(EBootstrapStage::Changefeed);
    }

    Y_UNIT_TEST(MissingSourceDnsNameDuringConsumerCreationRetries) {
        MissingSourceDnsName(EBootstrapStage::Consumer);
    }

    void SessionAcquisitionFailure(Ydb::StatusIds::StatusCode failure) {
        TEnv env(true, ERuntimeMode::Simulated);
        auto& runtime = env.GetRuntime();
        env.CreateTable("/Root", *MakeTableDescription(TTestTableDescription{
            .Name = "Table",
            .KeyColumns = {"key"},
            .Columns = {{.Name = "key", .Type = "Uint32"}},
            .ReplicationConfig = Nothing(),
        }));

        // No table operation has used this proxy, so GetSession must create a session.
        TSessionCreationFault fault(runtime, failure);
        TActorId creator;
        ui32 firstResponseType = 0;
        ui64 alterResponses = 0;
        std::optional<NYdb::TStatus> acquisitionStatus;
        auto observer = runtime.AddObserver([&](IEventHandle::TPtr& ev) {
            if (ev->Sender != env.GetYdbProxy() || ev->GetRecipientRewrite() != creator) {
                return;
            }
            if (!firstResponseType) {
                firstResponseType = ev->GetTypeRewrite();
            }
            if (ev->GetTypeRewrite() == TEvYdbProxy::TEvAlterTableResponse::EventType) {
                ++alterResponses;
                if (!acquisitionStatus) {
                    acquisitionStatus.emplace(ev->Get<TEvYdbProxy::TEvAlterTableResponse>()->Result);
                }
            }
        });

        creator = runtime.Register(CreateStreamCreator(
            env.GetSender(), env.GetYdbProxy(), 1, 1,
            std::make_shared<TTargetTable::TTableConfig>("/Root/Table", "/Root/Replica"),
            "Stream", "replicationConsumer", TDuration::Hours(1), std::nullopt));
        runtime.EnableScheduleForActor(creator);
        const auto request = runtime.GrabEdgeEvent<TEvPrivate::TEvRequestCreateStream>(
            env.GetSender(), TDuration::Seconds(30));
        UNIT_ASSERT(request);
        runtime.Send(creator, env.GetSender(), new TEvPrivate::TEvAllowCreateStream());
        runtime.WaitFor("first proxy response to stream creator", [&] { return firstResponseType != 0; },
            TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(firstResponseType, TEvYdbProxy::TEvAlterTableResponse::EventType);
        UNIT_ASSERT(acquisitionStatus);
        UNIT_ASSERT_VALUES_EQUAL(acquisitionStatus->GetIssues().Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(acquisitionStatus->GetIssues().begin()->GetMessage(), "injected GetSession failure");
        UNIT_ASSERT_VALUES_EQUAL(fault.SessionRequests, 1);
        UNIT_ASSERT_VALUES_EQUAL(fault.TableRequests, 0);

        const auto result = runtime.GrabEdgeEvent<TEvPrivate::TEvCreateStreamResult>(
            env.GetSender(), TDuration::Seconds(60));
        UNIT_ASSERT_C(result, "Stream creator must report completion after GetSession failure");
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->ReplicationId, 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->TargetId, 1);

        if (failure == Ydb::StatusIds::UNAVAILABLE) {
            UNIT_ASSERT_VALUES_EQUAL(acquisitionStatus->GetStatus(), NYdb::EStatus::UNAVAILABLE);
            UNIT_ASSERT_C(result->Get()->IsSuccess(), result->Get()->Status.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(alterResponses, 2);
            UNIT_ASSERT(fault.SessionRequests >= 2);
            const auto desc = env.GetDescription("/Root/Table");
            const auto& streams = desc.GetPathDescription().GetTable().GetCdcStreams();
            UNIT_ASSERT_VALUES_EQUAL(streams.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(streams.at(0).GetName(), "Stream");

            env.SendAsync(env.GetYdbProxy(), new TEvYdbProxy::TEvDescribeConsumerRequest(
                "/Root/Table/Stream", "replicationConsumer", {}));
            const auto consumer = runtime.GrabEdgeEvent<TEvYdbProxy::TEvDescribeConsumerResponse>(
                env.GetSender(), TDuration::Seconds(30));
            UNIT_ASSERT(consumer);
            UNIT_ASSERT_C(consumer->Get()->Result.IsSuccess(), consumer->Get()->Result.GetIssues().ToString());
        } else {
            UNIT_ASSERT_VALUES_EQUAL(acquisitionStatus->GetStatus(), NYdb::EStatus::UNAUTHORIZED);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status.GetStatus(), NYdb::EStatus::UNAUTHORIZED);
            UNIT_ASSERT_STRING_CONTAINS(result->Get()->Status.GetIssues().ToString(), "injected GetSession failure");
            UNIT_ASSERT_VALUES_EQUAL(alterResponses, 1);
            UNIT_ASSERT_VALUES_EQUAL(fault.SessionRequests, 1);
            UNIT_ASSERT_VALUES_EQUAL(fault.TableRequests, 0);
            UNIT_ASSERT(env.GetDescription("/Root/Table").GetPathDescription().GetTable().GetCdcStreams().empty());
        }
    }

    Y_UNIT_TEST(SessionAcquisitionFailureRetries) {
        SessionAcquisitionFailure(Ydb::StatusIds::UNAVAILABLE);
    }

    Y_UNIT_TEST(TerminalSessionAcquisitionFailureReplies) {
        SessionAcquisitionFailure(Ydb::StatusIds::UNAUTHORIZED);
    }

    void Basic(const std::optional<TDuration>& resolvedTimestamps = {}) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        const auto tableDesc = TTestTableDescription{
            .Name = "Table",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "key", .Type = "Uint32"},
                {.Name = "value", .Type = "Utf8"},
            },
            .ReplicationConfig = Nothing(),
        };

        env.CreateTable("/Root", *MakeTableDescription(tableDesc));

        env.GetRuntime().Register(CreateDstCreator(
            env.GetSender(), env.GetSchemeshardId("/Root/Table"), env.GetYdbProxy(),
            "/Root", env.GetPathId("/Root"),
            1 /* rid */, 1 /* tid */, TReplication::ETargetKind::Table, "/Root/Table", "/Root/Replica"
        ));
        {
            auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvCreateDstResult>(env.GetSender());
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrScheme::StatusSuccess);
        }

        env.GetRuntime().Register(CreateStreamCreator(
            env.GetSender(), env.GetYdbProxy(), 1 /* rid */, 1 /* tid */,
            std::make_shared<TTargetTable::TTableConfig>("/Root/Table", "/Root/Replica"),
            "Stream", "replicationConsumer", TDuration::Hours(1), resolvedTimestamps
        ));
        {
            auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvRequestCreateStream>(env.GetSender());
            env.GetRuntime().Send(ev->Sender, env.GetSender(), new TEvPrivate::TEvAllowCreateStream());
        }
        {
            auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvCreateStreamResult>(env.GetSender());
            UNIT_ASSERT(ev->Get()->IsSuccess());
            UNIT_ASSERT(!ev->Get()->SchemaChanges);
        }

        auto desc = env.GetDescription("/Root/Table");

        const auto& streams = desc.GetPathDescription().GetTable().GetCdcStreams();
        UNIT_ASSERT_VALUES_EQUAL(streams.size(), 1);

        const auto& stream = streams.at(0);
        UNIT_ASSERT_VALUES_EQUAL(stream.GetMode(), NKikimrSchemeOp::ECdcStreamModeUpdate);
        UNIT_ASSERT_VALUES_EQUAL(stream.GetFormat(), NKikimrSchemeOp::ECdcStreamFormatJson);
        UNIT_ASSERT(!stream.GetSchemaChanges());
        UNIT_ASSERT_VALUES_EQUAL(stream.GetVirtualTimestamps(), resolvedTimestamps.has_value());
        UNIT_ASSERT_VALUES_EQUAL(stream.GetResolvedTimestampsIntervalMs(), resolvedTimestamps.value_or(TDuration::Zero()).MilliSeconds());
    }

    Y_UNIT_TEST(Basic) {
        Basic();
    }

    Y_UNIT_TEST(WithResolvedTimestamps) {
        Basic(TDuration::Seconds(10));
    }

    Y_UNIT_TEST(WithoutInitialScan) {
        TEnv env;
        env.CreateTable("/Root", *MakeTableDescription(TTestTableDescription{
            .Name = "Table",
            .KeyColumns = {"key"},
            .Columns = {{.Name = "key", .Type = "Uint32"}},
            .ReplicationConfig = Nothing(),
        }));
        env.GetRuntime().Register(CreateStreamCreator(
            env.GetSender(), env.GetYdbProxy(), 1, 1,
            std::make_shared<TTargetTable::TTableConfig>("/Root/Table", "/Root/Restored"),
            "Stream", "replicationConsumer", TDuration::Hours(1), std::nullopt,
            false, true, false, false));
        auto request = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvRequestCreateStream>(env.GetSender());
        env.GetRuntime().Send(request->Sender, env.GetSender(), new TEvPrivate::TEvAllowCreateStream());
        UNIT_ASSERT(env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvCreateStreamResult>(env.GetSender())->Get()->IsSuccess());

        const auto stream = env.GetDescription("/Root/Table/Stream");
        UNIT_ASSERT_VALUES_EQUAL(stream.GetPathDescription().GetCdcStreamDescription().GetState(),
            NKikimrSchemeOp::ECdcStreamStateReady);
    }

    void TopicAutoPartitioning(bool enabled) {
        TEnv env(TFeatureFlags{});
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        env.CreateTable("/Root", *MakeTableDescription(TTestTableDescription{
            .Name = "Table",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "key", .Type = "Uint32"},
                {.Name = "value", .Type = "Utf8"},
            },
            .ReplicationConfig = Nothing(),
        }));

        env.GetRuntime().Register(CreateStreamCreator(
            env.GetSender(), env.GetYdbProxy(), 1 /* rid */, 1 /* tid */,
            std::make_shared<TTargetTable::TTableConfig>("/Root/Table", "/Root/Replica"),
            "Stream", "replicationConsumer", TDuration::Hours(1), std::nullopt, enabled
        ));
        {
            auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvRequestCreateStream>(env.GetSender());
            env.GetRuntime().Send(ev->Sender, env.GetSender(), new TEvPrivate::TEvAllowCreateStream());
        }
        {
            auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvCreateStreamResult>(env.GetSender());
            UNIT_ASSERT(ev->Get()->IsSuccess());
        }

        auto desc = env.GetDescription("/Root/Table/Stream/streamImpl");

        const auto& pqconfig = desc.GetPathDescription().GetPersQueueGroup().GetPQTabletConfig();
        const auto& strategy = pqconfig.GetPartitionStrategy();

        if (enabled) {
            UNIT_ASSERT_EQUAL(strategy.GetPartitionStrategyType(), NKikimrPQ::TPQTabletConfig::CAN_SPLIT);
        } else {
            UNIT_ASSERT_EQUAL(strategy.GetPartitionStrategyType(), NKikimrPQ::TPQTabletConfig::DISABLED);
        }
    }

    Y_UNIT_TEST(TopicAutoPartitioning) {
        TopicAutoPartitioning(true);
        TopicAutoPartitioning(false);
    }

    Y_UNIT_TEST(SchemaChangesDisableAutoPartitioning) {
        TEnv env;
        env.CreateTable("/Root", *MakeTableDescription(TTestTableDescription{
            .Name = "Table",
            .KeyColumns = {"key"},
            .Columns = {{.Name = "key", .Type = "Uint32"}},
            .ReplicationConfig = Nothing(),
        }));

        env.GetRuntime().Register(CreateStreamCreator(
            env.GetSender(), env.GetYdbProxy(), 1, 1,
            std::make_shared<TTargetTable::TTableConfig>("/Root/Table", "/Root/Replica"),
            "Stream", "replicationConsumer", TDuration::Hours(1), std::nullopt,
            true, true, true));
        auto request = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvRequestCreateStream>(env.GetSender());
        env.GetRuntime().Send(request->Sender, env.GetSender(), new TEvPrivate::TEvAllowCreateStream());
        const auto result = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvCreateStreamResult>(env.GetSender());
        UNIT_ASSERT(result->Get()->IsSuccess());
        UNIT_ASSERT(result->Get()->SchemaChanges);

        const auto table = env.GetDescription("/Root/Table");
        const auto& stream = table.GetPathDescription().GetTable().GetCdcStreams().at(0);
        UNIT_ASSERT(stream.GetSchemaChanges());

        const auto topic = env.GetDescription("/Root/Table/Stream/streamImpl");
        const auto& strategy = topic.GetPathDescription().GetPersQueueGroup().GetPQTabletConfig().GetPartitionStrategy();
        UNIT_ASSERT_EQUAL(strategy.GetPartitionStrategyType(), NKikimrPQ::TPQTabletConfig::DISABLED);
    }

    Y_UNIT_TEST(ExistingStreamKeepsActualSchemaChangeCapability) {
        TEnv env;
        env.CreateTable("/Root", *MakeTableDescription(TTestTableDescription{
            .Name = "Table",
            .KeyColumns = {"key"},
            .Columns = {{.Name = "key", .Type = "Uint32"}},
            .ReplicationConfig = Nothing(),
        }));
        for (const bool schemaChanges : {false, true}) {
            env.GetRuntime().Register(CreateStreamCreator(
                env.GetSender(), env.GetYdbProxy(), 1, 1,
                std::make_shared<TTargetTable::TTableConfig>("/Root/Table", "/Root/Replica"),
                "Stream", "replicationConsumer", TDuration::Hours(1), std::nullopt,
                false, true, schemaChanges));
            const auto request = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvRequestCreateStream>(env.GetSender());
            env.GetRuntime().Send(request->Sender, env.GetSender(), new TEvPrivate::TEvAllowCreateStream());
            const auto result = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvCreateStreamResult>(env.GetSender());
            UNIT_ASSERT_C(result->Get()->IsSuccess(), result->Get()->Status.GetIssues().ToString());
            UNIT_ASSERT(!result->Get()->SchemaChanges);
        }
    }
}

}
