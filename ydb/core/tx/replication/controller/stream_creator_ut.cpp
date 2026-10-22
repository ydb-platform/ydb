#include "dst_creator.h"
#include "private_events.h"
#include "stream_creator.h"
#include "target_table.h"

#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/replication/ut_helpers/test_table.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/json/json_reader.h>

namespace NKikimr::NReplication::NController {

Y_UNIT_TEST_SUITE(StreamCreator) {
    using namespace NTestHelpers;

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

    Y_UNIT_TEST(AdvertisesPayloadCodecsBeforeConsumerCreation) {
        for (const bool internal : {false, true}) {
            TEnv env(true, ERuntimeMode::Simulated);
            env.GetRuntime().GetAppData().FeatureFlags.SetTransferInternalDataDecompression(internal);
            auto& runtime = env.GetRuntime();
            env.CreateTable("/Root", *MakeTableDescription(TTestTableDescription{
                .Name = "Table", .KeyColumns = {"key"},
                .Columns = {{.Name = "key", .Type = "Uint32"}}, .ReplicationConfig = Nothing(),
            }));
            TActorId creator;
            THolder<IEventHandle> consumerRequest;
            auto blockConsumer = runtime.AddObserver<TEvYdbProxy::TEvAlterTopicRequest>([&](auto& ev) {
                if (ev->Sender == creator) {
                    consumerRequest.Reset(ev.Release());
                }
            });
            creator = runtime.Register(CreateStreamCreator(env.GetSender(), env.GetYdbProxy(), 7, 9,
                std::make_shared<TTargetTable::TTableConfig>("/Root/Table", "/Root/Replica"),
                "Stream", "replicationConsumer", TDuration::Hours(1), std::nullopt));
            runtime.EnableScheduleForActor(creator);
            const auto request = runtime.GrabEdgeEvent<TEvPrivate::TEvRequestCreateStream>(env.GetSender(), TDuration::Seconds(30));
            UNIT_ASSERT(request);
            runtime.Send(creator, env.GetSender(), new TEvPrivate::TEvAllowCreateStream());
            runtime.WaitFor("consumer request after persisted CDC creation", [&] { return bool(consumerRequest); }, TDuration::Seconds(30));
            const auto description = env.GetDescription("/Root/Table/Stream");
            TString attribute;
            for (const auto& attr : description.GetPathDescription().GetUserAttributes()) {
                if (attr.GetKey() == "__async_replication") {
                    attribute = attr.GetValue();
                }
            }
            NJson::TJsonValue json;
            UNIT_ASSERT_C(NJson::ReadJsonTree(attribute, &json), attribute);
            UNIT_ASSERT_VALUES_EQUAL(json["path"].GetString(), "/Root/Replica");
            UNIT_ASSERT_VALUES_EQUAL(json["id"].GetString(), "7");
            UNIT_ASSERT(json["supports_topic_autopartitioning"].IsBoolean());
            UNIT_ASSERT(json["supported_payload_codecs"].IsArray());
            UNIT_ASSERT_VALUES_EQUAL(json["supported_payload_codecs"].GetArraySafe().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(json["supported_payload_codecs"][0].GetString(), "gzip");
            blockConsumer.Remove();
            runtime.Send(consumerRequest.Release());
            const auto result = runtime.GrabEdgeEvent<TEvPrivate::TEvCreateStreamResult>(env.GetSender(), TDuration::Seconds(60));
            UNIT_ASSERT(result);
            UNIT_ASSERT_C(result->Get()->IsSuccess(), result->Get()->Status.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(ReusesLegacyStreamWithoutAdvertisingPayloadCodecs) {
        TEnv env(true, ERuntimeMode::Simulated);
        auto& runtime = env.GetRuntime();
        env.CreateTable("/Root", *MakeTableDescription(TTestTableDescription{
            .Name = "Table", .KeyColumns = {"key"},
            .Columns = {{.Name = "key", .Type = "Uint32"}}, .ReplicationConfig = Nothing(),
        }));
        const TString legacy = R"({"path":"/Root/Replica","id":"7","supports_topic_autopartitioning":false})";
        const auto created = env.Send<TEvYdbProxy::TEvAlterTableResponse>(env.GetYdbProxy(),
            new TEvYdbProxy::TEvAlterTableRequest("/Root/Table", NYdb::NTable::TAlterTableSettings()
                .AppendAddChangefeeds(NYdb::NTable::TChangefeedDescription("Stream",
                    NYdb::NTable::EChangefeedMode::Updates, NYdb::NTable::EChangefeedFormat::Json)
                    .AddAttribute("__async_replication", legacy))));
        UNIT_ASSERT_C(created->Get()->Result.IsSuccess(), created->Get()->Result.GetIssues().ToString());
        const auto getAttribute = [&] {
            const auto description = env.GetDescription("/Root/Table/Stream");
            for (const auto& attr : description.GetPathDescription().GetUserAttributes()) {
                if (attr.GetKey() == "__async_replication") {
                    return attr.GetValue();
                }
            }
            return TString();
        };
        UNIT_ASSERT_VALUES_EQUAL(getAttribute(), legacy);
        const auto registered = env.Send<TEvYdbProxy::TEvAlterTopicResponse>(env.GetYdbProxy(),
            new TEvYdbProxy::TEvAlterTopicRequest("/Root/Table/Stream", NYdb::NTopic::TAlterTopicSettings()
                .BeginAddConsumer("replicationConsumer").EndAddConsumer()));
        UNIT_ASSERT(registered);
        UNIT_ASSERT_C(registered->Get()->Result.IsSuccess(), registered->Get()->Result.GetIssues().ToString());
        const auto creator = runtime.Register(CreateStreamCreator(env.GetSender(), env.GetYdbProxy(), 7, 9,
            std::make_shared<TTargetTable::TTableConfig>("/Root/Table", "/Root/Replica"),
            "Stream", "replicationConsumer", TDuration::Hours(1), std::nullopt, false, false));
        runtime.EnableScheduleForActor(creator);
        const auto result = runtime.GrabEdgeEvent<TEvPrivate::TEvCreateStreamResult>(env.GetSender(), TDuration::Seconds(60));
        UNIT_ASSERT(result);
        UNIT_ASSERT_C(result->Get()->IsSuccess(), result->Get()->Status.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(getAttribute(), legacy);
        const auto consumer = env.Send<TEvYdbProxy::TEvDescribeConsumerResponse>(env.GetYdbProxy(),
            new TEvYdbProxy::TEvDescribeConsumerRequest("/Root/Table/Stream", "replicationConsumer", {}));
        UNIT_ASSERT_C(consumer->Get()->Result.IsSuccess(), consumer->Get()->Result.GetIssues().ToString());
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
