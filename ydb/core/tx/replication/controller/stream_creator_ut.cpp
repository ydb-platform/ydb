#include "dst_creator.h"
#include "private_events.h"
#include "stream_creator.h"
#include "target_table.h"

#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/replication/ut_helpers/test_table.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NReplication::NController {

Y_UNIT_TEST_SUITE(StreamCreator) {
    using namespace NTestHelpers;

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
        }

        auto desc = env.GetDescription("/Root/Table");

        const auto& streams = desc.GetPathDescription().GetTable().GetCdcStreams();
        UNIT_ASSERT_VALUES_EQUAL(streams.size(), 1);

        const auto& stream = streams.at(0);
        UNIT_ASSERT_VALUES_EQUAL(stream.GetMode(), NKikimrSchemeOp::ECdcStreamModeUpdate);
        UNIT_ASSERT_VALUES_EQUAL(stream.GetFormat(), NKikimrSchemeOp::ECdcStreamFormatJson);
        UNIT_ASSERT_VALUES_EQUAL(stream.GetVirtualTimestamps(), resolvedTimestamps.has_value());
        UNIT_ASSERT_VALUES_EQUAL(stream.GetResolvedTimestampsIntervalMs(), resolvedTimestamps.value_or(TDuration::Zero()).MilliSeconds());
    }

    Y_UNIT_TEST(Basic) {
        Basic();
    }

    Y_UNIT_TEST(WithResolvedTimestamps) {
        Basic(TDuration::Seconds(10));
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
}

}
