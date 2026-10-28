#include "dst_creator.h"
#include "private_events.h"
#include "stream_creator.h"
#include "target_table.h"
#include "target_transfer.h"

#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/public/api/protos/ydb_table.pb.h>

#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/replication/ut_helpers/test_table.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NReplication::NController {

Y_UNIT_TEST_SUITE(ReplicationTestEnv) {
    Y_UNIT_TEST(RunCallWaitsBeyondSimulatedMinute) {
        NTestHelpers::TEnv env(false, NTestHelpers::ERuntimeMode::Simulated);
        auto& runtime = env.GetRuntime();
        const auto edge = runtime.AllocateEdgeActor();
        const auto startedAt = runtime.GetCurrentTime();
        const auto callDelay = TDuration::Minutes(2);
        auto completion = NThreading::NewPromise<ui32>();
        auto observer = runtime.AddObserver<TEvents::TEvWakeup>([&](TEvents::TEvWakeup::TPtr& ev) {
            if (ev->GetRecipientRewrite() != edge) {
                return;
            }

            if (ev->Get()->Tag) {
                completion.SetValue(42);
            }
            ev.Reset();
        });

        // Visit a timestamp past the old deadline before allowing the call to finish.
        runtime.Schedule(new IEventHandle(edge, edge, new TEvents::TEvWakeup()), callDelay - TDuration::Seconds(1));
        runtime.Schedule(new IEventHandle(edge, edge, new TEvents::TEvWakeup(1)), callDelay);

        const auto result = env.RunCall([future = completion.GetFuture()] {
            return future.GetValueSync();
        });
        UNIT_ASSERT_VALUES_EQUAL(result, 42);
        UNIT_ASSERT(runtime.GetCurrentTime() - startedAt >= callDelay);
    }
}

Y_UNIT_TEST_SUITE(StreamCreator) {
    using namespace NTestHelpers;

    enum class EBootstrapStage {
        Changefeed,
        Consumer,
        Retention,
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
            if (stage != EBootstrapStage::Changefeed) {
                const auto& settings = std::get<NYdb::NTopic::TAlterTopicSettings>(
                    ev->Get<TEvYdbProxy::TEvAlterTopicRequest>()->GetArgs());
                if (settings.SetRetentionPeriod_.has_value() != (stage == EBootstrapStage::Retention)) {
                    return;
                }
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

    Y_UNIT_TEST(MissingSourceDnsNameDuringRetentionUpdateRetries) {
        MissingSourceDnsName(EBootstrapStage::Retention);
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

        const auto topic = env.GetDescription("/Root/Table/Stream/streamImpl");
        const auto& config = topic.GetPathDescription().GetPersQueueGroup().GetPQTabletConfig();
        UNIT_ASSERT_VALUES_EQUAL(config.GetPartitionConfig().GetLifetimeSeconds(), 1);
        UNIT_ASSERT_VALUES_EQUAL(config.ConsumersSize(), 1);
        UNIT_ASSERT(!config.GetConsumers(0).GetImportant());
        UNIT_ASSERT_VALUES_EQUAL(config.GetConsumers(0).GetAvailabilityPeriodMs(), TDuration::Days(1).MilliSeconds());
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
    class TStreamSetup {
    public:
        TTestActorRuntime Runtime;
        TActorId Parent;
        TActorId Proxy;
        TActorId Creator;
        bool RetentionRequested = false;
        TTestActorRuntime::TEventObserverHolder Observer;

        explicit TStreamSetup(bool transfer = false, TDuration retention = TDuration::Seconds(1),
                TDuration availability = TDuration::Days(1), bool describeOnly = false)
        {
            Runtime.Initialize(TAppPrepare().Unwrap());
            Parent = Runtime.AllocateEdgeActor();
            Proxy = Runtime.AllocateEdgeActor();
            Observer = Runtime.AddObserver<TEvYdbProxy::TEvAlterTopicRequest>([&](const auto& ev) {
                if (ev->Sender == Creator && Settings(ev).SetRetentionPeriod_) {
                    RetentionRequested = true;
                }
            });
            TReplication::ITarget::IConfig::TPtr config;
            if (transfer) {
                config = std::make_shared<TTargetTransfer::TTransferConfig>("/Root/Topic", "/Root/Replica",
                    NKikimrReplication::TReplicationConfig{});
            } else {
                config = std::make_shared<TTargetIndexTable::TIndexTableConfig>("/Root/Table/Index", "/Root/Replica");
            }
            Creator = Runtime.Register(CreateStreamCreator(Parent, Proxy, 1, 1, config,
                "Stream", "replicationConsumer", TDuration::Days(1), std::nullopt,
                false, true, false, true, describeOnly, retention, availability));
            Runtime.EnableScheduleForActor(Creator);
        }

        void Allow() {
            UNIT_ASSERT(Runtime.GrabEdgeEvent<TEvPrivate::TEvRequestCreateStream>(Parent));
            Runtime.Send(Creator, Parent, new TEvPrivate::TEvAllowCreateStream());
        }

        template <typename TResponse>
        void Reply(ui64 cookie, NYdb::EStatus status = NYdb::EStatus::SUCCESS) {
            Runtime.Send(new IEventHandle(Creator, Proxy,
                new TResponse(NYdb::TStatus(status, {})), 0, cookie));
        }

        auto CreateConsumer() {
            Allow();
            const auto request = Runtime.GrabEdgeEvent<TEvYdbProxy::TEvAlterTableRequest>(Proxy);
            const auto& settings = std::get<NYdb::NTable::TAlterTableSettings>(request->Get()->GetArgs());
            Ydb::Table::Changefeed changefeed;
            settings.AddChangefeeds_.at(0).SerializeTo(changefeed);
            UNIT_ASSERT_VALUES_EQUAL(changefeed.retention_period().seconds(), TDuration::Days(1).Seconds());
            Reply<TEvYdbProxy::TEvAlterTableResponse>(request->Cookie);
            return Runtime.GrabEdgeEvent<TEvYdbProxy::TEvAlterTopicRequest>(Proxy);
        }

        static const NYdb::NTopic::TAlterTopicSettings& Settings(const TEvYdbProxy::TEvAlterTopicRequest::TPtr& request) {
            return std::get<NYdb::NTopic::TAlterTopicSettings>(request->Get()->GetArgs());
        }

        auto VerifyConsumer(bool important = false, TDuration availability = TDuration::Days(1)) {
            const auto request = Runtime.GrabEdgeEvent<TEvYdbProxy::TEvDescribeTopicRequest>(Proxy);
            Ydb::Topic::DescribeTopicResult result;
            auto* consumer = result.add_consumers();
            consumer->set_name("replicationConsumer");
            consumer->set_important(important);
            consumer->mutable_availability_period()->set_seconds(availability.Seconds());
            Runtime.Send(new IEventHandle(Creator, Proxy, new TEvYdbProxy::TEvDescribeTopicResponse(
                NYdb::TStatus(NYdb::EStatus::SUCCESS, {}), std::move(result)), 0, request->Cookie));
            return request->Cookie;
        }

        void Finish(NYdb::EStatus status = NYdb::EStatus::SUCCESS) {
            const auto result = Runtime.GrabEdgeEvent<TEvPrivate::TEvCreateStreamResult>(Parent);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status.GetStatus(), status);
        }
    };

    Y_UNIT_TEST(FiniteAvailabilityBeforeShortRetention) {
        TStreamSetup setup(false, TDuration::Minutes(5), TDuration::Hours(2));
        const auto consumer = setup.CreateConsumer();
        const auto& settings = TStreamSetup::Settings(consumer);
        UNIT_ASSERT_VALUES_EQUAL(settings.AddConsumers_.size(), 1);
        UNIT_ASSERT(!settings.AddConsumers_[0].Important_);
        UNIT_ASSERT_VALUES_EQUAL(settings.AddConsumers_[0].AvailabilityPeriod_, TDuration::Hours(2));
        UNIT_ASSERT(!settings.SetRetentionPeriod_);
        setup.Reply<TEvYdbProxy::TEvAlterTopicResponse>(consumer->Cookie);
        setup.VerifyConsumer(false, TDuration::Hours(2));
        const auto retention = setup.Runtime.GrabEdgeEvent<TEvYdbProxy::TEvAlterTopicRequest>(setup.Proxy);
        UNIT_ASSERT_VALUES_EQUAL(*TStreamSetup::Settings(retention).SetRetentionPeriod_, TDuration::Minutes(5));
        UNIT_ASSERT(TStreamSetup::Settings(retention).AddConsumers_.empty());
        // Duplicate completion of an earlier operation must not finish retention setup.
        setup.Reply<TEvYdbProxy::TEvAlterTopicResponse>(consumer->Cookie, NYdb::EStatus::UNAUTHORIZED);
        setup.Reply<TEvYdbProxy::TEvAlterTopicResponse>(retention->Cookie);
        setup.Finish();
    }

    Y_UNIT_TEST(ExistingConsumerIsReconciledBeforeRetention) {
        TStreamSetup setup;
        const auto consumer = setup.CreateConsumer();
        setup.Reply<TEvYdbProxy::TEvAlterTopicResponse>(consumer->Cookie, NYdb::EStatus::ALREADY_EXISTS);
        const auto reconcile = setup.Runtime.GrabEdgeEvent<TEvYdbProxy::TEvAlterTopicRequest>(setup.Proxy);
        const auto& settings = TStreamSetup::Settings(reconcile);
        UNIT_ASSERT_VALUES_EQUAL(settings.AlterConsumers_.size(), 1);
        UNIT_ASSERT(settings.AlterConsumers_[0].SetImportant_.has_value());
        UNIT_ASSERT(!*settings.AlterConsumers_[0].SetImportant_);
        UNIT_ASSERT_VALUES_EQUAL(*settings.AlterConsumers_[0].SetAvailabilityPeriod_, TDuration::Days(1));
        UNIT_ASSERT(!settings.SetRetentionPeriod_);
        setup.Reply<TEvYdbProxy::TEvAlterTopicResponse>(reconcile->Cookie);
        setup.VerifyConsumer();
        const auto retention = setup.Runtime.GrabEdgeEvent<TEvYdbProxy::TEvAlterTopicRequest>(setup.Proxy);
        UNIT_ASSERT_VALUES_EQUAL(*TStreamSetup::Settings(retention).SetRetentionPeriod_, TDuration::Seconds(1));
        setup.Reply<TEvYdbProxy::TEvAlterTopicResponse>(retention->Cookie);
        setup.Finish();
    }

    Y_UNIT_TEST(ConsumerFailureKeepsBootstrapRetention) {
        for (const bool existing : {false, true}) {
            TStreamSetup setup;
            auto request = setup.CreateConsumer();
            if (existing) {
                setup.Reply<TEvYdbProxy::TEvAlterTopicResponse>(request->Cookie, NYdb::EStatus::ALREADY_EXISTS);
                request = setup.Runtime.GrabEdgeEvent<TEvYdbProxy::TEvAlterTopicRequest>(setup.Proxy);
                UNIT_ASSERT(!TStreamSetup::Settings(request).SetRetentionPeriod_);
            }
            setup.Reply<TEvYdbProxy::TEvAlterTopicResponse>(request->Cookie, NYdb::EStatus::UNAUTHORIZED);
            setup.Finish(NYdb::EStatus::UNAUTHORIZED);
            UNIT_ASSERT(!setup.RetentionRequested);
        }
    }

    Y_UNIT_TEST(IgnoredAvailabilityKeepsBootstrapRetention) {
        for (const bool important : {false, true}) {
            TStreamSetup setup;
            const auto consumer = setup.CreateConsumer();
            setup.Reply<TEvYdbProxy::TEvAlterTopicResponse>(consumer->Cookie);
            setup.VerifyConsumer(important, important ? TDuration::Days(1) : TDuration::Zero());
            setup.Finish(NYdb::EStatus::PRECONDITION_FAILED);
            UNIT_ASSERT(!setup.RetentionRequested);
        }
    }

    Y_UNIT_TEST(RetentionUpdateReplayedAfterProxyRefresh) {
        TStreamSetup setup;
        const auto consumer = setup.CreateConsumer();
        setup.Reply<TEvYdbProxy::TEvAlterTopicResponse>(consumer->Cookie);
        setup.VerifyConsumer();
        const auto oldRequest = setup.Runtime.GrabEdgeEvent<TEvYdbProxy::TEvAlterTopicRequest>(setup.Proxy);
        const auto oldProxy = setup.Proxy;
        setup.Proxy = setup.Runtime.AllocateEdgeActor();
        setup.Runtime.Send(setup.Creator, setup.Parent, new TEvPrivate::TEvUpdateYdbProxy(setup.Proxy));
        const auto request = setup.Runtime.GrabEdgeEvent<TEvYdbProxy::TEvAlterTopicRequest>(setup.Proxy);
        UNIT_ASSERT_VALUES_UNEQUAL(request->Cookie, oldRequest->Cookie);
        UNIT_ASSERT_VALUES_EQUAL(*TStreamSetup::Settings(request).SetRetentionPeriod_, TDuration::Seconds(1));
        setup.Runtime.Send(setup.Creator, oldProxy, new TEvents::TEvWakeup(oldRequest->Cookie));
        setup.Reply<TEvYdbProxy::TEvAlterTopicResponse>(oldRequest->Cookie, NYdb::EStatus::UNAUTHORIZED);
        setup.Reply<TEvYdbProxy::TEvAlterTopicResponse>(request->Cookie);
        setup.Finish();
    }

    Y_UNIT_TEST(RejectedRetentionDoesNotReportReady) {
        TStreamSetup setup;
        const auto consumer = setup.CreateConsumer();
        setup.Reply<TEvYdbProxy::TEvAlterTopicResponse>(consumer->Cookie);
        setup.VerifyConsumer();
        const auto retention = setup.Runtime.GrabEdgeEvent<TEvYdbProxy::TEvAlterTopicRequest>(setup.Proxy);
        setup.Reply<TEvYdbProxy::TEvAlterTopicResponse>(retention->Cookie, NYdb::EStatus::BAD_REQUEST);
        setup.Finish(NYdb::EStatus::BAD_REQUEST);
    }

    Y_UNIT_TEST(ZeroRetentionOrAvailabilityRejectedBeforeCreation) {
        for (const bool zeroAvailability : {false, true}) {
            TStreamSetup setup(false, zeroAvailability ? TDuration::Seconds(1) : TDuration::Zero(),
                zeroAvailability ? TDuration::Zero() : TDuration::Days(1));
            setup.Finish(NYdb::EStatus::BAD_REQUEST);
        }
    }

    Y_UNIT_TEST(TransferConsumerSettingsUnchanged) {
        for (const bool existing : {false, true}) {
            TStreamSetup setup(true);
            setup.Allow();
            const auto consumer = setup.Runtime.GrabEdgeEvent<TEvYdbProxy::TEvAlterTopicRequest>(setup.Proxy);
            const auto& settings = TStreamSetup::Settings(consumer);
            UNIT_ASSERT_VALUES_EQUAL(settings.AddConsumers_.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(settings.AddConsumers_[0].AvailabilityPeriod_, TDuration::Zero());
            UNIT_ASSERT(!settings.SetRetentionPeriod_);
            setup.Reply<TEvYdbProxy::TEvAlterTopicResponse>(consumer->Cookie,
                existing ? NYdb::EStatus::ALREADY_EXISTS : NYdb::EStatus::SUCCESS);
            setup.Finish();
        }
    }

}

}
