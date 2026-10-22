#include "dst_creator.h"
#include "private_events.h"
#include "public_events.h"
#include "stream_creator.h"
#include "target_table.h"

#include <ydb/core/tx/replication/service/service.h>
#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/replication/ut_helpers/test_table.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash_set.h>
#include <util/string/printf.h>
#include <functional>

namespace NKikimr::NReplication::NController {

Y_UNIT_TEST_SUITE(StreamCreator) {
    using namespace NTestHelpers;

    enum class ECancelCase {
        HoldingPermit,
        QueuedCreatorAndStaleGrant,
        CommittedCreatorAndStaleResult,
    };

    enum class EEventGate { Hold, Forward };
    enum class EWorkerObservation { Setup, Resumed, Cleanup };

    enum class ECancelStage {
        InitialCreation,
        HoldingOriginalPoison,
        ReleaseOriginalPoison,
        Open,
    };

    void PauseDuringStreamCreation(ECancelCase scenario) {
        TEnv env(true, ERuntimeMode::Simulated);
        auto& runtime = env.GetRuntime();
        // The real SDK completes gRPC requests on foreign threads. Do not
        // exhaust simulated deadlines before those completions can run.
        runtime.SetFastSimulatedTime(false);
        runtime.SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);
        runtime.GetAppData().ReplicationConfig.MutableSchemeOperationLimits()->SetInflightCreateStreamLimit(1);
        const ui32 targetCount = scenario == ECancelCase::QueuedCreatorAndStaleGrant ? 2 : 1;
        const auto service = runtime.Register(CreateReplicationService());
        runtime.RegisterService(MakeReplicationServiceId(runtime.GetNodeId(0)), service);
        for (ui32 i = 1; i <= targetCount; ++i) {
            env.CreateTable("/Root", *MakeTableDescription(TTestTableDescription{
                .Name = Sprintf("table%u", i),
                .KeyColumns = {"key"},
                .Columns = {{.Name = "key", .Type = "Uint32"}},
                .ReplicationConfig = Nothing(),
            }));
        }
        NYdb::NTable::TTableClient client(env.GetDriver());
        auto session = env.RunCall([&] { return client.CreateSession().GetValueSync().GetSession(); });
        auto query = [&](const TString& text) {
            auto result = env.RunCall([&] { return session.ExecuteSchemeQuery(text).GetValueSync(); });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        };
        auto wait = [&](const TString& reason, const std::function<bool()>& predicate) {
            if (!predicate()) {
                TDispatchOptions options;
                options.CustomFinalCondition = predicate;
                options.FinalEvents.emplace_back([](IEventHandle&) { return false; });
                runtime.DispatchEvents(options, TDuration::Seconds(60));
            }
            UNIT_ASSERT_C(predicate(), reason);
        };

        ECancelStage stage = ECancelStage::InitialCreation;
        TActorId controllerActor;
        TActorId originalHolder;
        TActorId originalWaiter;
        THashSet<TActorId> originalRequests;
        THashSet<TActorId> originalGrants;
        THashSet<TActorId> replacementRequests;
        THashSet<TActorId> replacementRemoteRequests;
        THashSet<ui64> completedTargets;
        THashSet<ui64> runningTargets;
        THashSet<ui64> acknowledgedRunningTargets;
        THashSet<TActorId> cleanupActors;
        EWorkerObservation workerObservation = EWorkerObservation::Setup;
        ui64 controllerId = 0;
        ui32 generation = 0;
        IEventHandle::TPtr heldRemoteRequest;
        IEventHandle::TPtr heldPoison;
        IEventHandle::TPtr heldOriginalResult;
        IEventHandle::TPtr heldReplacementRequest;
        EEventGate originalRequestGate = EEventGate::Hold;
        EEventGate originalResultGate = EEventGate::Hold;
        EEventGate replacementRequestGate = EEventGate::Hold;
        TVector<IEventHandle::TPtr> staleGrants;

        auto observer = runtime.AddObserver([&](IEventHandle::TPtr& ev) {
            if (!ev->HasEvent()) {
                return;
            }
            if (ev->GetTypeRewrite() == TEvents::TEvPoison::EventType
                && workerObservation == EWorkerObservation::Cleanup
                && (ev->Sender == service || ev->Sender == controllerActor || cleanupActors.contains(ev->Sender)))
            {
                cleanupActors.insert(ev->GetRecipientRewrite());
            }
            // Several components share ES_PRIVATE event numbers: inspect the
            // dynamic type, not just the event number.
            if (dynamic_cast<TEvPrivate::TEvRequestCreateStream*>(ev->GetBase())) {
                if (!controllerActor) {
                    controllerActor = ev->GetRecipientRewrite();
                }
                UNIT_ASSERT_VALUES_EQUAL(ev->GetRecipientRewrite(), controllerActor);
                if (stage == ECancelStage::InitialCreation) {
                    originalRequests.insert(ev->Sender);
                } else {
                    UNIT_ASSERT(!originalRequests.contains(ev->Sender));
                    replacementRequests.insert(ev->Sender);
                }
            } else if (dynamic_cast<TEvPrivate::TEvAllowCreateStream*>(ev->GetBase())) {
                if (stage == ECancelStage::InitialCreation) {
                    originalGrants.insert(ev->GetRecipientRewrite());
                } else if (stage != ECancelStage::Open && ev->GetRecipientRewrite() == originalWaiter) {
                    // A grant already emitted before cancellation may still
                    // arrive after poison. Do not let that race rescue a bug.
                    staleGrants.push_back(std::move(ev));
                }
            } else if (dynamic_cast<TEvYdbProxy::TEvAlterTableRequest*>(ev->GetBase())) {
                if (stage == ECancelStage::InitialCreation && originalRequests.contains(ev->Sender) && originalRequestGate == EEventGate::Hold) {
                    UNIT_ASSERT(!heldRemoteRequest);
                    originalHolder = ev->Sender;
                    heldRemoteRequest = std::move(ev);
                } else if (replacementRequests.contains(ev->Sender)) {
                    replacementRemoteRequests.insert(ev->Sender);
                    if (scenario == ECancelCase::CommittedCreatorAndStaleResult && replacementRequestGate == EEventGate::Hold) {
                        heldReplacementRequest = std::move(ev);
                    }
                }
            } else if (auto* result = dynamic_cast<TEvPrivate::TEvCreateStreamResult*>(ev->GetBase())) {
                if (scenario == ECancelCase::CommittedCreatorAndStaleResult && originalRequests.contains(ev->Sender) && originalResultGate == EEventGate::Hold) {
                    UNIT_ASSERT_C(result->IsSuccess(), result->Status.GetIssues().ToString());
                    heldOriginalResult = std::move(ev);
                } else if (replacementRequests.contains(ev->Sender)) {
                    UNIT_ASSERT_C(result->IsSuccess(), result->Status.GetIssues().ToString());
                    completedTargets.insert(result->TargetId);
                }
            } else if (auto* status = dynamic_cast<TEvService::TEvWorkerStatus*>(ev->GetBase());
                status && ev->Sender == service && workerObservation == EWorkerObservation::Resumed
                && status->Record.GetStatus() == NKikimrReplication::TEvWorkerStatus::STATUS_RUNNING)
            {
                acknowledgedRunningTargets.insert(TWorkerId::Parse(status->Record.GetWorker()).TargetId());
            } else if (auto* handshake = dynamic_cast<TEvService::TEvHandshake*>(ev->GetBase())) {
                const auto& controller = handshake->Record.GetController();
                if (generation) {
                    UNIT_ASSERT_VALUES_EQUAL(controller.GetGeneration(), generation);
                }
                generation = controller.GetGeneration();
            } else if (auto* run = dynamic_cast<TEvService::TEvRunWorker*>(ev->GetBase());
                run && ev->Sender == controllerActor
                && workerObservation == EWorkerObservation::Resumed)
            {
                UNIT_ASSERT_VALUES_EQUAL(run->Record.GetController().GetTabletId(), controllerId);
                UNIT_ASSERT_VALUES_EQUAL(run->Record.GetController().GetGeneration(), generation);
                runningTargets.insert(TWorkerId::Parse(run->Record.GetWorker()).TargetId());
            } else if (ev->GetTypeRewrite() == TEvents::TEvPoison::EventType
                && stage == ECancelStage::HoldingOriginalPoison
                && scenario == ECancelCase::QueuedCreatorAndStaleGrant
                && ev->GetRecipientRewrite() == originalHolder)
            {
                UNIT_ASSERT(!heldPoison);
                heldPoison = std::move(ev);
            }
        });

        TString targets = "`/Root/table1` AS `/Root/replica1`";
        if (targetCount == 2) {
            targets += ", `/Root/table2` AS `/Root/replica2`";
        }
        query(Sprintf(R"(CREATE ASYNC REPLICATION `/Root/replication` FOR %s
            WITH (CONNECTION_STRING = "grpc://%s/?database=/Root", TOKEN = "root@builtin");)",
            targets.c_str(), env.GetEndpoint().c_str()));
        const auto description = env.GetDescription("/Root/replication").GetPathDescription().GetReplicationDescription();
        controllerId = description.GetControllerId();
        const auto pathId = env.GetPathId("/Root/replication");
        wait("real service handshakes with the original controller", [&] { return generation != 0; });

        wait("initial creator owns the only permit and all initial creators requested permission", [&] {
            return heldRemoteRequest && originalRequests.size() == targetCount;
        });
        UNIT_ASSERT_VALUES_EQUAL(originalGrants.size(), 1);
        UNIT_ASSERT(originalGrants.contains(originalHolder));
        if (targetCount == 2) {
            for (const auto& id : originalRequests) {
                if (id != originalHolder) {
                    originalWaiter = id;
                }
            }
            UNIT_ASSERT(originalWaiter);
            UNIT_ASSERT(!originalGrants.contains(originalWaiter));
        }

        if (scenario == ECancelCase::CommittedCreatorAndStaleResult) {
            // Let the original operation really commit both CDC and consumer,
            // but keep its successful result from reaching the controller.
            originalRequestGate = EEventGate::Forward;
            runtime.Send(heldRemoteRequest.Release());
            wait("original committed stream result is retained after its creator dies", [&] {
                return heldOriginalResult && !runtime.FindActor(originalHolder);
            });
        }

        stage = ECancelStage::HoldingOriginalPoison;
        query(R"(ALTER ASYNC REPLICATION `/Root/replication` SET (STATE = "Paused");)");
        if (targetCount == 2) {
            wait("queued original creator was poisoned before the holder is allowed to die", [&] {
                return heldPoison && !runtime.FindActor(originalWaiter);
            });
            stage = ECancelStage::ReleaseOriginalPoison;
            runtime.Send(heldPoison.Release());
        } else {
            stage = ECancelStage::ReleaseOriginalPoison;
        }
        wait("original permit owner was poisoned", [&] { return !runtime.FindActor(originalHolder); });
        stage = ECancelStage::Open;
        for (auto& grant : staleGrants) {
            runtime.Send(grant.Release());
        }
        staleGrants.clear();

        if (scenario == ECancelCase::CommittedCreatorAndStaleResult) {
            wait("replacement creator is installed before the stale result arrives", [&] {
                return replacementRequests.size() == 1;
            });
            originalResultGate = EEventGate::Forward;
            runtime.Send(heldOriginalResult.Release());
        }

        // Keep the original remote request withheld through every assertion.
        // A completion from the abandoned helper cannot release leaked quota.
        wait("replacement creators must issue real source DDL after PAUSED without controller restart", [&] {
            return replacementRemoteRequests.size() == targetCount;
        });
        if (scenario == ECancelCase::CommittedCreatorAndStaleResult) {
            UNIT_ASSERT(heldReplacementRequest);
            // The old completion must not finish PAUSED while the current
            // creator cannot complete. Exercise asynchronous destination work
            // for a bounded interval before allowing the replacement request.
            for (ui32 attempt = 0; attempt < 5; ++attempt) {
                runtime.SimulateSleep(TDuration::Seconds(1));
                auto request = MakeHolder<TEvController::TEvDescribeReplication>();
                pathId.ToProto(request->Record.MutablePathId());
                auto reply = env.Send<TEvController::TEvDescribeReplicationResult>(controllerId, std::move(request));
                UNIT_ASSERT_C(!reply->Get()->Record.GetState().HasPaused(),
                    "stale successful creator result must not complete the replacement pause");
                UNIT_ASSERT(!reply->Get()->Record.GetState().HasError());
            }
            replacementRequestGate = EEventGate::Forward;
            runtime.Send(heldReplacementRequest.Release());
        }
        wait("every replacement stream must finish successfully", [&] {
            return completedTargets.size() == targetCount;
        });
        UNIT_ASSERT_VALUES_EQUAL(replacementRequests.size(), targetCount);

        bool paused = false;
        for (ui32 attempt = 0; attempt < 60 && !paused; ++attempt) {
            auto request = MakeHolder<TEvController::TEvDescribeReplication>();
            pathId.ToProto(request->Record.MutablePathId());
            auto reply = env.Send<TEvController::TEvDescribeReplicationResult>(controllerId, std::move(request));
            UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(reply->Get()->Record.GetStatus()), static_cast<int>(NKikimrReplication::TEvDescribeReplicationResult::SUCCESS));
            UNIT_ASSERT_C(!reply->Get()->Record.GetState().HasError(), reply->Get()->Record.GetState().DebugString());
            paused = reply->Get()->Record.GetState().HasPaused();
            if (!paused) {
                runtime.SimulateSleep(TDuration::Seconds(1));
            }
        }
        UNIT_ASSERT_C(paused, "successful replacement creation must let PAUSED finish");
        for (ui32 i = 1; i <= targetCount; ++i) {
            const auto source = env.GetDescription(Sprintf("/Root/table%u", i));
            UNIT_ASSERT_VALUES_EQUAL(source.GetPathDescription().GetTable().CdcStreamsSize(), 1);
        }

        workerObservation = EWorkerObservation::Resumed;
        query(R"(ALTER ASYNC REPLICATION `/Root/replication` SET (STATE = "StandBy");)");
        wait("resume must start a worker for every replacement stream", [&] {
            return runningTargets.size() == targetCount;
        });
        UNIT_ASSERT(runningTargets == completedTargets);
        wait("real service acknowledges each resumed worker", [&] {
            return acknowledgedRunningTargets == completedTargets;
        });

        // Stop streaming SDK sessions while both the runtime and gRPC server
        // can still deliver their asynchronous shutdown completions.
        workerObservation = EWorkerObservation::Cleanup;
        query(R"(DROP ASYNC REPLICATION `/Root/replication` CASCADE;)");
        runtime.Send(service, env.GetSender(), new TEvents::TEvPoison());
        runtime.Send(env.GetYdbProxy(), env.GetSender(), new TEvents::TEvPoison());
        cleanupActors.insert(env.GetYdbProxy());
        wait("owned replication actors stop before gRPC shutdown", [&] {
            if (runtime.FindActor(service)) {
                return false;
            }
            for (const auto& actor : cleanupActors) {
                if (runtime.FindActor(actor)) {
                    return false;
                }
            }
            return true;
        });
        Cerr << "C1 resume verified; owned replication actors stopped" << Endl;
    }

    Y_UNIT_TEST(PauseReleasesAcquiredCreationPermit) {
        PauseDuringStreamCreation(ECancelCase::HoldingPermit);
    }

    Y_UNIT_TEST(PauseRemovesQueuedCreatorAndToleratesStaleGrant) {
        PauseDuringStreamCreation(ECancelCase::QueuedCreatorAndStaleGrant);
    }


    Y_UNIT_TEST(PauseIgnoresCommittedCreatorsStaleResult) {
        PauseDuringStreamCreation(ECancelCase::CommittedCreatorAndStaleResult);
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
