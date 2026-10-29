#include "controller_impl.h"

#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/core/tx/replication/service/service.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/replication/ut_helpers/mock_service.h>
#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/library/testlib/helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/printf.h>

#include <memory>

namespace NKikimr::NReplication::NController {

Y_UNIT_TEST_SUITE(SessionLifecycle) {
    using namespace NTestHelpers;

    // Exercise the real resolver and its sender/cookie checks while controlling
    // the external secret lookup and the order of delivery to the controller.
    class TSecretGate {
        TTestActorRuntime& Runtime;
        TTestActorRuntime::TEventObserverHolder Observer;

    public:
        bool Hold = false;
        TAutoPtr<IEventHandle> Pending;

        explicit TSecretGate(TTestActorRuntime& runtime)
            : Runtime(runtime)
            , Observer(runtime.AddObserver([this](IEventHandle::TPtr& ev) {
                if (ev->GetTypeRewrite() == TEvTxProxySchemeCache::EvNavigateKeySet
                    && Runtime.FindActor(ev->Sender)->GetActivityType()
                        == NKikimrServices::TActivity::REPLICATION_CONTROLLER_SECRET_RESOLVER)
                {
                    auto request = ev->Get<TEvTxProxySchemeCache::TEvNavigateKeySet>()->Request;
                    request->ResultSet.front().Status = NSchemeCache::TSchemeCacheNavigate::EStatus::Ok;
                    request->ResultSet.front().SecurityObject = nullptr;
                    Runtime.Send(new IEventHandle(ev->Sender, ev->GetRecipientRewrite(),
                        new TEvTxProxySchemeCache::TEvNavigateKeySetResult(request)));
                    ev.Reset();
                } else if (ev->GetTypeRewrite() == TEvPrivate::EvResolveSecretResult) {
                    auto result = MakeHolder<TEvPrivate::TEvResolveSecretResult>(
                        ev->Get<TEvPrivate::TEvResolveSecretResult>()->ReplicationId, "root@builtin");
                    ev.Reset(new IEventHandle(ev->GetRecipientRewrite(), ev->Sender,
                        result.Release(), 0, ev->Cookie));
                    if (Hold) {
                        UNIT_ASSERT(!Pending);
                        Pending = std::move(ev);
                    }
                }
            }))
        {}

        void Release() {
            UNIT_ASSERT(Pending);
            Hold = false;
            Runtime.Send(Pending.Release());
        }
    };

    struct TSetup {
        TEnv<> Env;
        std::unique_ptr<TSecretGate> Secret;
        TActorId Edges[2];
        TActorId Controller;
        ui64 ControllerId = 0;
        TPathId PathId;
        TWorkerId Worker{0, 0, 0};

        explicit TSetup(ui32 nodeCount = 2, bool secret = false)
            : Env(true, ERuntimeMode::Simulated, nodeCount)
        {
            UNIT_ASSERT(nodeCount >= 1 && nodeCount <= 2);
            auto& runtime = Env.GetRuntime();
            runtime.SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);
            if (secret) {
                Secret = std::make_unique<TSecretGate>(runtime);
            }

            for (ui32 node = 0; node < nodeCount; ++node) {
                Edges[node] = runtime.AllocateEdgeActor(node);
                runtime.RegisterService(MakeReplicationServiceId(runtime.GetNodeId(node)),
                    runtime.Register(CreateReplicationMockService(Edges[node]), node), node);
            }

            Env.RunCall([&] {
                NYdb::NTable::TTableClient client(Env.GetDriver(), NYdb::NTable::TClientSettings()
                    .DiscoveryEndpoint(Env.GetEndpoint())
                    .DiscoveryMode(NYdb::EDiscoveryMode::Async)
                    .Database(Env.GetDatabase()));
                const auto sessionResult = client.CreateSession().GetValueSync();
                UNIT_ASSERT_C(sessionResult.IsSuccess(), sessionResult.GetIssues().ToString());
                auto session = sessionResult.GetSession();

                auto result = session.ExecuteSchemeQuery(R"(
                    CREATE TABLE `/Root/table` (key Uint32, PRIMARY KEY (key));
                )").GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

                result = session.ExecuteSchemeQuery(Sprintf(R"(
                    CREATE ASYNC REPLICATION `replication` FOR
                        `/Root/table` AS `/Root/replica`
                    WITH (CONNECTION_STRING = "grpc://%s/?database=/Root"%s);
                )", Env.GetEndpoint().c_str(), secret ? ", TOKEN_SECRET_PATH = \"/Root/source-token\"" : "")).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            });

            PathId = Env.GetPathId("/Root/replication");

            for (ui32 node = 0; node < nodeCount; ++node) {
                const auto handshake = runtime.GrabEdgeEvent<TEvService::TEvHandshake>(Edges[node]);
                UNIT_ASSERT(handshake);

                Controller = handshake->Sender;
                ControllerId = handshake->Get()->Record.GetController().GetTabletId();
            }

            // Start on node 0 only, so placement is deterministic.
            Status(0, false);
            const auto run = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(Edges[0]);
            UNIT_ASSERT(run);
            Worker = TWorkerId::Parse(run->Get()->Record.GetWorker());

            if (nodeCount > 1) {
                Status(1, false);
            }

            // Queue processing is driven explicitly below, without advancing timers.
            runtime.SetScheduledEventFilter([](TTestActorRuntimeBase& runtime,
                    TAutoPtr<IEventHandle>&, TDuration delay, TInstant& deadline) {
                deadline = runtime.GetCurrentTime() + delay;
                return false;
            });
        }

        void Reboot() {
            Env.SendAsync(ControllerId, new TEvents::TEvPoisonPill());
            const auto handshake = Env.GetRuntime().GrabEdgeEvent<TEvService::TEvHandshake>(Edges[0]);
            UNIT_ASSERT(handshake);
            Controller = handshake->Sender;
            Sync();
        }

        void Sync() {
            auto request = MakeHolder<TEvController::TEvDescribeReplication>();
            PathId.ToProto(request->Record.MutablePathId());

            const auto response = Env.Send<TEvController::TEvDescribeReplicationResult>(Controller, std::move(request));
            UNIT_ASSERT_C(response->Get()->Record.GetStatus()
                == NKikimrReplication::TEvDescribeReplicationResult::SUCCESS,
                response->Get()->Record.ShortDebugString());
        }

        void Status(ui32 node, bool hasWorker = true) {
            auto status = MakeHolder<TEvService::TEvStatus>();
            if (hasWorker) {
                Worker.Serialize(*status->Record.AddWorkers());
            }

            Env.GetRuntime().Send(new IEventHandle(Controller, Edges[node], status.Release()));
            Sync();
        }

        void ProcessQueues(bool syncService = false) {
            Env.SendAsync(Controller, new TEvPrivate::TEvProcessQueues());
            Sync();
            if (syncService) {
                // Drain the service mailbox before asserting that no run was sent.
                Env.GetRuntime().Send(new IEventHandle(MakeReplicationServiceId(Edges[0].NodeId()),
                    Env.GetSender(), new TEvService::TEvHandshake(0, 0)), 0, true);
                UNIT_ASSERT(Env.GetRuntime().GrabEdgeEvent<TEvService::TEvHandshake>(Edges[0]));
            }
        }

        void Disconnect(ui32 node) {
            Env.SendAsync(Controller, new TEvInterconnect::TEvNodeDisconnected(Edges[node].NodeId()));
            Sync();
        }

        void ExpectStop(ui32 node) {
            const auto stop = Env.GetRuntime().GrabEdgeEvent<TEvService::TEvStopWorker>(Edges[node]);
            UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(stop->Get()->Record.GetWorker()), Worker);
        }

        void ExpectRun(ui32 node) {
            const auto run = Env.GetRuntime().GrabEdgeEvent<TEvService::TEvRunWorker>(Edges[node]);
            UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(run->Get()->Record.GetWorker()), Worker);
        }
    };

    void RecoverWithSecret(bool adopt, bool secretFirst, bool disconnect, bool readSource = false) {
        TSetup s(1, true);
        auto& runtime = s.Env.GetRuntime();
        UNIT_ASSERT(s.Secret);
        s.Secret->Hold = true;

        // Recovery must succeed independently of the registrar. In particular,
        // it cannot replace an adopted worker's stale command with a fresh one.
        TVector<TEvService::TEvRunWorker::TPtr> registrations;
        ui32 runs = 0;
        auto observer = runtime.AddObserver<TEvService::TEvRunWorker>([&](TEvService::TEvRunWorker::TPtr& ev) {
            if (ev->Get()->Record.HasController() && ev->GetRecipientRewrite() != s.Edges[0]) {
                ++runs;
            } else if (ev->GetRecipientRewrite() == s.Controller && ev->Get()->Record.HasCommand()) {
                registrations.push_back(std::move(ev));
            }
        });

        s.Reboot();
        runtime.WaitFor("held secret result", [&] { return bool(s.Secret->Pending); }, TDuration::Seconds(30));
        if (secretFirst) {
            s.Secret->Release();
            s.Sync();
        }
        s.Status(0, adopt);

        if (!adopt) {
            // This branch recovers workers through registrar requests rather
            // than persisting every runnable worker before dispatch.
            auto registration = MakeHolder<TEvService::TEvRunWorker>();
            s.Worker.Serialize(*registration->Record.MutableWorker());
            s.Env.SendAsync(s.Controller, std::move(registration));
            s.Sync();
        }
        s.ProcessQueues(true);
        UNIT_ASSERT_VALUES_EQUAL(runs, !adopt && secretFirst ? 1 : 0);

        // Reproduce the incident's stale-cache ordering on disconnect; the
        // STOPPED case below also covers a restart requested while unresolved.
        const bool resolveBeforeRestart = !secretFirst && adopt && disconnect;
        if (resolveBeforeRestart) {
            s.Secret->Release();
            s.Sync();
            s.ProcessQueues(true);
            UNIT_ASSERT_VALUES_EQUAL(runs, 0);
        }

        if (adopt) {
            if (disconnect) {
                s.Disconnect(0);
                s.Env.SendAsync(s.Controller, new TEvPrivate::TEvRestoreSession(s.Edges[0].NodeId()));
                s.Sync();
                UNIT_ASSERT(runtime.GrabEdgeEvent<TEvService::TEvHandshake>(s.Edges[0]));
                s.Status(0, false);
            } else {
                runtime.Send(new IEventHandle(s.Controller, s.Edges[0],
                    new TEvService::TEvWorkerStatus(s.Worker, NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED)));
                s.Sync();
            }
        }
        s.ProcessQueues(true);
        if (!secretFirst && !resolveBeforeRestart) {
            UNIT_ASSERT_VALUES_EQUAL_C(runs, 0, "worker booted before the secret resolved");
            s.Secret->Release();
            s.Sync();
            s.ProcessQueues(true);
        }

        auto run = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(s.Edges[0]);
        UNIT_ASSERT(run);
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(run->Get()->Record.GetWorker()), s.Worker);
        UNIT_ASSERT_VALUES_EQUAL(run->Get()->Record.GetCommand().GetRemoteTopicReader()
            .GetConnectionParams().GetOAuthToken().GetToken(), "root@builtin");
        UNIT_ASSERT_VALUES_EQUAL(runs, 1);

        if (readSource) {
            // Run the recovered command through the real service and SDK reader,
            // verifying the token on the source RPC as well as row delivery.
            ui32 authenticatedReads = 0;
            ui32 finishedReads = 0;
            auto readObserver = runtime.AddObserver<NGRpcService::TEvStreamTopicReadRequest>(
                [&](NGRpcService::TEvStreamTopicReadRequest::TPtr& ev) {
                    UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetYdbToken().GetOrElse(""), "root@builtin");
                    ev->Get()->SetAuditLogHook([&](auto, const auto&) { ++finishedReads; });
                    ++authenticatedReads;
                });
            const auto controller = run->Get()->Record.GetController();
            const auto service = runtime.Register(CreateReplicationService());
            runtime.Send(new IEventHandle(service, s.Controller, new TEvService::TEvHandshake(
                s.ControllerId, controller.GetGeneration())), 0, true);
            runtime.Send(run->Forward(service), 0, true);

            s.Env.RunCall([&] {
                NYdb::NTable::TTableClient client(s.Env.GetDriver());
                auto session = client.CreateSession().GetValueSync().GetSession();
                auto result = session.ExecuteDataQuery("UPSERT INTO `/Root/table` (key) VALUES (42u)",
                    NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

                for (const auto deadline = TInstant::Now() + TDuration::Seconds(30); TInstant::Now() < deadline;) {
                    result = session.ExecuteDataQuery("SELECT key FROM `/Root/replica` WHERE key = 42u",
                        NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
                    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                    if (result.GetResultSet(0).RowsCount()) {
                        return;
                    }
                    Sleep(TDuration::MilliSeconds(50));
                }
                UNIT_FAIL("recovered worker did not consume the source row");
            });
            UNIT_ASSERT_GT(authenticatedReads, 0);

            // Close the live SDK stream while its source GRpc server is alive.
            auto stop = MakeHolder<TEvService::TEvStopWorker>();
            stop->Record.MutableController()->CopyFrom(controller);
            s.Worker.Serialize(*stop->Record.MutableWorker());
            runtime.Send(new IEventHandle(service, s.Controller, stop.Release()), 0, true);
            runtime.WaitFor("source read stream closed", [&] { return finishedReads > 0; }, TDuration::Seconds(30));
            runtime.Send(new IEventHandle(service, s.Controller, new TEvents::TEvPoison()), 0, true);
            runtime.WaitFor("replication service stopped", [&] { return !runtime.FindActor(service); }, TDuration::Seconds(30));
        }
    }

    Y_UNIT_TEST_TWIN(AdoptedWorkerWaitsForSecretAfterStop, SecretFirst) {
        RecoverWithSecret(true, SecretFirst, false);
    }

    Y_UNIT_TEST_TWIN(AdoptedWorkerWaitsForSecretAfterDisconnect, SecretFirst) {
        RecoverWithSecret(true, SecretFirst, true);
    }

    Y_UNIT_TEST_TWIN(RecoveredRegistrarWorkerWaitsForSecret, SecretFirst) {
        RecoverWithSecret(false, SecretFirst, false);
    }

    Y_UNIT_TEST(ChildWorkerWaitsForSecretAfterDataEnd) {
        TSetup s(1, true);
        auto& runtime = s.Env.GetRuntime();
        s.Secret->Hold = true;
        s.Reboot();
        runtime.WaitFor("held secret result", [&] { return bool(s.Secret->Pending); }, TDuration::Seconds(30));
        s.Status(0);

        ui32 runs = 0;
        auto observer = runtime.AddObserver<TEvService::TEvRunWorker>([&](TEvService::TEvRunWorker::TPtr& ev) {
            if (ev->Get()->Record.HasController() && ev->GetRecipientRewrite() != s.Edges[0]) {
                ++runs;
            }
        });
        const TWorkerId child(s.Worker.ReplicationId(), s.Worker.TargetId(), s.Worker.WorkerId() + 1);
        auto dataEnd = MakeHolder<TEvService::TEvWorkerDataEnd>();
        s.Worker.Serialize(*dataEnd->Record.MutableWorker());
        dataEnd->Record.AddChildPartitionsIds(child.WorkerId());
        runtime.Send(new IEventHandle(s.Controller, s.Edges[0], dataEnd.Release()));
        s.Sync();
        s.ProcessQueues(true);
        UNIT_ASSERT_VALUES_EQUAL_C(runs, 0, "child worker booted before the secret resolved");

        s.Secret->Release();
        s.Sync();
        s.ProcessQueues(true);
        const auto run = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(s.Edges[0]);
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(run->Get()->Record.GetWorker()), child);
        UNIT_ASSERT_VALUES_EQUAL(run->Get()->Record.GetCommand().GetRemoteTopicReader()
            .GetConnectionParams().GetOAuthToken().GetToken(), "root@builtin");
        UNIT_ASSERT_VALUES_EQUAL(runs, 1);
    }

    Y_UNIT_TEST(RecoveredWorkerAuthenticatesAndConsumesAfterDelayedSecret) {
        RecoverWithSecret(true, false, false, true);
    }

    Y_UNIT_TEST(DisconnectBeforeStopAcknowledgement) {
        TSetup s;

        // Both nodes report the same worker before the queued stops are sent.
        s.Status(1);
        s.Status(0);

        s.ProcessQueues();
        s.ExpectStop(0);
        s.ExpectStop(1);

        // The stop acknowledgement is lost with the session. The worker must
        // lose its owner so another node can adopt it, without a stale session ID.
        s.Disconnect(0);
        s.Status(1);

        s.Disconnect(1);

        s.Env.SendAsync(s.Controller, new TEvPrivate::TEvRestoreSession(s.Edges[0].NodeId()));
        s.Sync();
        UNIT_ASSERT(s.Env.GetRuntime().GrabEdgeEvent<TEvService::TEvHandshake>(s.Edges[0]));

        s.Status(0, false);
        s.ProcessQueues();
        s.ExpectRun(0);
    }

    Y_UNIT_TEST(OldNodeDisconnectKeepsNewOwner) {
        TSetup s;

        s.Status(1);
        s.ProcessQueues();
        s.ExpectStop(0);

        // An acknowledgement from the old node must not detach the new owner.
        ui32 duplicateRuns = 0;
        auto observer = s.Env.GetRuntime().AddObserver<TEvService::TEvRunWorker>(
            [&](TEvService::TEvRunWorker::TPtr&) { ++duplicateRuns; });

        s.Env.GetRuntime().Send(new IEventHandle(s.Controller, s.Edges[0],
            new TEvService::TEvWorkerStatus(s.Worker, NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED)));
        s.Sync();
        s.ProcessQueues();
        UNIT_ASSERT_VALUES_EQUAL(duplicateRuns, 0);
        observer.Remove();

        s.Disconnect(0);
        s.Status(1);

        s.Disconnect(1);

        s.Env.SendAsync(s.Controller, new TEvPrivate::TEvRestoreSession(s.Edges[0].NodeId()));
        s.Sync();
        UNIT_ASSERT(s.Env.GetRuntime().GrabEdgeEvent<TEvService::TEvHandshake>(s.Edges[0]));

        s.Status(0, false);
        s.ProcessQueues();
        s.ExpectRun(0);
    }

    Y_UNIT_TEST(RemovalDetachesWorkerFromSession) {
        TSetup s;

        s.Env.SendAsync(s.Controller, new TEvPrivate::TEvRemoveWorker(
            s.Worker.ReplicationId(), s.Worker.TargetId(), s.Worker.WorkerId()));
        s.Sync();
        s.ProcessQueues();
        s.ExpectStop(0);

        s.Env.GetRuntime().Send(new IEventHandle(s.Controller, s.Edges[0],
            new TEvService::TEvWorkerStatus(s.Worker, NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED)));
        s.Sync();

        // This branch removes workers synchronously in the status handler.
        s.Status(1);

        // A stale entry in node 0's session would clear node 1's new owner
        // and incorrectly boot a second copy when node 0 disconnects.
        ui32 duplicateRuns = 0;
        auto observer = s.Env.GetRuntime().AddObserver<TEvService::TEvRunWorker>(
            [&](TEvService::TEvRunWorker::TPtr&) { ++duplicateRuns; });

        s.Disconnect(0);
        s.ProcessQueues();
        UNIT_ASSERT_VALUES_EQUAL(duplicateRuns, 0);
    }
}

}
