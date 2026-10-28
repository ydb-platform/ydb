#include "controller_impl.h"

#include <ydb/core/testlib/tx_helpers.h>
#include <ydb/core/tx/replication/ut_helpers/mock_service.h>
#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/library/mkql_proto/protos/minikql.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/printf.h>

namespace NKikimr::NReplication::NController {

Y_UNIT_TEST_SUITE(SessionLifecycle) {
    using namespace NTestHelpers;

    struct TSetup {
        TEnv<> Env;
        TActorId Edges[2];
        TActorId Controller;
        ui64 ControllerId = 0;
        TPathId PathId;
        TWorkerId Worker{0, 0, 0};
        const bool Transfer;

        explicit TSetup(ui32 nodeCount = 2, bool transfer = false)
            : Env(true, ERuntimeMode::Simulated, nodeCount)
            , Transfer(transfer)
        {
            UNIT_ASSERT(nodeCount >= 1 && nodeCount <= 2);
            auto& runtime = Env.GetRuntime();
            runtime.SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

            for (ui32 node = 0; node < nodeCount; ++node) {
                Edges[node] = runtime.AllocateEdgeActor(node);
                runtime.RegisterService(MakeReplicationServiceId(runtime.GetNodeId(node)),
                    runtime.Register(CreateReplicationMockService(Edges[node]), node), node);
            }

            Env.RunCall([&] {
                NYdb::NTable::TTableClient client(Env.GetDriver());
                auto session = client.CreateSession().GetValueSync().GetSession();

                if (Transfer) {
                    auto result = session.ExecuteSchemeQuery(R"(
                        CREATE TABLE `/Root/table` (Key Uint64, Value String, PRIMARY KEY (Key));
                    )").GetValueSync();
                    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

                    result = session.ExecuteSchemeQuery(R"(
                        CREATE TOPIC `/Root/topic`;
                    )").GetValueSync();
                    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

                    result = session.ExecuteSchemeQuery(Sprintf(R"(
                        CREATE TRANSFER `/Root/transfer`
                            FROM `/Root/topic` TO `/Root/table`
                            USING ($x) -> { RETURN <| Key:$x._offset, Value:$x._data |> }
                        WITH (CONNECTION_STRING = "grpc://%s/?database=/Root");
                    )", Env.GetEndpoint().c_str())).GetValueSync();
                    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                    return;
                }

                auto result = session.ExecuteSchemeQuery(R"(
                    CREATE TABLE `/Root/table` (key Uint32, PRIMARY KEY (key));
                )").GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

                result = session.ExecuteSchemeQuery(Sprintf(R"(
                    CREATE ASYNC REPLICATION `replication` FOR
                        `/Root/table` AS `/Root/replica`
                    WITH (CONNECTION_STRING = "grpc://%s/?database=/Root");
                )", Env.GetEndpoint().c_str())).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            });

            PathId = Env.GetPathId(Transfer ? "/Root/transfer" : "/Root/replication");

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

        void ProcessQueues() {
            Env.SendAsync(Controller, new TEvPrivate::TEvProcessQueues());
            Sync();
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

        void AlterState(const TString& state) {
            Env.RunCall([&] {
                NYdb::NTable::TTableClient client(Env.GetDriver());
                auto session = client.CreateSession().GetValueSync().GetSession();
                const auto result = session.ExecuteSchemeQuery(Sprintf(R"(
                    ALTER %s SET (STATE = "%s");
                )", Transfer ? "TRANSFER `/Root/transfer`" : "ASYNC REPLICATION `/Root/replication`",
                    state.c_str())).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            });
        }

        bool HasWorkerRow() {
            NKikimrMiniKQL::TResult result;
            const auto query = Sprintf(R"((
                (let key '('('ReplicationId (Uint64 '%lu)) '('TargetId (Uint64 '%lu)) '('WorkerId (Uint64 '%lu))))
                (return (AsList (SetResult 'Worker (SelectRow 'Workers key '('WorkerId)))))
            ))", Worker.ReplicationId(), Worker.TargetId(), Worker.WorkerId());
            UNIT_ASSERT_VALUES_EQUAL(LocalQuery(Env.GetRuntime(), ControllerId, query, result), NKikimrProto::OK);
            return result.GetValue().GetStruct(0).GetOptional().HasOptional();
        }
    };

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

        // Wait for the removal transaction before reusing the same worker ID.
        NKikimrMiniKQL::TResult result;
        const auto query = Sprintf(R"((
            (let key '('('ReplicationId (Uint64 '%lu)) '('TargetId (Uint64 '%lu)) '('WorkerId (Uint64 '%lu))))
            (return (AsList (SetResult 'Worker (SelectRow 'Workers key '('WorkerId)))))
        ))", s.Worker.ReplicationId(), s.Worker.TargetId(), s.Worker.WorkerId());
        UNIT_ASSERT_VALUES_EQUAL(LocalQuery(s.Env.GetRuntime(), s.ControllerId, query, result), NKikimrProto::OK);
        UNIT_ASSERT_C(!result.GetValue().GetStruct(0).GetOptional().HasOptional(), result.ShortDebugString());

        s.Sync();
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

    // The node is still in discovery, so the session is restored after a disconnect.
    // A failed restore handshake is reported the way interconnect does for an event
    // with FlagSubscribeOnSession: TEvNodeDisconnected to the sender.
    Y_UNIT_TEST(WorkerBootsAfterFailedRestoreHandshake) {
        TSetup s(1);
        auto& runtime = s.Env.GetRuntime();
        const auto nodeId = s.Edges[0].NodeId();

        ui32 dropped = 0;
        bool dropHandshake = true;
        auto observer = runtime.AddObserver<TEvService::TEvHandshake>([&](TEvService::TEvHandshake::TPtr& ev) {
            if (!dropHandshake || ev->GetRecipientRewrite().NodeId() != nodeId) {
                return;
            }
            ++dropped;
            runtime.Send(new IEventHandle(ev->Sender, ev->Sender, new TEvInterconnect::TEvNodeDisconnected(nodeId)));
            ev.Reset();
        });

        s.Disconnect(0);
        runtime.DispatchEvents({}, TDuration::Seconds(5));
        UNIT_ASSERT_C(dropped > 1, "restore is not retried after a failed handshake, dropped: " << dropped);
        dropHandshake = false;
        observer.Remove();

        const auto handshake = runtime.GrabEdgeEvent<TEvService::TEvHandshake>(s.Edges[0], TDuration::Seconds(15));
        UNIT_ASSERT_C(handshake, "no handshake after the node returns");

        s.Status(0, false);
        s.ProcessQueues();
        s.ExpectRun(0);
    }

    // The stop is sent, then the node disconnects before STATUS_STOPPED arrives.
    // DeleteSession moves the worker back to BootQueue regardless of RemoveQueue,
    // so the removal never completes.
    Y_UNIT_TEST(TransferRemovalCompletesWhenStopAcknowledgementIsLost) {
        TSetup s(2, true);
        auto& runtime = s.Env.GetRuntime();

        s.Env.SendAsync(s.Controller, new TEvPrivate::TEvRemoveWorker(
            s.Worker.ReplicationId(), s.Worker.TargetId(), s.Worker.WorkerId()));
        s.Sync();
        s.ProcessQueues();
        s.ExpectStop(0);

        ui32 runs = 0;
        auto observer = runtime.AddObserver<TEvService::TEvRunWorker>(
            [&](TEvService::TEvRunWorker::TPtr&) { ++runs; });

        s.Disconnect(0);
        for (ui32 i = 0; i < 10; ++i) {
            s.ProcessQueues();
        }
        UNIT_ASSERT_VALUES_EQUAL_C(runs, 0, "worker pending removal was booted again");

        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT_C(!s.HasWorkerRow(), "worker pending removal is never removed");
    }

    // Incident scenario: a transfer is paused while nodes restart. The node with
    // the worker restarts after the stop is sent, the acknowledgement is lost.
    // The worker stays in RemoveQueue forever, the target never leaves Alter,
    // and neither pause nor resume can recover it; only a controller restart can.
    void ResumeAfterPauseWithLostStopAcknowledgement(bool transfer) {
        TSetup s(1, transfer);
        auto& runtime = s.Env.GetRuntime();
        const auto nodeId = s.Edges[0].NodeId();

        s.AlterState("Paused");
        s.ProcessQueues();
        s.ExpectStop(0);

        // The node restarts: the acknowledgement is lost, the new process has no workers.
        s.Disconnect(0);
        s.Env.SendAsync(s.Controller, new TEvPrivate::TEvRestoreSession(nodeId));
        s.Sync();
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvService::TEvHandshake>(s.Edges[0]));
        s.Status(0, false);
        s.ProcessQueues();

        s.AlterState("StandBy");
        for (ui32 i = 0; i < 50; ++i) {
            s.ProcessQueues();
            runtime.SimulateSleep(TDuration::MilliSeconds(200));
        }

        const auto run = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(s.Edges[0], TDuration::Seconds(10));
        UNIT_ASSERT_C(run, "worker does not start after resume");
    }

    Y_UNIT_TEST(TransferResumeAfterPauseWithLostStopAcknowledgement) {
        ResumeAfterPauseWithLostStopAcknowledgement(true);
    }

    Y_UNIT_TEST(ReplicationResumeAfterPauseWithLostStopAcknowledgement) {
        ResumeAfterPauseWithLostStopAcknowledgement(false);
    }

    // The same pause and resume with a delivered acknowledgement.
    Y_UNIT_TEST(TransferResumeAfterPause) {
        TSetup s(1, true);
        auto& runtime = s.Env.GetRuntime();

        s.AlterState("Paused");
        s.ProcessQueues();
        s.ExpectStop(0);
        runtime.Send(new IEventHandle(s.Controller, s.Edges[0],
            new TEvService::TEvWorkerStatus(s.Worker, NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED)));
        s.Sync();

        s.AlterState("StandBy");
        for (ui32 i = 0; i < 50; ++i) {
            s.ProcessQueues();
            runtime.SimulateSleep(TDuration::MilliSeconds(200));
        }

        const auto run = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(s.Edges[0], TDuration::Seconds(10));
        UNIT_ASSERT_C(run, "transfer worker does not start after resume");
    }
}

}
