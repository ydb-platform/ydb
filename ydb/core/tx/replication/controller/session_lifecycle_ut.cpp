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
        TEnv<> Env{true, ERuntimeMode::Simulated, 2};
        TActorId Edges[2];
        TActorId Controller;
        ui64 ControllerId = 0;
        TPathId PathId;
        TWorkerId Worker{0, 0, 0};

        TSetup() {
            auto& runtime = Env.GetRuntime();
            runtime.SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

            for (ui32 node = 0; node < 2; ++node) {
                Edges[node] = runtime.AllocateEdgeActor(node);
                runtime.RegisterService(MakeReplicationServiceId(runtime.GetNodeId(node)),
                    runtime.Register(CreateReplicationMockService(Edges[node]), node), node);
            }

            Env.RunCall([&] {
                NYdb::NTable::TTableClient client(Env.GetDriver());
                auto session = client.CreateSession().GetValueSync().GetSession();

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

            PathId = Env.GetPathId("/Root/replication");

            for (ui32 node = 0; node < 2; ++node) {
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

            Status(1, false);

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

        TString Monitoring(const TVector<std::pair<TString, TString>>& parameters = {}) {
            NActorsProto::TRemoteHttpInfo request;
            request.SetMethod(HTTP_METHOD_GET);
            request.SetPath("/app");
            for (const auto& [key, value] : parameters) {
                auto* parameter = request.AddQueryParams();
                parameter->SetKey(key);
                parameter->SetValue(value);
            }
            return Env.Send<NMon::TEvRemoteHttpInfoRes>(Controller,
                new NMon::TEvRemoteHttpInfo(std::move(request)))->Get()->Html;
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

    Y_UNIT_TEST(MonitoringMemoryAndPersistedTables) {
        TSetup s;
        const auto memory = s.Monitoring();
        for (TStringBuf expected : {"Replication controller", "In memory", "Persisted tables",
                "/Root/replica", "Workers", "Sessions", "Pipeline", "Schema barriers", "Index builds"}) {
            UNIT_ASSERT_C(memory.Contains(expected), expected);
        }
        for (const TString table : {"Replications", "Targets", "SrcStreams", "Workers", "TxIds",
                "SysParams", "SchemaBarrierWorkers", "IndexBuildWorkers"}) {
            const auto storage = s.Monitoring({{"view", "storage"}, {"table", table}});
            UNIT_ASSERT_C(storage.Contains("<h3>" + table + "</h3>"), storage);
            UNIT_ASSERT_C(storage.Contains("Read-only snapshot"), storage);
            UNIT_ASSERT_C(!storage.Contains("Cannot decode"), storage);
        }
        const auto storedTargets = s.Monitoring({{"view", "storage"}, {"table", "Targets"}});
        UNIT_ASSERT_C(storedTargets.Contains("/Root/replica"), storedTargets);

        const auto filtered = s.Monitoring({{"rid", ToString(Max<ui64>())}});
        UNIT_ASSERT_C(filtered.Contains("is not present in memory"), filtered);
        UNIT_ASSERT_C(!filtered.Contains("/Root/replica"), filtered);
        const auto secondPage = s.Monitoring({{"page", "1"}});
        UNIT_ASSERT_C(secondPage.Contains("Previous page"), secondPage);
        UNIT_ASSERT_C(secondPage.Contains("No entries on this page"), secondPage);
        UNIT_ASSERT_C(!secondPage.Contains("/Root/replica"), secondPage);
    }

    Y_UNIT_TEST(MonitoringEscapesErrorsAndRejectsInvalidParameters) {
        TSetup s;
        const TString error = "source <script>alert('error')</script> & destination";
        s.Env.GetRuntime().Send(new IEventHandle(s.Controller, s.Edges[0], new TEvService::TEvWorkerStatus(
            s.Worker, NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED,
            NKikimrReplication::TEvWorkerStatus::REASON_ERROR, error)));
        s.Sync();

        for (const auto& parameters : TVector<TVector<std::pair<TString, TString>>>{
                {}, {{"view", "storage"}, {"table", "Replications"}}, {{"view", "storage"}, {"table", "Targets"}}}) {
            const auto page = s.Monitoring(parameters);
            UNIT_ASSERT_C(!page.Contains(error), page);
            UNIT_ASSERT_C(page.Contains("&lt;script&gt;alert(&#39;error&#39;)&lt;/script&gt; &amp; destination"), page);
        }
        const auto invalidId = s.Monitoring({{"rid", "<script>"}});
        UNIT_ASSERT_C(invalidId.Contains("Invalid replication ID"), invalidId);
        const auto invalidPage = s.Monitoring({{"page", "-1"}});
        UNIT_ASSERT_C(invalidPage.Contains("Invalid page number"), invalidPage);
        const auto invalidTable = s.Monitoring({{"view", "storage"}, {"table", "<script>untrusted-table</script>"}});
        UNIT_ASSERT_C(invalidTable.Contains("Unknown persisted table"), invalidTable);
        UNIT_ASSERT_C(!invalidTable.Contains("untrusted-table"), invalidTable);
        for (const TString cursor : {"1", "1,2,3,4", "-1,2,3", "1,2,<script>"}) {
            const auto invalidCursor = s.Monitoring({{"view", "storage"}, {"table", "Workers"}, {"from", cursor}});
            UNIT_ASSERT_C(invalidCursor.Contains("Invalid table cursor"), invalidCursor);
        }
    }

    Y_UNIT_TEST(MonitoringReadsStorageIndependentlyAndPaginates) {
        TSetup s;
        NKikimrMiniKQL::TResult result;
        const auto query = Sprintf(R"((
            (let key '('('ReplicationId (Uint64 '%lu)) '('Id (Uint64 '%lu))))
            (return (AsList (UpdateRow 'Targets key '('('DstPath (Utf8 '/Root/persisted-only))))))
        ))", s.Worker.ReplicationId(), s.Worker.TargetId());
        UNIT_ASSERT_VALUES_EQUAL(LocalQuery(s.Env.GetRuntime(), s.ControllerId, query, result), NKikimrProto::OK);
        UNIT_ASSERT(s.Monitoring().Contains("/Root/replica"));
        UNIT_ASSERT(!s.Monitoring().Contains("/Root/persisted-only"));
        const auto targets = s.Monitoring({{"view", "storage"}, {"table", "Targets"}});
        UNIT_ASSERT_C(targets.Contains("/Root/persisted-only"), targets);

        TStringBuilder updates;
        updates << "((return (AsList ";
        for (ui32 id = 100; id <= 200; ++id) {
            updates << "(UpdateRow 'SysParams '('('Id (Uint32 '" << id << "))) "
                << "'('('IntValue (Uint64 '" << id << ")) "
                << "'('TextValue (Utf8 'monitoring-secret)) '('BinaryValue (String 'monitoring-secret)))) ";
        }
        updates << ")))";
        UNIT_ASSERT_VALUES_EQUAL(LocalQuery(s.Env.GetRuntime(), s.ControllerId, updates, result), NKikimrProto::OK);
        const auto first = s.Monitoring({{"view", "storage"}, {"table", "SysParams"}});
        UNIT_ASSERT_C(first.Contains("Next page"), first);
        UNIT_ASSERT_C(first.Contains("from=199"), first);
        UNIT_ASSERT_C(!first.Contains("<td>200</td>"), first);
        UNIT_ASSERT_C(!first.Contains("monitoring-secret"), first);
        const auto next = s.Monitoring({{"view", "storage"}, {"table", "SysParams"}, {"from", "199"}});
        UNIT_ASSERT_C(next.Contains("<td>199</td>"), next);
        UNIT_ASSERT_C(next.Contains("<td>200</td>"), next);
        UNIT_ASSERT_C(!next.Contains("Next page"), next);
        UNIT_ASSERT_C(!next.Contains("monitoring-secret"), next);
    }
}

}
