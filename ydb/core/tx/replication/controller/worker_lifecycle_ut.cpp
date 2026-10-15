#include "controller_impl.h"

#include <ydb/core/base/statestorage.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/replication/service/service.h>
#include <ydb/core/tx/replication/ut_helpers/mock_service.h>
#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/actors/core/interconnect.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/string/printf.h>

namespace NKikimr::NReplication::NController {

// Children of a split changefeed topic partition get their workers only when
// the controller handles the parent's TEvWorkerDataEnd. These tests cover the ways
// it was lost: node disconnect (session never restored), controller restart
// (DataEnd not resent), and re-adopted workers that were never rebooted.
Y_UNIT_TEST_SUITE(WorkerLifecycle) {
    using namespace NTestHelpers;

    struct TSetup {
        TEnv<> Env;
        TActorId Service;
        TActorId LookupEdge;
        ui64 ControllerId = 0;
        TString StreamPath;
        TWorkerId Root{0, 0, 0};

        explicit TSetup(bool mockService = true) {
            auto& runtime = Env.GetRuntime();
            LookupEdge = runtime.AllocateEdgeActor();
            runtime.SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);
            runtime.SetLogPriority(NKikimrServices::REPLICATION_SERVICE, NLog::PRI_DEBUG);

            if (mockService) {
                Service = runtime.AllocateEdgeActor();
                runtime.RegisterService(
                    MakeReplicationServiceId(runtime.GetNodeId(0)),
                    runtime.Register(CreateReplicationMockService(Service))
                );
            } else {
                runtime.RegisterService(
                    MakeReplicationServiceId(runtime.GetNodeId(0)),
                    runtime.Register(CreateReplicationService())
                );
            }

            auto result = Session().ExecuteSchemeQuery(R"(
                CREATE TABLE `/Root/table` (key Uint32, value Utf8, PRIMARY KEY (key));
            )").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            // goes through the changefeed initial scan
            Upsert(1, 100);

            result = Session().ExecuteSchemeQuery(Sprintf(R"(
                CREATE ASYNC REPLICATION `replication` FOR
                    `/Root/table` AS `/Root/replica`
                WITH (
                    CONNECTION_STRING = "grpc://%s/?database=/Root"
                );
            )", Env.GetEndpoint().c_str())).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            ControllerId = Env.GetDescription("/Root/replication")
                .GetPathDescription().GetReplicationDescription().GetControllerId();

            if (mockService) {
                Handshake({});

                auto ev = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(Service, TDuration::Seconds(30));
                UNIT_ASSERT_C(ev, "root worker was not started");
                Root = TWorkerId::Parse(ev->Get()->Record.GetWorker());
            } else {
                UNIT_ASSERT_VALUES_EQUAL(WaitReplicated(100, TDuration::Seconds(60)), 100);
            }

            const auto desc = Env.GetDescription("/Root/table");
            for (const auto& stream : desc.GetPathDescription().GetTable().GetCdcStreams()) {
                StreamPath = "/Root/table/" + stream.GetName();
            }
            UNIT_ASSERT_C(StreamPath, desc.ShortDebugString());
            UNIT_ASSERT_VALUES_EQUAL(Partitions().size(), 1);
        }

        NYdb::NTable::TSession Session() {
            NYdb::NTable::TTableClient client(Env.GetDriver(), NYdb::NTable::TClientSettings()
                .DiscoveryEndpoint(Env.GetEndpoint())
                .Database(Env.GetDatabase())
            );
            return client.CreateSession().GetValueSync().GetSession();
        }

        void Upsert(ui32 from, ui32 to) {
            TStringBuilder query;
            query << "UPSERT INTO `/Root/table` (key, value) VALUES ";
            for (ui32 i = from; i <= to; ++i) {
                query << (i == from ? "" : ", ") << "(" << i << "u, \"" << TString(1000, 'a') << "\"u)";
            }
            auto result = Session().ExecuteDataQuery(query, NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        ui64 WaitReplicated(ui64 expected, TDuration timeout) {
            ui64 count = 0;
            for (const auto deadline = TInstant::Now() + timeout; TInstant::Now() < deadline; Sleep(TDuration::MilliSeconds(500))) {
                auto result = Session().ExecuteDataQuery("SELECT COUNT(*) FROM `/Root/replica`",
                    NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
                if (!result.IsSuccess()) {
                    continue;
                }
                auto parser = result.GetResultSetParser(0);
                UNIT_ASSERT(parser.TryNextRow());
                count = parser.ColumnParser(0).GetUint64();
                if (count == expected) {
                    break;
                }
            }
            Cerr << "... replicated " << count << " of " << expected << Endl;
            return count;
        }

        ui32 Generation() {
            auto& runtime = Env.GetRuntime();
            runtime.Send(new IEventHandle(MakeStateStorageProxyID(), LookupEdge, new TEvStateStorage::TEvLookup(ControllerId, 0)));
            const auto ev = runtime.GrabEdgeEvent<TEvStateStorage::TEvInfo>(LookupEdge);
            return ev && ev->Get()->Status == NKikimrProto::OK ? ev->Get()->CurrentGeneration : 0;
        }

        // RebootTablet() waits for TEvBoot via the test dispatcher, which does not work with real threads
        void RebootController() {
            auto& runtime = Env.GetRuntime();
            const auto generation = Generation();
            ForwardToTablet(runtime, ControllerId, Env.GetSender(), new TEvents::TEvPoisonPill());
            for (int i = 0; i < 300 && Generation() <= generation; ++i) {
                Sleep(TDuration::MilliSeconds(100));
            }
            UNIT_ASSERT_GT(Generation(), generation);
            InvalidateTabletResolverCache(runtime, ControllerId);
        }

        // Takes over the real service's session as if it were the current controller
        // generation, so DataEnd goes to the returned edge instead of the controller.
        TActorId HijackServiceSession() {
            auto& runtime = Env.GetRuntime();
            const auto generation = Generation();
            UNIT_ASSERT_GT(generation, 0);

            const auto edge = runtime.AllocateEdgeActor();
            runtime.Send(new IEventHandle(MakeReplicationServiceId(runtime.GetNodeId(0)), edge,
                new TEvService::TEvHandshake(ControllerId, generation)));
            UNIT_ASSERT(runtime.GrabEdgeEvent<TEvService::TEvStatus>(edge));
            return edge;
        }

        // plays the replication service side of a (re)established session
        void Handshake(const TVector<TWorkerId>& alive) {
            auto ev = Env.GetRuntime().GrabEdgeEvent<TEvService::TEvHandshake>(Service, TDuration::Seconds(30));
            UNIT_ASSERT_C(ev, "no handshake from controller");

            auto status = MakeHolder<TEvService::TEvStatus>();
            for (const auto& id : alive) {
                id.Serialize(*status->Record.AddWorkers());
            }
            Env.SendAsync(ControllerId, status.Release());
        }

        google::protobuf::RepeatedPtrField<NKikimrSchemeOp::TPersQueueGroupDescription::TPartition> Partitions() {
            const auto desc = Env.GetDescription(StreamPath + "/streamImpl");
            return desc.GetPathDescription().GetPersQueueGroup().GetPartitions();
        }

        // what topic autopartitioning does on write load
        TVector<ui64> SplitRoot() {
            auto ev = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
            auto& tx = *ev->Record.MutableTransaction()->MutableModifyScheme();
            tx.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAlterPersQueueGroup);
            tx.SetWorkingDir(StreamPath);
            tx.SetAllowAccessToPrivatePaths(true);
            tx.MutableAlterPersQueueGroup()->SetName("streamImpl");
            auto& split = *tx.MutableAlterPersQueueGroup()->AddSplit();
            split.SetPartition(0);
            split.SetSplitBoundary(TString(1, '\x80'));

            auto resp = Env.Send<TEvTxUserProxy::TEvProposeTransactionStatus>(MakeTxProxyID(), ev.Release());
            const auto status = resp->Get()->Record.GetStatus();
            UNIT_ASSERT_C(status == NTxProxy::TResultStatus::ExecComplete || status == NTxProxy::TResultStatus::ExecInProgress,
                resp->Get()->Record.ShortDebugString());

            for (int i = 0; i < 60 && Partitions().size() < 3; ++i) {
                Sleep(TDuration::MilliSeconds(500));
            }

            TVector<ui64> children;
            for (const auto& p : Partitions()) {
                if (p.GetParentPartitionIds().size()) {
                    children.push_back(p.GetPartitionId());
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(children.size(), 2);
            return children;
        }

        // what the service sends when the reader gets TEndPartitionSessionEvent
        void DataEnd(const TVector<ui64>& children) {
            auto ev = MakeHolder<TEvService::TEvWorkerDataEnd>();
            Root.Serialize(*ev->Record.MutableWorker());
            ev->Record.SetPartitionId(Root.WorkerId());
            for (auto id : children) {
                ev->Record.AddChildPartitionsIds(id);
            }
            Env.SendAsync(ControllerId, ev.Release());
        }

        THashSet<ui64> WaitStarted(const TVector<ui64>& children, TDuration timeout) {
            THashSet<ui64> started;
            const auto deadline = TInstant::Now() + timeout;
            while (started.size() < children.size() && TInstant::Now() < deadline) {
                auto ev = Env.GetRuntime().GrabEdgeEvent<TEvService::TEvRunWorker>(Service, deadline - TInstant::Now());
                if (!ev) {
                    break;
                }
                const auto id = TWorkerId::Parse(ev->Get()->Record.GetWorker());
                Cerr << "... run worker " << id << Endl;
                if (Find(children, id.WorkerId()) != children.end()) {
                    started.insert(id.WorkerId());
                }
            }
            return started;
        }
    };

    Y_UNIT_TEST(ChildrenStartOnDataEnd) {
        TSetup s;
        const auto children = s.SplitRoot();

        s.DataEnd(children);
        UNIT_ASSERT_VALUES_EQUAL(s.WaitStarted(children, TDuration::Seconds(30)).size(), 2);
    }

    // No faults: the real service reads the parent to the end, reports DataEnd,
    // and the controller starts the children. Rows written after the split
    // can only reach the replica through the children.
    Y_UNIT_TEST(EndToEndSplit) {
        TSetup s(false);
        s.SplitRoot();

        s.Upsert(101, 200);
        UNIT_ASSERT_VALUES_EQUAL(s.WaitReplicated(200, TDuration::Seconds(60)), 200);
    }

    // The controller subscribes to the interconnect session with every worker node.
    // On TEvNodeDisconnected it deletes the replication session, but a new one is
    // created only for nodes that discovery reports as new. The node is still in
    // discovery, so the session is never re-established.
    Y_UNIT_TEST(SessionRestoredAfterNodeDisconnect) {
        TSetup s;
        auto& runtime = s.Env.GetRuntime();

        s.Env.SendAsync(s.ControllerId, new TEvInterconnect::TEvNodeDisconnected(runtime.GetNodeId(0)));

        auto ev = runtime.GrabEdgeEvent<TEvService::TEvHandshake>(s.Service, TDuration::Seconds(60));
        UNIT_ASSERT_C(ev, "no new session with the node after a transient disconnect");
    }

    // The parent's DataEnd is sent while the controller has no session with the node
    // (here it goes to a stand-in), then the node disconnects. The controller must
    // restore the session, and the service must resend DataEnd on the new handshake.
    // TEvNodeDisconnected is injected: the controller is on the same node, so it does
    // not subscribe to the interconnect session in this test.
    Y_UNIT_TEST(EndToEndDataEndLostOnNodeDisconnect) {
        TSetup s(false);
        auto& runtime = s.Env.GetRuntime();
        const auto edge = s.HijackServiceSession();

        s.SplitRoot();
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvService::TEvWorkerDataEnd>(edge));

        s.Env.SendAsync(s.ControllerId, new TEvInterconnect::TEvNodeDisconnected(runtime.GetNodeId(0)));

        s.Upsert(101, 200);
        UNIT_ASSERT_VALUES_EQUAL_C(s.WaitReplicated(200, TDuration::Seconds(60)), 200,
            "rows written after the split are lost: child partitions are never read");
    }

    // After a controller restart running workers are re-adopted from TEvStatus.
    // A re-adopted child (its command is not persisted) must be rebooted when it stops.
    Y_UNIT_TEST(AdoptedWorkerRebootedAfterStop) {
        TSetup s;
        const auto children = s.SplitRoot();

        s.DataEnd(children);
        UNIT_ASSERT_VALUES_EQUAL(s.WaitStarted(children, TDuration::Seconds(30)).size(), 2);

        s.RebootController();
        const TWorkerId child(s.Root.ReplicationId(), s.Root.TargetId(), children.at(0));
        s.Handshake({s.Root, child, TWorkerId(s.Root.ReplicationId(), s.Root.TargetId(), children.at(1))});

        s.Env.SendAsync(s.ControllerId, new TEvService::TEvWorkerStatus(child, NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED));

        const auto started = s.WaitStarted({children.at(0)}, TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL_C(started.size(), 1, "re-adopted worker is not rebooted");
    }

    // The parent's DataEnd goes to a controller generation that is gone
    // (here: a stand-in with the same generation), then the controller restarts.
    // The service must resend DataEnd on the next handshake.
    Y_UNIT_TEST(EndToEndDataEndLost) {
        TSetup s(false);
        auto& runtime = s.Env.GetRuntime();
        const auto edge = s.HijackServiceSession();

        s.SplitRoot();
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvService::TEvWorkerDataEnd>(edge));

        s.RebootController();

        s.Upsert(101, 200);
        UNIT_ASSERT_VALUES_EQUAL_C(s.WaitReplicated(200, TDuration::Seconds(60)), 200,
            "rows written after the split are lost: child partitions are never read");
    }
}

}
