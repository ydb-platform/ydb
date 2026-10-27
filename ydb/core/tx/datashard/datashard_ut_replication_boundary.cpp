#include "datashard_ut_common_kqp.h"

#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/tx/long_tx_service/public/events.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/public/api/protos/draft/ydb_replication.pb.h>

namespace NKikimr::NDataShard {
using namespace Tests;
using namespace NKqpHelpers;

Y_UNIT_TEST_SUITE(ReplicationBoundary) {
    struct TEnv {
        TPortManager Ports;
        TServer::TPtr Server = new TServer(TServerSettings(Ports.GetPort(2134))
            .SetUseRealThreads(false).SetDomainName("Root"));
        TTestActorRuntime& Runtime = *Server->GetRuntime();
        TActorId Sender = Runtime.AllocateEdgeActor();

        TEnv() {
            InitRoot(Server, Sender);
            for (const TString name : {"One", "Two"}) {
                CreateShardedTable(Server, Sender, "/Root", name, TShardedTableOptions());
            }
        }

        void Freeze(const TString& name) {
            auto request = new TEvTxUserProxy::TEvProposeTransaction();
            auto* tx = request->Record.MutableTransaction()->MutableModifyScheme();
            tx->SetOperationType(NKikimrSchemeOp::ESchemeOpAlterTable);
            tx->SetWorkingDir("/Root");
            tx->MutableAlterTable()->SetName(name);
            tx->MutableAlterTable()->MutablePartitionConfig()->SetFreezeState(NKikimrSchemeOp::Freeze);
            Runtime.Send(new IEventHandle(MakeTxProxyID(), Sender, request));
            auto response = Runtime.GrabEdgeEventRethrow<TEvTxUserProxy::TEvProposeTransactionStatus>(Sender);
            UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetStatus(), TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress);
            WaitTxNotification(Server, Sender, response->Get()->Record.GetTxId());
        }

        Ydb::Operations::Operation Acquire(ui64 interval = 1000, const TVector<TString>& paths = {"/Root/One", "/Root/Two"}) {
            Ydb::Replication::AcquireReplicationBoundaryRequest request;
            request.set_commit_interval_ms(interval);
            for (const auto& path : paths) {
                request.add_source_paths(path);
            }
            using TCall = NGRpcService::TGrpcRequestOperationCall<Ydb::Replication::AcquireReplicationBoundaryRequest,
                Ydb::Replication::AcquireReplicationBoundaryResponse>;
            auto result = NRpcService::DoLocalRpc<TCall>(std::move(request), "/Root", "", Runtime.GetActorSystem(0));
            Runtime.WaitFor("boundary RPC", [&] { return result.HasValue(); });
            return result.GetValueSync().operation();
        }
    };

    Y_UNIT_TEST(RequiresEveryCompletedFreeze) {
        TEnv env;
        UNIT_ASSERT_VALUES_EQUAL(env.Acquire().status(), Ydb::StatusIds::PRECONDITION_FAILED);
        env.Freeze("One");
        UNIT_ASSERT_VALUES_EQUAL(env.Acquire().status(), Ydb::StatusIds::PRECONDITION_FAILED);
        env.Freeze("Two");
        const auto before = AcquireReadSnapshot(env.Runtime, "/Root");
        const auto operation = env.Acquire();
        UNIT_ASSERT_VALUES_EQUAL_C(operation.status(), Ydb::StatusIds::SUCCESS, operation.DebugString());
        Ydb::Replication::AcquireReplicationBoundaryResult result;
        UNIT_ASSERT(operation.result().UnpackTo(&result));
        UNIT_ASSERT(TRowVersion(result.snapshot().step(), result.snapshot().tx_id()) >= before);
        UNIT_ASSERT_VALUES_EQUAL(result.boundary().step(), (result.snapshot().step() / 1000 + 1) * 1000);
        UNIT_ASSERT_VALUES_EQUAL(result.boundary().tx_id(), 0);
        UNIT_ASSERT_VALUES_EQUAL(result.commit_interval_ms(), 1000);
        const auto retry = env.Acquire();
        UNIT_ASSERT_VALUES_EQUAL(retry.status(), Ydb::StatusIds::SUCCESS);
        Ydb::Replication::AcquireReplicationBoundaryResult next;
        UNIT_ASSERT(retry.result().UnpackTo(&next));
        UNIT_ASSERT(next.boundary().step() >= result.boundary().step());
    }

    Y_UNIT_TEST(RejectsInvalidScopeAndInterval) {
        TEnv env;
        UNIT_ASSERT_VALUES_EQUAL(env.Acquire(0).status(), Ydb::StatusIds::BAD_REQUEST);
        UNIT_ASSERT_VALUES_EQUAL(env.Acquire(1000, {}).status(), Ydb::StatusIds::BAD_REQUEST);
        UNIT_ASSERT_VALUES_EQUAL(env.Acquire(1000, {"/Root"}).status(), Ydb::StatusIds::BAD_REQUEST);
        UNIT_ASSERT_VALUES_EQUAL(env.Acquire(1000, {"/Root/Missing"}).status(), Ydb::StatusIds::SCHEME_ERROR);
    }

    void CheckSnapshotRounding(ui64 step, ui64 interval, std::optional<ui64> expected) {
        TEnv env;
        env.Freeze("One");
        env.Freeze("Two");
        using TSnapshot = NLongTxService::TEvLongTxService::TEvAcquireReadSnapshotResult;
        auto observer = env.Runtime.AddObserver<TSnapshot>([&](TSnapshot::TPtr& ev) {
            ev->Get()->Snapshot = TRowVersion(step, Max<ui64>());
        });
        const auto operation = env.Acquire(interval);
        if (!expected) {
            UNIT_ASSERT_VALUES_EQUAL(operation.status(), Ydb::StatusIds::BAD_REQUEST);
            return;
        }
        UNIT_ASSERT_VALUES_EQUAL_C(operation.status(), Ydb::StatusIds::SUCCESS, operation.DebugString());
        Ydb::Replication::AcquireReplicationBoundaryResult result;
        UNIT_ASSERT(operation.result().UnpackTo(&result));
        UNIT_ASSERT_VALUES_EQUAL(result.boundary().step(), *expected);
    }

    Y_UNIT_TEST(ExactMultipleUsesNextInterval) { CheckSnapshotRounding(2000, 1000, 3000); }
    Y_UNIT_TEST(RejectsOverflow) { CheckSnapshotRounding(Max<ui64>(), 1000, {}); }
    Y_UNIT_TEST(LargestRepresentableBoundary) { CheckSnapshotRounding(Max<ui64>() - 1, Max<ui64>(), Max<ui64>()); }
}
} // namespace NKikimr::NDataShard
