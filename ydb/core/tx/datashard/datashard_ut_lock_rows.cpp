#include "datashard.h"
#include <ydb/core/tx/data_events/payload_helper.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/protos/query_stats.pb.h>
#include <ydb/core/tx/long_tx_service/public/lock_handle.h>
#include "datashard_ut_common_kqp.h"

#include <util/digest/multi.h>

namespace NKikimr {

using namespace NKikimr::NDataShard;
using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace NLongTxService;
using namespace NSchemeShard;
using namespace Tests;

Y_UNIT_TEST_SUITE(DataShardLockRows) {

    struct TRequestId {
        TActorId Sender;
        ui64 RequestId;

        operator size_t() const {
            return ::MultiHash(Sender, RequestId);
        }
    };

    struct TWaitGraphEdge {
        ui64 LockId;
        ui64 OtherLockId;

        friend std::strong_ordering operator<=>(const TWaitGraphEdge&, const TWaitGraphEdge&) = default;
    };

    std::tuple<TTestActorRuntime&, Tests::TServer::TPtr, TActorId> TestCreateServer(TPortManager& pm, std::optional<TServerSettings> serverSettings = {}) {
        if (!serverSettings) {
            serverSettings.emplace(pm.GetPort(2134));
            serverSettings->SetDomainName("Root").SetUseRealThreads(false);
        }

        Tests::TServer::TPtr server = new TServer(serverSettings.value());
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);

        return {runtime, server, sender};
    }

    class TTestPipe {
    public:
        TTestPipe(TTestActorRuntime& runtime, ui64 tabletId, ui32 nodeIndex = 0)
            : Runtime(runtime)
            , TabletId(tabletId)
        {
            PipeActor = Runtime.ConnectToPipe(TabletId, TActorId(), nodeIndex, NTabletPipe::TClientConfig{});
        }

        ~TTestPipe() {
            ui32 nodeIndex = PipeActor.NodeId() - Runtime.GetNodeId(0);
            Runtime.ClosePipe(PipeActor, TActorId(), nodeIndex);
        }

        void Send(const TActorId& sender, IEventBase* payload, ui64 cookie = 0) {
            ui32 nodeIndex = PipeActor.NodeId() - Runtime.GetNodeId(0);
            Runtime.SendToPipe(PipeActor, sender, payload, nodeIndex, cookie);
        }

    private:
        TTestActorRuntime& Runtime;
        const ui64 TabletId;
        TActorId PipeActor;
    };

    Y_UNIT_TEST(Basics) {
        TPortManager pm;

        auto [runtime, server, sender] = TestCreateServer(pm);

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key));
            )"),
            "SUCCESS");

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        TTestPipe pipe(runtime, shards.at(0));

        NLongTxService::TLockHandle lock1(123, runtime.GetActorSystem(0));
        NLongTxService::TLockHandle lock2(234, runtime.GetActorSystem(0));

        // Lock key 1 by lock 1
        {
            auto req = std::make_unique<NEvents::TDataEvents::TEvLockRows>(1);
            req->Record.SetLockId(lock1.GetLockId());
            req->Record.SetLockNodeId(lock1.GetLockNodeId());
            req->Record.SetLockMode(NKikimrDataEvents::PESSIMISTIC_EXCLUSIVE);
            req->SetTableId(tableId);
            req->Record.AddColumnIds(1);
            req->Record.SetPayloadFormat(NKikimrDataEvents::FORMAT_CELLVEC);
            TVector<TCell> cells({
                TCell::Make(i32(1)),
            });
            req->SetCellMatrix(TSerializedCellMatrix::Serialize(cells, 1, 1));
            pipe.Send(sender, req.release());

            auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvLockRowsResult>(sender);
            auto* res = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetRequestId(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetStatus(), NKikimrDataEvents::TEvLockRowsResult::STATUS_SUCCESS);
        }

        // Try locking key 1 by lock 2
        {
            auto req = std::make_unique<NEvents::TDataEvents::TEvLockRows>(2);
            req->Record.SetLockId(lock2.GetLockId());
            req->Record.SetLockNodeId(lock2.GetLockNodeId());
            req->Record.SetLockMode(NKikimrDataEvents::PESSIMISTIC_EXCLUSIVE);
            req->SetTableId(tableId);
            req->Record.AddColumnIds(1);
            req->Record.SetPayloadFormat(NKikimrDataEvents::FORMAT_CELLVEC);
            TVector<TCell> cells({
                TCell::Make(i32(1)),
            });
            req->SetCellMatrix(TSerializedCellMatrix::Serialize(cells, 1, 1));
            pipe.Send(sender, req.release());

            auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvLockRowsResult>(sender, TDuration::Seconds(1));
            UNIT_ASSERT_C(!ev, "Unexpected TEvLockRowsResult with status " << ev->Get()->Record.GetStatus());
        }

        // Try locking key 1 by lock 1 again
        {
            auto req = std::make_unique<NEvents::TDataEvents::TEvLockRows>(3);
            req->Record.SetLockId(lock1.GetLockId());
            req->Record.SetLockNodeId(lock1.GetLockNodeId());
            req->Record.SetLockMode(NKikimrDataEvents::PESSIMISTIC_EXCLUSIVE);
            req->SetTableId(tableId);
            req->Record.AddColumnIds(1);
            req->Record.SetPayloadFormat(NKikimrDataEvents::FORMAT_CELLVEC);
            TVector<TCell> cells({
                TCell::Make(i32(1)),
            });
            req->SetCellMatrix(TSerializedCellMatrix::Serialize(cells, 1, 1));
            pipe.Send(sender, req.release());

            auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvLockRowsResult>(sender);
            auto* res = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetRequestId(), 3u);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetStatus(), NKikimrDataEvents::TEvLockRowsResult::STATUS_SUCCESS);
        }

        // Try waiting for lock 2 some more
        {
            auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvLockRowsResult>(sender, TDuration::Seconds(1));
            UNIT_ASSERT_C(!ev, "Unexpected TEvLockRowsResult with status " << ev->Get()->Record.GetStatus());
        }

        // Destroy lock 1 handle, it must rollback all changes
        lock1.Reset();

        // Try waiting for lock 2 now
        {
            auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvLockRowsResult>(sender);
            auto* res = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetRequestId(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetStatus(), NKikimrDataEvents::TEvLockRowsResult::STATUS_SUCCESS);
        }
    }

    Y_UNIT_TEST(RequestCancelledOnSplit) {
        TPortManager pm;

        auto [runtime, server, sender] = TestCreateServer(pm);

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key));
            )"),
            "SUCCESS");

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        TTestPipe pipe(runtime, shards.at(0));

        NLongTxService::TLockHandle lock1(123, runtime.GetActorSystem(0));
        NLongTxService::TLockHandle lock2(234, runtime.GetActorSystem(0));

        // Lock key 1 by lock 1
        {
            auto req = std::make_unique<NEvents::TDataEvents::TEvLockRows>(1);
            req->Record.SetLockId(lock1.GetLockId());
            req->Record.SetLockNodeId(lock1.GetLockNodeId());
            req->Record.SetLockMode(NKikimrDataEvents::PESSIMISTIC_EXCLUSIVE);
            req->SetTableId(tableId);
            req->Record.AddColumnIds(1);
            req->Record.SetPayloadFormat(NKikimrDataEvents::FORMAT_CELLVEC);
            TVector<TCell> cells({
                TCell::Make(i32(1)),
            });
            req->SetCellMatrix(TSerializedCellMatrix::Serialize(cells, 1, 1));
            pipe.Send(sender, req.release());

            auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvLockRowsResult>(sender);
            auto* res = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetRequestId(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetStatus(), NKikimrDataEvents::TEvLockRowsResult::STATUS_SUCCESS);
        }

        // Try locking key 1 by lock 2
        {
            auto req = std::make_unique<NEvents::TDataEvents::TEvLockRows>(2);
            req->Record.SetLockId(lock2.GetLockId());
            req->Record.SetLockNodeId(lock2.GetLockNodeId());
            req->Record.SetLockMode(NKikimrDataEvents::PESSIMISTIC_EXCLUSIVE);
            req->SetTableId(tableId);
            req->Record.AddColumnIds(1);
            req->Record.SetPayloadFormat(NKikimrDataEvents::FORMAT_CELLVEC);
            TVector<TCell> cells({
                TCell::Make(i32(1)),
            });
            req->SetCellMatrix(TSerializedCellMatrix::Serialize(cells, 1, 1));
            pipe.Send(sender, req.release());

            auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvLockRowsResult>(sender, TDuration::Seconds(1));
            UNIT_ASSERT_C(!ev, "Unexpected TEvLockRowsResult with status " << ev->Get()->Record.GetStatus());
        }

        // Split would fail otherwise :(
        SetSplitMergePartCountLimit(&runtime, -1);

        auto makeSplitKey = [](i32 value) {
            NKikimrMiniKQL::TValue proto;
            proto.SetInt32(value);
            return proto;
        };

        auto senderSplit = runtime.AllocateEdgeActor();
        ui64 txId = AsyncSplitTable(server, senderSplit, "/Root/table", shards.at(0), makeSplitKey(3));
        WaitTxNotification(server, senderSplit, txId);

        {
            auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvLockRowsResult>(sender, TDuration::Seconds(1));
            UNIT_ASSERT_C(ev, "Unexpected failure to receive TEvLockRowsResult");
            auto* res = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetRequestId(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetStatus(), NKikimrDataEvents::TEvLockRowsResult::STATUS_OVERLOADED);
        }
    }

    Y_UNIT_TEST(RequestCancelledOnGracefulRestart) {
        TPortManager pm;

        auto [runtime, server, sender] = TestCreateServer(pm);

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key));
            )"),
            "SUCCESS");

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        TTestPipe pipe(runtime, shards.at(0));

        NLongTxService::TLockHandle lock1(123, runtime.GetActorSystem(0));
        NLongTxService::TLockHandle lock2(234, runtime.GetActorSystem(0));

        // Lock key 1 by lock 1
        {
            auto req = std::make_unique<NEvents::TDataEvents::TEvLockRows>(1);
            req->Record.SetLockId(lock1.GetLockId());
            req->Record.SetLockNodeId(lock1.GetLockNodeId());
            req->Record.SetLockMode(NKikimrDataEvents::PESSIMISTIC_EXCLUSIVE);
            req->SetTableId(tableId);
            req->Record.AddColumnIds(1);
            req->Record.SetPayloadFormat(NKikimrDataEvents::FORMAT_CELLVEC);
            TVector<TCell> cells({
                TCell::Make(i32(1)),
            });
            req->SetCellMatrix(TSerializedCellMatrix::Serialize(cells, 1, 1));
            pipe.Send(sender, req.release());

            auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvLockRowsResult>(sender);
            auto* res = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetRequestId(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetStatus(), NKikimrDataEvents::TEvLockRowsResult::STATUS_SUCCESS);
        }

        // Try locking key 1 by lock 2
        {
            auto req = std::make_unique<NEvents::TDataEvents::TEvLockRows>(2);
            req->Record.SetLockId(lock2.GetLockId());
            req->Record.SetLockNodeId(lock2.GetLockNodeId());
            req->Record.SetLockMode(NKikimrDataEvents::PESSIMISTIC_EXCLUSIVE);
            req->SetTableId(tableId);
            req->Record.AddColumnIds(1);
            req->Record.SetPayloadFormat(NKikimrDataEvents::FORMAT_CELLVEC);
            TVector<TCell> cells({
                TCell::Make(i32(1)),
            });
            req->SetCellMatrix(TSerializedCellMatrix::Serialize(cells, 1, 1));
            pipe.Send(sender, req.release());

            auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvLockRowsResult>(sender, TDuration::Seconds(1));
            UNIT_ASSERT_C(!ev, "Unexpected TEvLockRowsResult with status " << ev->Get()->Record.GetStatus());
        }

        GracefulRestartTablet(runtime, shards.at(0), sender);

        {
            auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvLockRowsResult>(sender, TDuration::Seconds(1));
            UNIT_ASSERT_C(ev, "Unexpected failure to receive TEvLockRowsResult");
            auto* res = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetRequestId(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetStatus(), NKikimrDataEvents::TEvLockRowsResult::STATUS_OVERLOADED);
        }
    }

    Y_UNIT_TEST(BrokenOwnerUnblocksAwaiter) {
        TPortManager pm;

        auto [runtime, server, sender] = TestCreateServer(pm);

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key));
            )"),
            "SUCCESS");

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        TTestPipe pipe(runtime, shards.at(0));

        NLongTxService::TLockHandle lock1(123, runtime.GetActorSystem(0));
        NLongTxService::TLockHandle lock2(234, runtime.GetActorSystem(0));

        THashMap<TRequestId, TWaitGraphEdge> waitGraph;
        auto waitGraphAddObserver = runtime.AddObserver<TEvLongTxService::TEvWaitingLockAdd>([&](auto& ev) {
            auto* msg = ev->Get();
            waitGraph[TRequestId{ ev->Sender, msg->RequestId }] = { msg->Lock.LockId, msg->OtherLock.LockId };
            ev.Reset();
        });
        auto waitGraphRemoveObserver = runtime.AddObserver<TEvLongTxService::TEvWaitingLockRemove>([&](auto& ev) {
            auto* msg = ev->Get();
            waitGraph.erase(TRequestId{ ev->Sender, msg->RequestId });
            ev.Reset();
        });
        auto waitGraphString = [&]() -> TString {
            TVector<TWaitGraphEdge> edges;
            for (auto& pr : waitGraph) {
                edges.push_back(pr.second);
            }
            std::sort(edges.begin(), edges.end());
            TStringBuilder sb;
            for (auto& edge : edges) {
                sb << edge.LockId << " -> " << edge.OtherLockId << "\n";
            }
            return sb;
        };

        // Lock keys 1 and 2 by lock 1
        {
            auto req = std::make_unique<NEvents::TDataEvents::TEvLockRows>(1);
            req->Record.SetLockId(lock1.GetLockId());
            req->Record.SetLockNodeId(lock1.GetLockNodeId());
            req->Record.SetLockMode(NKikimrDataEvents::PESSIMISTIC_EXCLUSIVE);
            req->SetTableId(tableId);
            req->Record.AddColumnIds(1);
            req->Record.SetPayloadFormat(NKikimrDataEvents::FORMAT_CELLVEC);
            TVector<TCell> cells({
                TCell::Make(i32(1)),
                TCell::Make(i32(2)),
            });
            req->SetCellMatrix(TSerializedCellMatrix::Serialize(cells, 2, 1));
            pipe.Send(sender, req.release());

            auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvLockRowsResult>(sender);
            auto* res = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetRequestId(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetStatus(), NKikimrDataEvents::TEvLockRowsResult::STATUS_SUCCESS);
        }

        // Lock keys 2 and 3 by lock 2
        {
            auto req = std::make_unique<NEvents::TDataEvents::TEvLockRows>(2);
            req->Record.SetLockId(lock2.GetLockId());
            req->Record.SetLockNodeId(lock2.GetLockNodeId());
            req->Record.SetLockMode(NKikimrDataEvents::PESSIMISTIC_EXCLUSIVE);
            req->SetTableId(tableId);
            req->Record.AddColumnIds(1);
            req->Record.SetPayloadFormat(NKikimrDataEvents::FORMAT_CELLVEC);
            TVector<TCell> cells({
                TCell::Make(i32(2)),
                TCell::Make(i32(3)),
            });
            req->SetCellMatrix(TSerializedCellMatrix::Serialize(cells, 2, 1));
            pipe.Send(sender, req.release());

            auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvLockRowsResult>(sender, TDuration::Seconds(1));
            UNIT_ASSERT_C(!ev, "Unexpected TEvLockRowsResult with status " << ev->Get()->Record.GetStatus());
        }

        UNIT_ASSERT_VALUES_EQUAL(
            waitGraphString(),
            "234 -> 123\n");

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                UPSERT INTO `/Root/table` (key, value) VALUES (1, 101);
            )"),
            "<empty>");

        {
            auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvLockRowsResult>(sender, TDuration::Seconds(1));
            UNIT_ASSERT_C(ev, "Expected TEvLockRowsResult after the current owner is broken");
            auto* res = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetRequestId(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetStatus(), NKikimrDataEvents::TEvLockRowsResult::STATUS_SUCCESS);
        }

        UNIT_ASSERT_VALUES_EQUAL(
            waitGraphString(),
            "");
    }

    Y_UNIT_TEST(BrokenAwaiterUnblocksAwait) {
        TPortManager pm;

        auto [runtime, server, sender] = TestCreateServer(pm);

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key));
            )"),
            "SUCCESS");

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        TTestPipe pipe(runtime, shards.at(0));

        NLongTxService::TLockHandle lock1(123, runtime.GetActorSystem(0));
        NLongTxService::TLockHandle lock2(234, runtime.GetActorSystem(0));

        THashMap<TRequestId, TWaitGraphEdge> waitGraph;
        auto waitGraphAddObserver = runtime.AddObserver<TEvLongTxService::TEvWaitingLockAdd>([&](auto& ev) {
            auto* msg = ev->Get();
            waitGraph[TRequestId{ ev->Sender, msg->RequestId }] = { msg->Lock.LockId, msg->OtherLock.LockId };
            ev.Reset();
        });
        auto waitGraphRemoveObserver = runtime.AddObserver<TEvLongTxService::TEvWaitingLockRemove>([&](auto& ev) {
            auto* msg = ev->Get();
            waitGraph.erase(TRequestId{ ev->Sender, msg->RequestId });
            ev.Reset();
        });
        auto waitGraphString = [&]() -> TString {
            TVector<TWaitGraphEdge> edges;
            for (auto& pr : waitGraph) {
                edges.push_back(pr.second);
            }
            std::sort(edges.begin(), edges.end());
            TStringBuilder sb;
            for (auto& edge : edges) {
                sb << edge.LockId << " -> " << edge.OtherLockId << "\n";
            }
            return sb;
        };

        // Lock keys 1 and 2 by lock 1
        {
            auto req = std::make_unique<NEvents::TDataEvents::TEvLockRows>(1);
            req->Record.SetLockId(lock1.GetLockId());
            req->Record.SetLockNodeId(lock1.GetLockNodeId());
            req->Record.SetLockMode(NKikimrDataEvents::PESSIMISTIC_EXCLUSIVE);
            req->SetTableId(tableId);
            req->Record.AddColumnIds(1);
            req->Record.SetPayloadFormat(NKikimrDataEvents::FORMAT_CELLVEC);
            TVector<TCell> cells({
                TCell::Make(i32(1)),
                TCell::Make(i32(2)),
            });
            req->SetCellMatrix(TSerializedCellMatrix::Serialize(cells, 2, 1));
            pipe.Send(sender, req.release());

            auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvLockRowsResult>(sender);
            auto* res = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetRequestId(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetStatus(), NKikimrDataEvents::TEvLockRowsResult::STATUS_SUCCESS);
        }

        // Lock keys 3 and 2 by lock 2
        {
            auto req = std::make_unique<NEvents::TDataEvents::TEvLockRows>(2);
            req->Record.SetLockId(lock2.GetLockId());
            req->Record.SetLockNodeId(lock2.GetLockNodeId());
            req->Record.SetLockMode(NKikimrDataEvents::PESSIMISTIC_EXCLUSIVE);
            req->SetTableId(tableId);
            req->Record.AddColumnIds(1);
            req->Record.SetPayloadFormat(NKikimrDataEvents::FORMAT_CELLVEC);
            TVector<TCell> cells({
                TCell::Make(i32(3)),
                TCell::Make(i32(2)),
            });
            req->SetCellMatrix(TSerializedCellMatrix::Serialize(cells, 2, 1));
            pipe.Send(sender, req.release());

            auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvLockRowsResult>(sender, TDuration::Seconds(1));
            UNIT_ASSERT_C(!ev, "Unexpected TEvLockRowsResult with status " << ev->Get()->Record.GetStatus());
        }

        UNIT_ASSERT_VALUES_EQUAL(
            waitGraphString(),
            "234 -> 123\n");

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                UPSERT INTO `/Root/table` (key, value) VALUES (3, 101);
            )"),
            "<empty>");

        {
            auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvLockRowsResult>(sender, TDuration::Seconds(1));
            UNIT_ASSERT_C(ev, "Expected TEvLockRowsResult after the awaiter is broken");
            auto* res = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetRequestId(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetStatus(), NKikimrDataEvents::TEvLockRowsResult::STATUS_LOCKS_BROKEN);
        }

        UNIT_ASSERT_VALUES_EQUAL(
            waitGraphString(),
            "");
    }

    Y_UNIT_TEST(RollbackAwaiterUnblocksAwait) {
        TPortManager pm;

        auto [runtime, server, sender] = TestCreateServer(pm);

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key));
            )"),
            "SUCCESS");

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        TTestPipe pipe(runtime, shards.at(0));

        NLongTxService::TLockHandle lock1(123, runtime.GetActorSystem(0));
        NLongTxService::TLockHandle lock2(234, runtime.GetActorSystem(0));

        THashMap<TRequestId, TWaitGraphEdge> waitGraph;
        auto waitGraphAddObserver = runtime.AddObserver<TEvLongTxService::TEvWaitingLockAdd>([&](auto& ev) {
            auto* msg = ev->Get();
            waitGraph[TRequestId{ ev->Sender, msg->RequestId }] = { msg->Lock.LockId, msg->OtherLock.LockId };
            ev.Reset();
        });
        auto waitGraphRemoveObserver = runtime.AddObserver<TEvLongTxService::TEvWaitingLockRemove>([&](auto& ev) {
            auto* msg = ev->Get();
            waitGraph.erase(TRequestId{ ev->Sender, msg->RequestId });
            ev.Reset();
        });
        auto waitGraphString = [&]() -> TString {
            TVector<TWaitGraphEdge> edges;
            for (auto& pr : waitGraph) {
                edges.push_back(pr.second);
            }
            std::sort(edges.begin(), edges.end());
            TStringBuilder sb;
            for (auto& edge : edges) {
                sb << edge.LockId << " -> " << edge.OtherLockId << "\n";
            }
            return sb;
        };

        // Lock keys 1 and 2 by lock 1
        {
            auto req = std::make_unique<NEvents::TDataEvents::TEvLockRows>(1);
            req->Record.SetLockId(lock1.GetLockId());
            req->Record.SetLockNodeId(lock1.GetLockNodeId());
            req->Record.SetLockMode(NKikimrDataEvents::PESSIMISTIC_EXCLUSIVE);
            req->SetTableId(tableId);
            req->Record.AddColumnIds(1);
            req->Record.SetPayloadFormat(NKikimrDataEvents::FORMAT_CELLVEC);
            TVector<TCell> cells({
                TCell::Make(i32(1)),
                TCell::Make(i32(2)),
            });
            req->SetCellMatrix(TSerializedCellMatrix::Serialize(cells, 2, 1));
            pipe.Send(sender, req.release());

            auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvLockRowsResult>(sender);
            auto* res = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetRequestId(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetStatus(), NKikimrDataEvents::TEvLockRowsResult::STATUS_SUCCESS);
        }

        // Lock keys 3 and 2 by lock 2
        {
            auto req = std::make_unique<NEvents::TDataEvents::TEvLockRows>(2);
            req->Record.SetLockId(lock2.GetLockId());
            req->Record.SetLockNodeId(lock2.GetLockNodeId());
            req->Record.SetLockMode(NKikimrDataEvents::PESSIMISTIC_EXCLUSIVE);
            req->SetTableId(tableId);
            req->Record.AddColumnIds(1);
            req->Record.SetPayloadFormat(NKikimrDataEvents::FORMAT_CELLVEC);
            TVector<TCell> cells({
                TCell::Make(i32(3)),
                TCell::Make(i32(2)),
            });
            req->SetCellMatrix(TSerializedCellMatrix::Serialize(cells, 2, 1));
            pipe.Send(sender, req.release());

            auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvLockRowsResult>(sender, TDuration::Seconds(1));
            UNIT_ASSERT_C(!ev, "Unexpected TEvLockRowsResult with status " << ev->Get()->Record.GetStatus());
        }

        UNIT_ASSERT_VALUES_EQUAL(
            waitGraphString(),
            "234 -> 123\n");

        // Break the second lock (it will notify the long tx service and rollback datashard changes)
        lock2.Reset();

        {
            auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvLockRowsResult>(sender, TDuration::Seconds(1));
            UNIT_ASSERT_C(ev, "Expected TEvLockRowsResult after the awaiter is broken");
            auto* res = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetRequestId(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetStatus(), NKikimrDataEvents::TEvLockRowsResult::STATUS_LOCKS_BROKEN);
        }

        UNIT_ASSERT_VALUES_EQUAL(
            waitGraphString(),
            "");
    }

    Y_UNIT_TEST(DeadlockDetectionUnblocksAwait) {
        TPortManager pm;

        auto [runtime, server, sender] = TestCreateServer(pm);

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key));
            )"),
            "SUCCESS");

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        TTestPipe pipe(runtime, shards.at(0));

        NLongTxService::TLockHandle lock1(123, runtime.GetActorSystem(0));
        NLongTxService::TLockHandle lock2(234, runtime.GetActorSystem(0));
        NLongTxService::TLockHandle lock3(345, runtime.GetActorSystem(0));

        THashMap<TRequestId, TWaitGraphEdge> waitGraph;
        auto waitGraphAddObserver = runtime.AddObserver<TEvLongTxService::TEvWaitingLockAdd>([&](auto& ev) {
            auto* msg = ev->Get();
            waitGraph[TRequestId{ ev->Sender, msg->RequestId }] = { msg->Lock.LockId, msg->OtherLock.LockId };
            ev.Reset();
        });
        auto waitGraphRemoveObserver = runtime.AddObserver<TEvLongTxService::TEvWaitingLockRemove>([&](auto& ev) {
            auto* msg = ev->Get();
            waitGraph.erase(TRequestId{ ev->Sender, msg->RequestId });
            ev.Reset();
        });
        auto waitGraphString = [&]() -> TString {
            TVector<TWaitGraphEdge> edges;
            for (auto& pr : waitGraph) {
                edges.push_back(pr.second);
            }
            std::sort(edges.begin(), edges.end());
            TStringBuilder sb;
            for (auto& edge : edges) {
                sb << edge.LockId << " -> " << edge.OtherLockId << "\n";
            }
            return sb;
        };

        // Lock keys 1 and 2 by lock 1
        {
            auto req = std::make_unique<NEvents::TDataEvents::TEvLockRows>(1);
            req->Record.SetLockId(lock1.GetLockId());
            req->Record.SetLockNodeId(lock1.GetLockNodeId());
            req->Record.SetLockMode(NKikimrDataEvents::PESSIMISTIC_EXCLUSIVE);
            req->SetTableId(tableId);
            req->Record.AddColumnIds(1);
            req->Record.SetPayloadFormat(NKikimrDataEvents::FORMAT_CELLVEC);
            TVector<TCell> cells({
                TCell::Make(i32(1)),
                TCell::Make(i32(2)),
            });
            req->SetCellMatrix(TSerializedCellMatrix::Serialize(cells, 2, 1));
            pipe.Send(sender, req.release());

            auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvLockRowsResult>(sender);
            auto* res = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetRequestId(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetStatus(), NKikimrDataEvents::TEvLockRowsResult::STATUS_SUCCESS);
        }

        // Lock keys 2 and 3 by lock 2
        {
            auto req = std::make_unique<NEvents::TDataEvents::TEvLockRows>(2);
            req->Record.SetLockId(lock2.GetLockId());
            req->Record.SetLockNodeId(lock2.GetLockNodeId());
            req->Record.SetLockMode(NKikimrDataEvents::PESSIMISTIC_EXCLUSIVE);
            req->SetTableId(tableId);
            req->Record.AddColumnIds(1);
            req->Record.SetPayloadFormat(NKikimrDataEvents::FORMAT_CELLVEC);
            TVector<TCell> cells({
                TCell::Make(i32(2)),
                TCell::Make(i32(3)),
            });
            req->SetCellMatrix(TSerializedCellMatrix::Serialize(cells, 2, 1));
            pipe.Send(sender, req.release());

            auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvLockRowsResult>(sender, TDuration::Seconds(1));
            UNIT_ASSERT_C(!ev, "Unexpected TEvLockRowsResult with status " << ev->Get()->Record.GetStatus());
        }

        // Lock keys 3 and 2 by lock 3
        {
            auto req = std::make_unique<NEvents::TDataEvents::TEvLockRows>(3);
            req->Record.SetLockId(lock3.GetLockId());
            req->Record.SetLockNodeId(lock3.GetLockNodeId());
            req->Record.SetLockMode(NKikimrDataEvents::PESSIMISTIC_EXCLUSIVE);
            req->SetTableId(tableId);
            req->Record.AddColumnIds(1);
            req->Record.SetPayloadFormat(NKikimrDataEvents::FORMAT_CELLVEC);
            TVector<TCell> cells({
                TCell::Make(i32(3)),
                TCell::Make(i32(2)),
            });
            req->SetCellMatrix(TSerializedCellMatrix::Serialize(cells, 2, 1));
            pipe.Send(sender, req.release());

            auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvLockRowsResult>(sender, TDuration::Seconds(1));
            UNIT_ASSERT_C(!ev, "Unexpected TEvLockRowsResult with status " << ev->Get()->Record.GetStatus());
        }

        UNIT_ASSERT_VALUES_EQUAL(
            waitGraphString(),
            "234 -> 123\n"
            "345 -> 234\n");

        // Break the current owner
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                UPSERT INTO `/Root/table` (key, value) VALUES (1, 101);
            )"),
            "<empty>");

        // Both operations should be blocked waiting for each other
        {
            auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvLockRowsResult>(sender, TDuration::Seconds(1));
            UNIT_ASSERT_C(!ev, "Unexpected TEvLockRowsResult with status " << ev->Get()->Record.GetStatus());
        }

        UNIT_ASSERT_VALUES_EQUAL(
            waitGraphString(),
            "234 -> 345\n"
            "345 -> 234\n");

        // Ask the third (yongest) operation to break the deadlock
        for (auto& pr : waitGraph) {
            if (pr.second.LockId == 345) {
                runtime.Send(new IEventHandle(pr.first.Sender, TActorId(), new TEvLongTxService::TEvWaitingLockDeadlock(pr.first.RequestId)), 0, true);
            }
        }

        // The third operation should reply with the STATUS_DEADLOCK
        {
            auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvLockRowsResult>(sender, TDuration::Seconds(1));
            UNIT_ASSERT_C(ev, "Expected TEvLockRowsResult after the deadlock notification");
            auto* res = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetRequestId(), 3u);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetStatus(), NKikimrDataEvents::TEvLockRowsResult::STATUS_DEADLOCK);
        }

        // Perhaps unexpectedly the row 3 is still locked until the lock is broken or removed
        {
            auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvLockRowsResult>(sender, TDuration::Seconds(1));
            UNIT_ASSERT_C(!ev, "Unexpected TEvLockRowsResult with status " << ev->Get()->Record.GetStatus());
        }

        // Reset the third lock (it will notify the long tx service and rollback datashard changes)
        lock3.Reset();

        // Finally the second operation should successfully lock all rows
        {
            auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvLockRowsResult>(sender, TDuration::Seconds(1));
            UNIT_ASSERT_C(ev, "Expected TEvLockRowsResult after the deadlock notification");
            auto* res = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetRequestId(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetStatus(), NKikimrDataEvents::TEvLockRowsResult::STATUS_SUCCESS);
        }

        UNIT_ASSERT_VALUES_EQUAL(
            waitGraphString(),
            "");
    }

} // Y_UNIT_TEST_SUITE(DataShardLockRows)

} // namespace NKikimr
